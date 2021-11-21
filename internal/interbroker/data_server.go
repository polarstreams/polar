package interbroker

import (
	"bufio"
	"fmt"
	"io"
	"net"

	"github.com/jorgebay/soda/internal/conf"
	"github.com/jorgebay/soda/internal/data"
	"github.com/jorgebay/soda/internal/metrics"
	"github.com/jorgebay/soda/internal/types"
	"github.com/jorgebay/soda/internal/utils"
	"github.com/rs/zerolog/log"
)

// acceptDataConnections starts listening to TCP connections for data
func (g *gossiper) acceptDataConnections() error {
	port := g.config.GossipDataPort()
	address := utils.GetServiceAddress(port, g.discoverer.LocalInfo(), g.config)

	listener, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}

	c := make(chan bool, 1)
	go func() {
		c <- true
		for {
			// HTTP/2 only server (prior knowledge)
			conn, err := listener.Accept()
			if err != nil {
				log.Err(err).Msgf("Failed to accept new data connections")
				break
			}

			log.Debug().Msgf("Accepted new gossip data connection on %v", conn.LocalAddr())
			g.handleData(conn)
		}
	}()

	<-c

	log.Info().Msgf("Start listening to peers for data streams on port %d", port)

	return nil
}

func (g *gossiper) handleData(conn net.Conn) {
	if tcpConn, isTcp := conn.(*net.TCPConn); isTcp {
		tcpConn.SetNoDelay(false)
	}

	s := &peerDataServer{
		conn:           conn,
		config:         g.config,
		replicaWriters: g.replicaWriters,
		responses:      make(chan dataResponse, 512),
	}
	go s.serve()
	go s.writeResponses()
}

// peerDataServer represents a handler for individual connections initiated
// by a peer client
type peerDataServer struct {
	conn           net.Conn
	initialized    bool
	config         conf.GossipConfig
	replicaWriters *utils.CopyOnWriteMap
	responses      chan dataResponse
}

func (s *peerDataServer) serve() {
	headerBuf := make([]byte, headerSize)
	reader := bufio.NewReaderSize(s.conn, receiveBufferSize)
	largeBodyBuf := make([]byte, s.config.MaxDataBodyLength())
	canUseReusableBuffer := make(chan bool, 1)
	canUseReusableBuffer <- true
	for {
		reader.Reset(s.conn)
		if n, err := io.ReadFull(reader, headerBuf); err != nil {
			log.Warn().Err(err).Int("n", n).Msg("There was an error reading header from peer client")
			break
		}
		header, err := readHeader(headerBuf)
		if err != nil {
			log.Warn().Msg("Invalid data header from peer, closing connection")
			break
		}

		if !s.initialized {
			s.initialized = true
			// It's the first message
			if header.Op != startupOp {
				log.Error().Msgf("Invalid first message %v", header.Op)
				s.responses <- newErrorResponse("Invalid first message", header)
				break
			}
			s.responses <- &emptyResponse{streamId: header.StreamId, op: readyOp}

			// Process the next message
			continue
		}

		<-canUseReusableBuffer
		bodyBuf := largeBodyBuf[:header.BodyLength]

		if _, err := io.ReadFull(reader, bodyBuf); err != nil {
			log.Warn().Msg("There was an error reading body from peer")
			break
		}

		if header.Op != dataReplicationOp {
			s.responses <- newErrorResponse("Only data replication operations are supported", header)
			break
		}

		// Append in the background while the next message is received
		go s.parseAndAppend(header, bodyBuf, canUseReusableBuffer)
	}
	log.Info().Msg("Data server reader closing connection")
	_ = s.conn.Close()
}

func (s *peerDataServer) parseAndAppend(header *header, bodyBuf []byte, done chan bool) {
	request, err := unmarshalDataRequest(bodyBuf)

	if err != nil {
		s.responses <- newErrorResponse("Parsing error", header)
	} else {
		s.responses <- s.append(request, header)
	}
	done <- true
}

// append stores data as a replica
func (s *peerDataServer) append(d *dataRequest, requestHeader *header) dataResponse {
	metrics.InterbrokerReceivedGroups.Inc()
	writer, err := s.segmentWriter(d)

	if err != nil {
		return nil
	}

	// Use a channel for the result
	d.appendResult = make(chan error, 1)

	// Send it to the writer
	writer.Items <- d

	// Wait for the result
	if err = <-d.appendResult; err != nil {
		return newErrorResponse(fmt.Sprintf("Append error: %s", err.Error()), requestHeader)
	}

	return &emptyResponse{streamId: requestHeader.StreamId, op: dataReplicationResponseOp}
}

func (s *peerDataServer) segmentWriter(d *dataRequest) (*data.SegmentWriter, error) {
	topic := d.topicId()
	segmentId := d.meta.SegmentId
	key := getReplicaWriterKey(&topic)
	writer, _, err := s.replicaWriters.LoadOrStore(key, func() (interface{}, error) {
		return data.NewSegmentWriter(topic, nil, s.config, segmentId)
	})

	if err != nil {
		return nil, err
	}

	return writer.(*data.SegmentWriter), nil
}

func getReplicaWriterKey(topic *types.TopicDataId) string {
	return fmt.Sprintf("%s|%d|%d", topic.Name, topic.Token, topic.GenId)
}

func (s *peerDataServer) writeResponses() {
	// TODO: Coalesce responses and disable Nagle
	w := utils.NewBufferCap(maxDataResponseSize)
	for response := range s.responses {
		w.Reset()
		if err := response.Marshal(w); err != nil {
			log.Warn().Err(err).Msg("There was an error while marshalling, closing connection")
			break
		}
		if n, err := s.conn.Write(w.Bytes()); err != nil {
			log.Warn().Err(err).Msg("There was an error while writing to peer, closing connection")
			break
		} else if n < w.Len() {
			log.Warn().Msg("Peer data server write was not able to send all the data")
			break
		}
	}

	log.Info().Msgf("Data server writer closing connection to %s", s.conn.RemoteAddr())
	_ = s.conn.Close()
}
