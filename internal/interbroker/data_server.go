package interbroker

import (
	"bufio"
	"fmt"
	"io"
	"net"

	"github.com/barcostreams/barco/internal/conf"
	"github.com/barcostreams/barco/internal/data"
	"github.com/barcostreams/barco/internal/metrics"
	"github.com/barcostreams/barco/internal/utils"
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
				if !g.localDb.IsShuttingDown() {
					log.Err(err).Msgf("Failed to accept new data connections")
				}
				break
			}

			log.Debug().Msgf("Accepted new gossip data connection on %v", conn.LocalAddr())
			g.handleData(conn)
		}
	}()

	<-c
	g.dataListener = listener

	log.Info().Msgf("Start listening to peers for data streams on port %d", port)

	return nil
}

func (g *gossiper) handleData(conn net.Conn) {
	s := &peerDataServer{
		conn:           conn,
		config:         g.config,
		datalog:        g.datalog,
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
	datalog        data.Datalog
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

		if header.Op == chunkReplicationOp {
			// Use a reusable buffer for data replication requests
			<-canUseReusableBuffer
			bodyBuf := largeBodyBuf[:header.BodyLength]

			if _, err := io.ReadFull(reader, bodyBuf); err != nil {
				log.Warn().Msg("There was an error reading body from peer")
				break
			}

			// Append in the background while the next message is received
			go s.handleChunkReplication(header, bodyBuf, canUseReusableBuffer)
			continue
		}

		if header.Op == fileStreamOp {
			// TODO: Logic can be moved to function
			// file stream request bodies are very small (meta & some other field), use tiny buffers
			bodyBuf := make([]byte, header.BodyLength)
			if _, err := io.ReadFull(reader, bodyBuf); err != nil {
				log.Warn().Msg("There was an error reading body from peer")
				break
			}
			// respond in the background
			go s.handleFileStreamRequest(header, bodyBuf)
			continue
		}

		s.responses <- newErrorResponse("Only data replication operations are supported", header)

	}
	log.Info().Msg("Data server reader closing connection")
	_ = s.conn.Close()
}

func (s *peerDataServer) handleChunkReplication(header *header, bodyBuf []byte, done chan bool) {
	request, err := unmarshalDataRequest(bodyBuf)

	if err != nil {
		s.responses <- newErrorResponse("Parsing error", header)
	} else {
		s.responses <- s.appendChunk(request, header)
	}
	done <- true
}

// append stores data as a replica
func (s *peerDataServer) appendChunk(d *chunkReplicationRequest, requestHeader *header) dataResponse {
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

	return &emptyResponse{streamId: requestHeader.StreamId, op: chunkReplicationResponseOp}
}

func (s *peerDataServer) segmentWriter(d *chunkReplicationRequest) (*data.SegmentWriter, error) {
	topic := d.topicId()
	segmentId := d.meta.SegmentId
	writer, _, err := s.replicaWriters.LoadOrStore(topic, func() (interface{}, error) {
		return data.NewSegmentWriter(topic, nil, s.config, &segmentId)
	})

	if err != nil {
		return nil, err
	}

	return writer.(*data.SegmentWriter), nil
}

func (s *peerDataServer) handleFileStreamRequest(header *header, bodyBuf []byte) {
	// r, err := unmarshalFileStreamRequest(bodyBuf)

	// if err != nil {
	// 	s.responses <- newErrorResponse("Parsing error", header)
	// 	return
	// }

	// topic := r.topicId()
	// buf = s.datalog.StreamBuffer()
	// TODO: Read from file
	// segmentId := r.meta.SegmentId

	s.responses <- newErrorResponse("Not implemented", header)
}

func (s *peerDataServer) writeResponses() {
	w := utils.NewBufferCap(maxResponseGroupSize)

	shouldExit := false
	var previousItem dataResponse
	for !shouldExit {
		w.Reset()
		groupSize := 0
		group := make([]dataResponse, 0)
		canAddNext := true

		if previousItem != nil {
			group = append(group, previousItem)
			groupSize += totalResponseSize(previousItem)
		}

		// Coalesce responses w/ Nagle disabled
		for canAddNext && !shouldExit {
			select {
			case response, ok := <-s.responses:
				previousItem = nil
				if !ok {
					shouldExit = true
					break
				}
				responseSize := totalResponseSize(response)
				if responseSize+w.Len() > maxResponseGroupSize {
					canAddNext = false
					previousItem = response
					break
				}
				group = append(group, response)
				groupSize += responseSize

			default:
				canAddNext = false
			}
		}

		bufferedResponses := make([]dataResponse, 0)
		for _, response := range group {
			if response.BodyBuffer() != nil {
				// Put it in a separate list that will be sent in a separate buffer
				bufferedResponses = append(bufferedResponses, response)
				continue
			}
			if err := response.Marshal(w); err != nil {
				log.Warn().Err(err).Msg("There was an error while marshalling, closing connection")
				shouldExit = true
				break
			}
		}

		if w.Len() > 0 {
			if _, err := s.conn.Write(w.Bytes()); err != nil {
				log.Warn().Err(err).Msg("There was an error while writing to peer, closing connection")
				shouldExit = true
				break
			}
		}

		if len(bufferedResponses) > 0 {
			// Use the provided buffer and not copy the buffer contents
			for _, response := range bufferedResponses {
				// Marshal and write header
				w.Reset()
				if err := response.Marshal(w); err != nil {
					log.Warn().Err(err).Msg("There was an error while marshalling, closing connection")
					shouldExit = true
					break
				}

				// Write header
				var writeErr error

				if _, writeErr = s.conn.Write(w.Bytes()); writeErr == nil {
					// Write body body
					_, writeErr = s.conn.Write(response.BodyBuffer())
				}

				// Whether the writing succeeded or failed, we should release the buffer
				response.ReleaseBuffer()

				if writeErr != nil {
					log.Warn().Err(writeErr).Msg("There was an error while writing to peer, closing connection")
					shouldExit = true
					break
				}
			}
		}
	}

	log.Info().Msgf("Data server writer closing connection to %s", s.conn.RemoteAddr())
	_ = s.conn.Close()
}

func totalResponseSize(r dataResponse) int {
	return int(r.BodyLength()) + headerSize
}
