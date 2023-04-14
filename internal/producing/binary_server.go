package producing

import (
	"encoding/binary"
	"io"
	"net"
	"net/url"
	"time"

	"github.com/polarstreams/polar/internal/conf"
	"github.com/polarstreams/polar/internal/discovery"
	"github.com/polarstreams/polar/internal/interbroker"
	"github.com/polarstreams/polar/internal/producing/pooling"
	. "github.com/polarstreams/polar/internal/types"
	"github.com/polarstreams/polar/internal/utils"
	"github.com/rs/zerolog/log"
)

const maxResponseGroupSize = 16 * 1024

func (p *producer) acceptBinaryConnections() error {
	port := p.config.ProducerBinaryPort()
	address := utils.GetServiceAddress(port, p.leaderGetter.LocalInfo(), p.config)

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
				log.Info()
				break
			}

			log.Debug().Msgf("Accepted new producer binary connection on %v", conn.LocalAddr())
			p.handleBinaryConnection(conn)
		}
	}()

	<-c
	log.Info().Msgf("Start listening to binary producer clients on %s", address)

	return nil
}

func (p *producer) handleBinaryConnection(conn net.Conn) {
	s := binaryServer{
		bufferPool:      p.bufferPool,
		gossiper:        p.gossiper,
		leaderGetter:    p.leaderGetter,
		coalescerGetter: p,
		conn:            conn,
		responses:       make(chan binaryResponse, 128),
	}

	go s.serve()
	go s.writeResponses()
}

type binaryServer struct {
	bufferPool      pooling.BufferPool
	gossiper        interbroker.Gossiper
	leaderGetter    discovery.TopologyGetter
	coalescerGetter coalescerGetter
	conn            io.ReadWriteCloser
	initialized     bool
	responses       chan binaryResponse
}

func (s *binaryServer) serve() {
	for {
		header := &binaryHeader{} // Reuse allocation
		err := binary.Read(s.conn, conf.Endianness, header)
		if err != nil {
			if err != io.EOF {
				log.Warn().Err(err).Msg("Invalid data header from producer client, closing connection")
			}
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

			continue
		}

		if header.Op == produceOp {
			if err := s.handleProduceMessage(header); err != nil {
				break
			}

			continue
		}

		if header.Op == heartbeatOp {
			s.responses <- &emptyResponse{streamId: header.StreamId, op: readyOp}
			continue
		}

		s.responses <- newErrorResponse("Only producer operations are supported", header)

	}
	log.Debug().Msg("Closing producer client connection")
	_ = s.conn.Close()
}

func (s *binaryServer) writeResponses() {
	w := utils.NewBufferCap(maxResponseGroupSize)

	shouldExit := false
	var item binaryResponse
	for !shouldExit {
		w.Reset()
		groupSize := 0
		group := make([]binaryResponse, 0)
		canAddNext := true

		if item == nil {
			// Block for the first item
			var ok bool
			item, ok = <-s.responses
			if !ok {
				break
			}
		}

		group = append(group, item)
		groupSize += totalResponseSize(item)
		item = nil

		// Coalesce responses w/ Nagle disabled
		for canAddNext && !shouldExit {
			select {
			case response, ok := <-s.responses:
				if !ok {
					shouldExit = true
					break
				}
				responseSize := totalResponseSize(response)
				if responseSize+groupSize > maxResponseGroupSize {
					canAddNext = false
					item = response
					break
				}
				group = append(group, response)
				groupSize += responseSize

			default:
				canAddNext = false
			}
		}

		for _, response := range group {
			if err := response.Marshal(w); err != nil {
				log.Warn().Err(err).Msg(
					"There was an error while marshaling a producer client response, closing connection")
				shouldExit = true
				break
			}
		}

		if w.Len() > 0 {
			if _, err := s.conn.Write(w.Bytes()); err != nil {
				log.Warn().Err(err).Msg("There was an error while writing to a producer client, closing connection")
				break
			}
		}
	}

	log.Info().Msgf("Producer binary server writer closing connection to client")
	_ = s.conn.Close()
}

func totalResponseSize(r binaryResponse) int {
	return r.BodyLength() + binaryHeaderSize
}

// Handles the message in the background and it returns an error when it's not safe to continue
func (s *binaryServer) handleProduceMessage(header *binaryHeader) error {
	bodyBuffers := s.bufferPool.Get(int(header.BodyLength))
	if err := utils.ReadIntoBuffers(s.conn, bodyBuffers, int(header.BodyLength)); err != nil {
		s.bufferPool.Free(bodyBuffers)
		log.Warn().Err(err).Msgf("Error reading from producer client")
		return err
	}

	// Process the message in the background
	go func() {
		s.responses <- s.processProduceMessage(header, bodyBuffers)
	}()

	return nil
}

func (s *binaryServer) processProduceMessage(header *binaryHeader, bodyBuffers [][]byte) binaryResponse {
	defer s.bufferPool.Free(bodyBuffers)
	body := utils.NewMultiBufferReader(bodyBuffers, s.bufferPool.BufferSize(), int(header.BodyLength))
	timestampMicros := time.Now().UnixMicro()
	if header.Flags&withTimestamp > 0 {
		ts, err := body.ReadUint64()
		if err != nil {
			return newErrorResponse(err.Error(), header)
		}
		timestampMicros = int64(ts)
	}

	partitionKey, err := body.ReadStringBytes()
	if err != nil {
		return newErrorResponse(err.Error(), header)
	}

	topic, err := body.ReadStringBytes()
	if err != nil {
		return newErrorResponse(err.Error(), header)
	}

	replication := s.leaderGetter.Leader(partitionKey)
	leader := replication.Leader

	if leader == nil {
		return newLeaderNotFoundErrorResponse(replication.Token, header)
	}

	payloadBuffers, payloadLength := body.Bytes()
	if !leader.IsSelf {
		// Route the message as-is
		key := url.Values{}
		if partitionKey != "" {
			key.Set("partitionKey", partitionKey)
		}
		err := s.gossiper.SendToLeader(replication, topic, key, int64(payloadLength), MIMETypeProducerBinary, body)
		if err != nil {
			return newRoutingErrorResponse(err, header)
		}
		return &emptyResponse{streamId: header.StreamId, op: produceResponseOp}
	}

	coalescer := s.coalescerGetter.Coalescer(topic, replication.Token, replication.RangeIndex)
	if err := coalescer.append(replication, uint32(payloadLength), timestampMicros, MIMETypeProducerBinary, payloadBuffers); err != nil {
		return newErrorResponse(err.Error(), header)
	}

	return &emptyResponse{streamId: header.StreamId, op: produceResponseOp}
}
