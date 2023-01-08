package producing

import (
	"bytes"
	"encoding/binary"
	"io"
	"net"

	"github.com/polarstreams/polar/internal/conf"
	"github.com/polarstreams/polar/internal/producing/pooling"
	"github.com/polarstreams/polar/internal/utils"
	"github.com/rs/zerolog/log"
)

func (p *producer) AcceptBinaryConnections() error {
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
	// TODO: CREATE BINARY SERVER
}

type binaryServer struct {
	bufferPool  pooling.BufferPool
	conn        io.ReadCloser
	initialized bool
	responses   chan binaryResponse
}

func (s *binaryServer) serve() {
	headerBuf := make([]byte, binaryHeaderSize)
	header := &binaryHeader{} // Reuse allocation
	reader := bytes.NewReader(headerBuf)

	for {
		if n, err := io.ReadFull(s.conn, headerBuf); err != nil {
			log.Warn().Err(err).Int("n", n).Msg("There was an error reading header from peer client")
			break
		}
		err := binary.Read(reader, conf.Endianness, header)
		if err != nil {
			log.Warn().Msg("Invalid data header from producer client, closing connection")
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

		s.responses <- newErrorResponse("Only data replication operations are supported", header)

	}
	log.Info().Msg("Data server reader closing connection")
	_ = s.conn.Close()
}

func (s *binaryServer) handleProduceMessage(header *binaryHeader) error {
	bodyBuffers := s.bufferPool.Get(int(header.BodyLength))
	if err := s.readBody(bodyBuffers); err != nil {
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
	if header.Flags&withTimestamp > 0 {
		err := binary.Read()

	}

	/*
		timestampMicros := time.Now().UnixMicro()
		if timestamp := querystring.Get("timestamp"); timestamp != "" {
			if n, err := strconv.ParseInt(timestamp, 10, 64); err != nil {
				timestampMicros = n
			}
		}

		coalescer := p.getCoalescer(topic, replication.Token, replication.RangeIndex)
		if err := coalescer.append(replication, uint32(bodyLength), timestampMicros, contentType, buffers); err != nil {
			return p.adaptCoalescerError(err)
		}
	*/

	return &emptyResponse{streamId: header.StreamId, op: produceResponseOp}
}

func (s *binaryServer) readBody(buffers [][]byte) error {
	// TODO: ADAPT TO USE LENGTH
	length := 0
	for i, b := range buffers {
		n, err := io.ReadFull(s.conn, b)
		length += n
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				if i == len(buffers)-1 {
					// EOF is expected on the last buffer
					return nil
				}
			}
			return err
		}
	}

	return nil
}
