package kafka_api

import (
	"bufio"
	"encoding/binary"
	"net"

	"github.com/polarstreams/polar/internal/conf"
	"github.com/polarstreams/polar/internal/discovery"
	"github.com/polarstreams/polar/internal/localdb"
	. "github.com/polarstreams/polar/internal/types"
	"github.com/polarstreams/polar/internal/utils"
	"github.com/rs/zerolog/log"
)

const receiveBufferSize = 32 * 1024

type KafkaServer interface {
	Initializer
	Closer

	// Starts accepting connections from Kafka clients.
	AcceptConnections() error
}

func NewKafkaServer(config conf.BasicConfig, localDb localdb.Client, discoverer discovery.Discoverer) KafkaServer {
	s := &kafkaServer{
		config:     config,
		discoverer: discoverer,
		localDb:    localDb,
	}
	return s
}

type kafkaServer struct {
	config     conf.BasicConfig
	discoverer discovery.Discoverer
	listener   net.Listener
	localDb    localdb.Client
}

func (s *kafkaServer) Init() error {
	return nil
}

func (s *kafkaServer) Close() {
	_ = s.listener.Close()
}

func (s *kafkaServer) AcceptConnections() error {
	port := s.config.KafkaPort()
	address := utils.GetServiceAddress(port, s.discoverer.LocalInfo(), s.config)

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
				if !s.localDb.IsShuttingDown() {
					log.Err(err).Msgf("Failed to accept new Kafka client connection")
				}
				break
			}

			log.Debug().Msgf("Accepted new Kafka client connection on %v", conn.LocalAddr())
			s.handleClient(conn)
		}
	}()

	<-c
	s.listener = listener
	log.Info().Msgf("Start listening to Kafka clients on %s", address)

	return nil
}

func (s *kafkaServer) handleClient(conn net.Conn) {
	h := &connectionHandler{
		conn:      conn,
		s:         s,
		responses: make(chan response, 32),
	}
	go h.serve()
	go h.writeResponses()
}

type connectionHandler struct {
	conn      net.Conn
	s         *kafkaServer
	responses chan response
}

func (h *connectionHandler) serve() {
	const concurrentRequests = 32
	reader := bufio.NewReaderSize(h.conn, receiveBufferSize)
	buffer := make([]byte, 32*1024) // max string length
	canProcess := make(chan bool, concurrentRequests)
	for i := 0; i < concurrentRequests; i++ {
		canProcess <- true
	}

	for {
		header, err := readRequestHeader(reader, buffer)
		if err != nil {
			log.Warn().Err(err).Msg("There was an error reading header from kafka client")
			break
		}
		request, err := readRequest(reader, header, buffer)
		if err != nil {
			log.Warn().Err(err).Msgf("There was an error reading request (%d) from kafka client", header.fixed.ApiKey)
			break
		}
		<-canProcess
		go func() {
			response := h.processRequest(header, request)
			h.responses <- response
			canProcess <- true
		}()
	}
	_ = h.conn.Close()
}

func (h *connectionHandler) processRequest(header *requestHeader, r request) response {
	response, err := r.Process(header, h.s)
	if err != nil {
		log.Warn().Err(err).
			Msgf("There was an error processing request (%d) from kafka client", header.fixed.ApiKey)
	}
	return response
}

func (h *connectionHandler) writeResponses() {
	const correlationIdSize = 4
	// TODO: TRIVIAL implementation, it should consider buffering
	for response := range h.responses {
		// Write size and stream id
		if err := binary.Write(h.conn, endianess, response.Size()+correlationIdSize); err != nil {
			log.Warn().Err(err).Msgf("There was an error writing the response size to a kafka client")
		}
		if err := binary.Write(h.conn, endianess, response.CorrelationId()); err != nil {
			log.Warn().Err(err).Msgf("There was an error writing the response id to a kafka client")
		}
		if err := response.Write(h.conn); err != nil {
			log.Warn().Err(err).Msgf("There was an error writing a response to a kafka client")
		}
		log.Debug().Msgf("--Written response")
	}
	_ = h.conn.Close()
}
