package interbroker

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/http"

	"github.com/jorgebay/soda/internal/conf"
	"github.com/jorgebay/soda/internal/metrics"
	"github.com/jorgebay/soda/internal/utils"
	"github.com/julienschmidt/httprouter"
	"github.com/rs/zerolog/log"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
)

const maxDataResponseSize = 1024
const receiveBufferSize = 32 * 1024

func (g *gossiper) AcceptConnections() error {
	if err := g.acceptHttpConnections(); err != nil {
		return err
	}

	if err := g.acceptDataConnections(); err != nil {
		return err
	}

	return nil
}

func (g *gossiper) acceptHttpConnections() error {
	server := &http2.Server{
		MaxConcurrentStreams: 2048,
	}
	port := g.config.GossipPort()
	address := utils.GetServiceAddress(port, g.discoverer, g.config)

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
				log.Err(err).Msgf("Failed to accept new connections")
				break
			}

			log.Debug().Msgf("Accepted new gossip http connection on %v", conn.LocalAddr())

			router := httprouter.New()
			router.GET("/status", func(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
				fmt.Fprintf(w, "Peer listening on %d\n", port)
			})

			//TODO: routes to propose/accept new generation

			// server.ServeConn() will block until the connection is not readable anymore
			// start it in the background
			go func() {
				server.ServeConn(conn, &http2.ServeConnOpts{
					Handler: h2c.NewHandler(router, server),
				})
			}()
		}
	}()

	<-c

	log.Info().Msgf("Start listening to peers for http requests on port %d", port)

	return nil
}

// acceptDataConnections starts listening to TCP connections for data
func (g *gossiper) acceptDataConnections() error {
	port := g.config.GossipDataPort()
	address := utils.GetServiceAddress(port, g.discoverer, g.config)

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
	s := &peerDataServer{
		conn:      conn,
		config:    g.config,
		responses: make(chan dataResponse, 256),
	}
	go s.serve()
	go s.writeResponses()
}

type peerDataServer struct {
	conn        net.Conn
	initialized bool
	config      conf.GossipConfig
	responses   chan dataResponse
}

func (s *peerDataServer) serve() {
	headerBuf := make([]byte, headerSize)
	largeBodyBuf := make([]byte, s.config.MaxDataBodyLength())
	c := bufio.NewReaderSize(s.conn, receiveBufferSize)
	for {
		if n, err := io.ReadFull(c, headerBuf); err != nil {
			log.Warn().Err(err).Int("n", n).Msg("There was an error reading header from peer client")
			break
		}
		header, err := readHeader(headerBuf)
		if err != nil {
			log.Warn().Msg("Invalid data header from peer, closing connection")
			break
		}

		bodyBuf := largeBodyBuf[:header.BodyLength]
		if _, err := io.ReadFull(c, bodyBuf); err != nil {
			log.Warn().Msg("There was an error reading body from peer")
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

		if header.Op != dataOp {
			s.responses <- newErrorResponse("Only data operations are supported", header)
			break
		}

		request, err := s.parseDataRequest(bodyBuf)

		if err != nil {
			s.responses <- newErrorResponse("Parsing error", header)
		} else {
			if err = s.append(request); err == nil {
				s.responses <- &emptyResponse{streamId: header.StreamId, op: dataResponseOp}
			} else {
				s.responses <- newErrorResponse(fmt.Sprintf("Append error: %s", err.Error()), header)
			}
		}
	}
	log.Info().Msg("Data server reader closing connection")
	_ = s.conn.Close()
}

func (s *peerDataServer) append(*dataRequest) error {
	metrics.InterbrokerReceivedMessages.Inc()
	// TODO Implement
	return nil
}

func (s *peerDataServer) parseDataRequest(body []byte) (*dataRequest, error) {
	meta := dataRequestMeta{}
	reader := bytes.NewReader(body)
	if err := binary.Read(reader, conf.Endianness, &meta); err != nil {
		return nil, err
	}

	index := dataRequestMetaSize
	topic := string(body[index : index+int(meta.TopicLength)])
	index += int(meta.TopicLength)
	request := &dataRequest{
		meta:  meta,
		topic: topic,
		data:  body[index:],
	}

	return request, nil
}

func (s *peerDataServer) writeResponses() {
	// TODO: Coalesce responses and disable Nagle

	w := bytes.NewBuffer(make([]byte, 0, maxDataResponseSize))
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
