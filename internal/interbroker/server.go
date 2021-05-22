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
	"github.com/julienschmidt/httprouter"
	"github.com/rs/zerolog/log"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
)

const maxDataResponseSize = 1024

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
	address := g.address(port)

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
	address := g.address(port)

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

func (g *gossiper) address(port int) string {
	address := fmt.Sprintf(":%d", port)

	if !g.config.ListenOnAllAddresses() {
		info := g.discoverer.GetBrokerInfo()
		// Use the provided name / address
		address = fmt.Sprintf("%s:%d", info.HostName, port)
	}

	return address
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
	c := bufio.NewReader(s.conn)
	for {
		if _, err := io.ReadFull(c, headerBuf); err != nil {
			log.Warn().Msg("There was an error reading header from peer")
			break
		}
		header := header{}
		if err := binary.Read(bytes.NewReader(headerBuf), binary.BigEndian, &header); err != nil {
			log.Warn().Msg("Invalid data header from peer, closing connection")
			break
		}

		bodyBuf := largeBodyBuf[:header.BodyLength]
		if _, err := io.ReadFull(c, bodyBuf); err != nil {
			log.Warn().Msg("There was an error reading header from peer")
			break
		}

		if !s.initialized {
			s.initialized = true
			// It's the first message
			if header.Op != startupOp {
				//TODO: s.writeError("Invalid first message")
				break
			}
		}
	}
	_ = s.conn.Close()
}

func (s *peerDataServer) writeResponses() {
	// TODO: Coalesce responses and disable Nagle

	// Response buffer
	buffer := make([]byte, maxDataResponseSize)
	for response := range s.responses {
		if _, err := io.CopyBuffer(s.conn, response, buffer); err != nil {
			log.Warn().Msg("There was an error while writing to peer, closing connection")
			break
		}
	}

	_ = s.conn.Close()
}

type dataResponse interface {
	// We could use another abstraction when implementing message coalescing
	io.Reader
}

type errorResponse struct {
	message  string
	streamId uint16
}

func (r *errorResponse) Read(p []byte) (n int, err error) {
	message := []byte(r.message)
	if len(message)+headerSize > len(p) {
		return 0, io.ErrShortBuffer
	}

	buffer := bytes.NewBuffer(p)
	buffer.Reset()
	header := header{
		Version:    1,
		StreamId:   r.streamId,
		Op:         errorOp,
		BodyLength: uint32(len(message)),
	}
	err = binary.Write(buffer, binary.BigEndian, header)
	buffer.Write(message)
	n = 0
	if err == nil {
		n = buffer.Len()
	}
	return
}
