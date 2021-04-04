package interbroker

import (
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/julienschmidt/httprouter"
	"github.com/rs/zerolog/log"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
)

func (g *gossiper) AcceptConnections() error {
	server := &http2.Server{
		MaxConcurrentStreams: 1024,
	}
	port := g.config.GossipPort()

	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
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
				// TODO: Define whether this should exit loop
				log.Warn().Msgf("Failed to accept connection")
				continue
			}

			log.Debug().Msgf("Accepted new gossip connection on %v from %v", conn.LocalAddr(), conn.RemoteAddr())

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

	log.Info().Msgf("Start listening to peers on port %d", port)

	return nil
}
