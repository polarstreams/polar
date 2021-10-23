package interbroker

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"strings"

	"github.com/jorgebay/soda/internal/conf"
	. "github.com/jorgebay/soda/internal/types"
	. "github.com/jorgebay/soda/internal/utils"
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
	address := GetServiceAddress(port, g.discoverer.LocalInfo(), g.config)

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
			router.GET(conf.StatusUrl, func(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
				fmt.Fprintf(w, "Peer listening on %d\n", port)
			})
			router.GET(fmt.Sprintf(conf.GossipGenerationUrl, ":token"), ToHandle(g.getGenHandler))
			router.POST(fmt.Sprintf(conf.GossipGenerationProposeUrl, ":token"), ToPostHandle(g.postGenProposeHandler))
			router.POST(fmt.Sprintf(conf.GossipGenerationCommmitUrl, ":token"), ToPostHandle(g.postGenCommitHandler))
			router.GET(fmt.Sprintf(conf.GossipTokenInRange, ":token"), ToHandle(g.getTokenInRangeHandler))

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

func (g *gossiper) getGenHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) error {
	token, err := strconv.ParseInt(strings.TrimSpace(ps.ByName("token")), 10, 64)
	if err != nil {
		return err
	}

	if result, err := g.localDb.GetGenerationsByToken(Token(token)); err == nil {
		w.Header().Set("Content-Type", "application/json")
		PanicIfErr(json.NewEncoder(w).Encode(result), "Unexpected error when serializing generation")

	} else {
		return err
	}

	return nil
}

func (g *gossiper) postGenProposeHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) error {
	if _, err := strconv.ParseInt(strings.TrimSpace(ps.ByName("token")), 10, 64); err != nil {
		return err
	}
	var message GenerationProposeMessage
	if err := json.NewDecoder(r.Body).Decode(&message); err != nil {
		return err
	}
	// Use the registered listener
	return g.genListener.OnRemoteSetAsProposed(message.Generation, message.ExpectedTx)
}

func (g *gossiper) postGenCommitHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) error {
	token, err := strconv.ParseInt(strings.TrimSpace(ps.ByName("token")), 10, 64)
	if err != nil {
		return err
	}
	var message GenerationCommitMessage
	if err := json.NewDecoder(r.Body).Decode(&message); err != nil {
		return err
	}
	// Use the registered listener
	return g.genListener.OnRemoteSetAsCommitted(Token(token), message.Tx, message.Origin)
}

func (g *gossiper) getTokenInRangeHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) error {
	token, err := strconv.ParseInt(strings.TrimSpace(ps.ByName("token")), 10, 64)
	if err != nil {
		return err
	}

	w.Header().Set("Content-Type", "application/json")
	// Encode can't fail for a bool
	_ = json.NewEncoder(w).Encode(g.discoverer.IsTokenInRange(Token(token)))
	return nil
}
