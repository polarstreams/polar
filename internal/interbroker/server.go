package interbroker

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"strings"

	"github.com/barcostreams/barco/internal/conf"
	"github.com/barcostreams/barco/internal/data"
	"github.com/barcostreams/barco/internal/metrics"
	. "github.com/barcostreams/barco/internal/types"
	. "github.com/barcostreams/barco/internal/utils"
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
		MaxConcurrentStreams: 16384,
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
			router.POST(conf.GossipBrokerIdentifyUrl, ToPostHandle(g.postBrokerIdentifyHandler))
			router.GET(fmt.Sprintf(conf.GossipGenerationUrl, ":token"), ToHandle(g.getGenHandler))
			router.POST(fmt.Sprintf(conf.GossipGenerationProposeUrl, ":token"), ToPostHandle(g.postGenProposeHandler))
			router.POST(fmt.Sprintf(conf.GossipGenerationCommmitUrl, ":token"), ToPostHandle(g.postGenCommitHandler))
			router.POST(conf.GossipGenerationSplitUrl, ToPostHandle(g.postGenSplitHandler))
			router.GET(fmt.Sprintf(conf.GossipTokenInRange, ":token"), ToHandle(g.getTokenInRangeHandler))
			router.GET(fmt.Sprintf(conf.GossipTokenHasHistoryUrl, ":token"), ToHandle(g.getTokenHasHistoryUrl))
			router.GET(fmt.Sprintf(conf.GossipTokenGetHistoryUrl, ":token"), ToHandle(g.getTokenHistoryUrl))
			router.GET(fmt.Sprintf(
				conf.GossipReadProducerOffsetUrl,
				":topic",
				":token",
				":rangeIndex",
				":version"), ToHandle(g.getProducerOffset))
			router.GET(fmt.Sprintf(conf.GossipHostIsUpUrl, ":broker"), ToHandle(g.getBrokerIsUpHandler))

			router.POST(conf.GossipConsumerGroupsInfoUrl, ToPostHandle(g.postConsumerGroupInfo))
			router.POST(conf.GossipConsumerOffsetUrl, ToPostHandle(g.postConsumerOffset))

			// Routing message is part of gossip but it's usually made using a different client connection
			router.POST(fmt.Sprintf(conf.RoutingMessageUrl, ":topic"), ToPostHandle(g.postReroutingHandler))

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

	committed, proposed := g.discoverer.GenerationProposed(Token(token))
	w.Header().Set("Content-Type", contentType)

	// Return an array of generations w/ committed in the first position
	result := make([]Generation, 2)
	if committed != nil {
		result[0] = *committed
	}
	if proposed != nil {
		result[1] = *proposed
	}

	PanicIfErr(json.NewEncoder(w).Encode(result), "Unexpected error when serializing generation")

	return nil
}

func (g *gossiper) postGenProposeHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) error {
	var message GenerationProposeMessage
	if err := json.NewDecoder(r.Body).Decode(&message); err != nil {
		return err
	}
	// Use the registered listener
	return g.genListener.OnRemoteSetAsProposed(message.Generation, message.Generation2, message.ExpectedTx)
}

func (g *gossiper) postGenCommitHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) error {
	var m GenerationCommitMessage
	if err := json.NewDecoder(r.Body).Decode(&m); err != nil {
		return err
	}
	// Use the registered listener
	return g.genListener.OnRemoteSetAsCommitted(m.Token1, m.Token2, m.Tx, m.Origin)
}

func (g *gossiper) postGenSplitHandler(w http.ResponseWriter, r *http.Request, _ httprouter.Params) error {
	var origin int
	if err := json.NewDecoder(r.Body).Decode(&origin); err != nil {
		return err
	}
	// Use the registered listener
	return g.genListener.OnRemoteRangeSplitStart(origin)
}

func (g *gossiper) postBrokerIdentifyHandler(w http.ResponseWriter, r *http.Request, _ httprouter.Params) error {
	var ordinal int
	if err := json.NewDecoder(r.Body).Decode(&ordinal); err != nil {
		return err
	}

	// We've got a message from a peer, it's ready to accept connections
	clientInfo := g.getClientInfo(ordinal)

	// Checking whether it's UP before sending the message as ready is prone to race conditions
	// But it's just a mechanism to avoid waiting, if possible, otherwise it will still reconnect
	if clientInfo != nil && !clientInfo.isHostUp() {
		clientInfo.readyNewGossipConnection <- true
		clientInfo.readyNewDataConnection <- true
	}

	return nil
}

func (g *gossiper) getBrokerIsUpHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) error {
	broker, err := strconv.ParseInt(ps.ByName("broker"), 10, 64)
	if err != nil {
		return err
	}

	w.Header().Set("Content-Type", contentType)
	// Encode can't fail for a bool
	_ = json.NewEncoder(w).Encode(g.IsHostUp(int(broker)))
	return nil
}

func (g *gossiper) getTokenInRangeHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) error {
	token, err := strconv.ParseInt(strings.TrimSpace(ps.ByName("token")), 10, 64)
	if err != nil {
		return err
	}

	w.Header().Set("Content-Type", contentType)
	// Encode can't fail for a bool
	_ = json.NewEncoder(w).Encode(g.discoverer.IsTokenInRange(Token(token)))
	return nil
}

func (g *gossiper) getTokenHasHistoryUrl(w http.ResponseWriter, r *http.Request, ps httprouter.Params) error {
	token, err := strconv.ParseInt(strings.TrimSpace(ps.ByName("token")), 10, 64)
	if err != nil {
		return err
	}

	result, err := g.discoverer.HasTokenHistory(Token(token))
	if err != nil {
		return err
	}

	w.Header().Set("Content-Type", contentType)
	// Encode can't fail for a bool
	_ = json.NewEncoder(w).Encode(result)
	return nil
}

func (g *gossiper) getTokenHistoryUrl(w http.ResponseWriter, r *http.Request, ps httprouter.Params) error {
	token, err := strconv.ParseInt(strings.TrimSpace(ps.ByName("token")), 10, 64)
	if err != nil {
		return err
	}

	gen, err := g.discoverer.GetTokenHistory(Token(token))
	if err != nil {
		return err
	}

	w.Header().Set("Content-Type", contentType)
	PanicIfErr(json.NewEncoder(w).Encode(gen), "Unexpected error when serializing generation")
	return nil
}

func (g *gossiper) getProducerOffset(w http.ResponseWriter, r *http.Request, ps httprouter.Params) error {
	topic := ps.ByName("topic")
	if topic == "" {
		return fmt.Errorf("Empty topic")
	}
	token, err := strconv.ParseInt(ps.ByName("token"), 10, 64)
	if err != nil {
		return err
	}
	rangeIndex, err := strconv.ParseUint(ps.ByName("rangeIndex"), 10, 8)
	if err != nil {
		return err
	}
	version, err := strconv.ParseUint(ps.ByName("version"), 10, 32)
	if err != nil {
		return err
	}
	topicId := TopicDataId{
		Name:       topic,
		Token:      Token(token),
		RangeIndex: RangeIndex(rangeIndex),
		GenId:      GenVersion(version),
	}

	value, err := data.ReadProducerOffset(&topicId, g.config)
	if err != nil {
		return err
	}
	w.Header().Set("Content-Type", contentType)
	// Encode can't fail for a bool
	_ = json.NewEncoder(w).Encode(value)
	return nil
}

func (g *gossiper) postConsumerGroupInfo(w http.ResponseWriter, r *http.Request, ps httprouter.Params) error {
	var message ConsumerGroupInfoMessage
	if err := json.NewDecoder(r.Body).Decode(&message); err != nil {
		return err
	}
	// Use the registered listener
	g.consumerInfoListener.OnConsumerInfoFromPeer(message.Origin, message.Groups)
	return nil
}

func (g *gossiper) postConsumerOffset(w http.ResponseWriter, r *http.Request, ps httprouter.Params) error {
	var message OffsetStoreKeyValue
	if err := json.NewDecoder(r.Body).Decode(&message); err != nil {
		return err
	}
	// Use the registered listener
	g.consumerInfoListener.OnOffsetFromPeer(&message)
	return nil
}

func (g *gossiper) postReroutingHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) error {
	metrics.ReroutedReceived.Inc()
	topic := ps.ByName("topic")
	return g.reroutingListener.OnReroutedMessage(topic, r.URL.Query(), r.ContentLength, r.Body)
}
