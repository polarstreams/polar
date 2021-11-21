package consuming

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/jorgebay/soda/internal/conf"
	"github.com/jorgebay/soda/internal/discovery"
	"github.com/jorgebay/soda/internal/interbroker"
	. "github.com/jorgebay/soda/internal/types"
	. "github.com/jorgebay/soda/internal/utils"
	"github.com/julienschmidt/httprouter"
	"github.com/rs/zerolog/log"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
)

const consumerGroupsToPeersDelay = 10 * time.Second

var addDebouncer = Debounce(10*time.Second, 0)
var removeDebouncer = Debounce(removeDelay, 0.4) // Debounce events that occurred in the following 2 minutes

// Consumer represents a consumer server
type Consumer interface {
	Initializer

	AcceptConnections() error
}

func NewConsumer(
	config conf.ConsumerConfig,
	topologyGetter discovery.TopologyGetter,
	gossiper interbroker.Gossiper,
) Consumer {
	return &consumer{
		config:         config,
		topologyGetter: topologyGetter,
		gossiper:       gossiper,
		meta:           NewConsumersMeta(topologyGetter),
	}
}

type consumer struct {
	config         conf.ConsumerConfig
	topologyGetter discovery.TopologyGetter
	gossiper       interbroker.Gossiper
	meta           *ConsumersMeta
}

func (c *consumer) Init() error {
	c.gossiper.RegisterConsumerInfoListener(c)

	// Send info in the background
	go c.sendConsumerGroupsToPeers()
	return nil
}

func (c *consumer) AcceptConnections() error {
	server := &http2.Server{}
	port := c.config.ConsumerPort()
	address := GetServiceAddress(port, c.topologyGetter.LocalInfo(), c.config)

	listener, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}

	startChan := make(chan bool, 1)
	go func() {
		startChan <- true
		for {
			// HTTP/2 only server (prior knowledge)
			conn, err := listener.Accept()
			if err != nil {
				log.Err(err).Msgf("Failed to accept new connections")
				break
			}

			log.Debug().Msgf("Accepted new consumer http connection on %s", conn.LocalAddr().String())

			router := httprouter.New()
			router.GET(conf.StatusUrl, func(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
				fmt.Fprintf(w, "Consumer server listening on %d\n", port)
			})

			trackedConn := NewTrackedConnection(conn, func(trackedConn *TrackedConnection) {
				log.Info().Msgf("Connection from consumer client %s closed", trackedConn.LocalAddr().String())
				c.unRegister(trackedConn)
			})

			router.POST(conf.ConsumerRegisterUrl, toPostHandler(trackedConn, c.postRegister))
			router.POST(conf.ConsumerPollUrl, toPostHandler(trackedConn, c.postPoll))

			// server.ServeConn() will block until the connection is not readable anymore
			// start it in the background to accept further connections
			go func() {
				server.ServeConn(conn, &http2.ServeConnOpts{
					Handler: h2c.NewHandler(router, server),
				})
			}()
		}
	}()

	<-startChan

	log.Info().Msgf("Start listening to consumers for http requests on port %d", port)
	return nil
}

func (c *consumer) postRegister(
	conn *TrackedConnection,
	w http.ResponseWriter,
	r *http.Request,
	ps httprouter.Params,
) error {
	var consumerInfo ConsumerInfo
	if err := json.NewDecoder(r.Body).Decode(&consumerInfo); err != nil {
		return err
	}
	c.meta.AddConnection(conn.Id(), consumerInfo)

	addDebouncer(func() {
		if c.meta.Rebalance() {
			log.Info().Msg("Consumer topology was rebalanced after adding a new consumer")
		}
	})

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write([]byte("OK"))
	return nil
}

func (c *consumer) unRegister(conn *TrackedConnection) {
	c.meta.RemoveConnection(conn.Id())

	// We shouldn't rush to rebalance
	removeDebouncer(func() {
		if c.meta.Rebalance() {
			log.Info().Msg("Consumer topology was rebalanced after adding a new consumer")
		}
	})
}

func (c *consumer) postPoll(
	conn *TrackedConnection,
	w http.ResponseWriter,
	r *http.Request,
	ps httprouter.Params,
) error {
	group, tokens, topics := c.meta.CanConsume(conn.Id())
	if len(tokens) == 0 {
		// TODO: Write the response
		_, _ = w.Write([]byte("NO TOKENS"))
		return nil
	}

	myOrdinal := c.topologyGetter.Topology().MyOrdinal()
	ownedTokens := make([]Token, 0)

	for _, token := range tokens {
		gen := c.topologyGetter.Generation(token)
		if gen != nil && gen.Leader == myOrdinal {
			ownedTokens = append(ownedTokens, token)
		}
	}

	if len(ownedTokens) == 0 {
		_, _ = w.Write([]byte("NO OWNED TOKENS"))
		return nil
	}

	// TODO: Check can consume from local token data
	// When there's an split token range, it should check with previous broker
	// To see whether it can serve to this consumer group (e.g. B3 should check with B0 about T3 for consumer group X)

	for _, topic := range topics {
		for _, token := range tokens {
			// TODO: get or create readers per group/topic/token
			// Most likely issue reads in a single channel
			fmt.Println("--Temp", token, topic, group)
		}
	}

	return nil
}

type ConnAwareHandle func(*TrackedConnection, http.ResponseWriter, *http.Request, httprouter.Params) error

func toPostHandler(c *TrackedConnection, h ConnAwareHandle) httprouter.Handle {
	return func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		if err := h(c, w, r, ps); err != nil {
			adaptHttpErr(err, w)
		}
	}
}

func adaptHttpErr(err error, w http.ResponseWriter) {
	httpErr, ok := err.(HttpError)

	if !ok {
		log.Err(err).Msg("Unexpected error when consuming")
		http.Error(w, "Internal server error", 500)
		return
	}

	w.WriteHeader(httpErr.StatusCode())
	// The message is supposed to be user friendly
	fmt.Fprintf(w, err.Error())
}

func (c *consumer) OnConsumerInfoFromPeer(ordinal int, groups []ConsumerGroup) {
	c.meta.SetInfoFromPeer(ordinal, groups)
}

func (c *consumer) sendConsumerGroupsToPeers() {
	const sendPeriod = 1000
	for i := 0; ; i++ {
		groups := c.meta.GetInfoForPeers()
		if len(groups) > 0 {
			topology := c.topologyGetter.Topology()
			brokers := topology.NextBrokers(topology.LocalIndex, 2)
			logEvent := log.Debug()
			if i%sendPeriod == 0 {
				logEvent = log.Info()
			}
			for i := 0; i < len(brokers); i++ {
				b := brokers[i]
				logEvent.Int("broker", b.Ordinal)
				go func() {
					err := c.gossiper.SendConsumerGroups(b.Ordinal, groups)
					if err != nil {
						log.Warn().Err(err).Msgf(
							"The was an error when sending consumer group info to peer B%d", b.Ordinal)
					}
				}()
			}
			logEvent.Int("groups", len(groups)).Msgf("Sending consumer group snapshot to peers")
		}
		time.Sleep(Jitter(consumerGroupsToPeersDelay))
	}
}
