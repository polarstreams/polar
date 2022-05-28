package consuming

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/barcostreams/barco/internal/conf"
	"github.com/barcostreams/barco/internal/discovery"
	"github.com/barcostreams/barco/internal/interbroker"
	"github.com/barcostreams/barco/internal/localdb"
	. "github.com/barcostreams/barco/internal/types"
	. "github.com/barcostreams/barco/internal/utils"
	. "github.com/google/uuid"
	"github.com/julienschmidt/httprouter"
	"github.com/rs/zerolog/log"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
)

const consumerGroupsToPeersDelay = 10 * time.Second

const (
	consumerNoOwnedDataDelay = 5 // Seconds to retry after
	consumerNoDataDelay      = 1 // Seconds to retry after
)

const contentType = "application/vnd.barco.consumermessage"

var removeDebouncer = Debounce(removeDelay, 0.4) // Debounce events that occurred in the following 2 minutes

// Consumer represents a consumer server
type Consumer interface {
	Initializer
	Closer

	AcceptConnections() error
}

func NewConsumer(
	config conf.ConsumerConfig,
	localDb localdb.Client,
	topologyGetter discovery.TopologyGetter,
	gossiper interbroker.Gossiper,
) Consumer {
	addDelay := config.ConsumerAddDelay()
	if config.DevMode() {
		addDelay = 20 * time.Millisecond // Don't wait for rebalancing in dev mode: faster round trips
	}

	return &consumer{
		config:         config,
		topologyGetter: topologyGetter,
		gossiper:       gossiper,
		localDb:        localDb,
		rrFactory:      newReplicationReaderFactory(gossiper),
		state:          NewConsumerState(config, topologyGetter),
		offsetState:    newDefaultOffsetState(localDb, topologyGetter, gossiper, config),
		readQueues:     NewCopyOnWriteMap(),
		addDebouncer:   Debounce(addDelay, 0),
	}
}

type consumer struct {
	config         conf.ConsumerConfig
	topologyGetter discovery.TopologyGetter
	gossiper       interbroker.Gossiper
	rrFactory      ReplicationReaderFactory
	localDb        localdb.Client
	state          *ConsumerState
	offsetState    OffsetState
	readQueues     *CopyOnWriteMap
	addDebouncer   Debouncer
	listener       net.Listener
}

func (c *consumer) Init() error {
	c.gossiper.RegisterConsumerInfoListener(c)
	if err := c.offsetState.Init(); err != nil {
		return err
	}

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
			// HTTP/2-only server (prior knowledge)
			conn, err := listener.Accept()
			if err != nil {
				if !c.localDb.IsShuttingDown() {
					log.Err(err).Msgf("Failed to accept new connections")
				}
				break
			}

			log.Debug().Msgf("Accepted new consumer http connection on %s", conn.LocalAddr().String())

			router := httprouter.New()
			router.GET(conf.StatusUrl, func(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
				fmt.Fprintf(w, "Consumer server listening on %d\n", port)
			})

			router.GET("/test/delay", func(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
				time.Sleep(10 * time.Second)
				fmt.Fprintln(w, "Delayed response after 10s")
			})

			trackedConn := NewTrackedConnection(conn, func(trackedConn *TrackedConnection) {
				log.Info().Msgf("Connection from consumer client")
				c.unRegister(trackedConn)
			})

			log.Debug().Msgf("Consumer client connection open with assigned id '%s'", trackedConn.Id())

			router.POST(conf.ConsumerRegisterUrl, toPostHandler(trackedConn, c.postRegister))

			router.POST(conf.ConsumerPollUrl, func(w http.ResponseWriter, _ *http.Request, _ httprouter.Params) {
				c.postPoll(trackedConn, w)
			})

			// server.ServeConn() will block until the connection is not readable anymore
			// start it in the background to accept further connections
			go func() {
				server.ServeConn(conn, &http2.ServeConnOpts{
					Handler: h2c.NewHandler(router, server),
				})
				if !c.localDb.IsShuttingDown() {
					log.Debug().Msgf(
						"Connection from consumer client with id '%s' is not readable anymore", trackedConn.Id())
				}
				trackedConn.Close()
			}()
		}
	}()

	<-startChan
	c.listener = listener

	log.Info().Msgf("Start listening to consumers for http requests on port %d", port)
	return nil
}

func (c *consumer) Close() {
	err := c.listener.Close()
	log.Err(err).Msgf("Consumer connection listener closed")
	connections := c.state.GetConnections()
	for _, conn := range connections {
		conn.Close()
	}
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
	added := c.state.AddConnection(conn, consumerInfo)

	if !added {
		return nil
	}

	log.Info().
		Stringer("connId", conn.Id()).
		Msgf("Registering new connection to consumer '%s' of group '%s' for topics %v",
			consumerInfo.Id, consumerInfo.Group, consumerInfo.Topics)

	c.addDebouncer(func() {
		if c.state.Rebalance() {
			log.Info().Msg("Consumer topology was rebalanced after adding a new consumer connection registered")
			log.Debug().Msgf("Consumer topology contains consumer groups %v and consumers by connection: %v)",
				c.state.groups, c.state.consumers)
		}
	})

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write([]byte("OK"))
	return nil
}

func (c *consumer) unRegister(conn *TrackedConnection) {
	c.state.RemoveConnection(conn.Id())

	// We shouldn't rush to rebalance
	removeDebouncer(func() {
		if c.state.Rebalance() {
			log.Info().Msg("Consumer topology was rebalanced after removing a connection to a consumer client")
		}
	})
}

func (c *consumer) postPoll(conn *TrackedConnection, w http.ResponseWriter) {
	group, tokens, _ := logsToServe(c.state, c.topologyGetter, conn.Id())
	if len(tokens) == 0 {
		log.Debug().Msgf("Received consumer client poll from connection '%s' with no assigned tokens", conn.Id())
		NoContentResponse(w, consumerNoOwnedDataDelay)
		return
	}

	log.Debug().Msgf("Received consumer client poll from connection '%s'", conn.Id())

	grq, _, _ := c.readQueues.LoadOrStore(group, func() (interface{}, error) {
		return newGroupReadQueue(group, c.state, c.offsetState, c.topologyGetter, c.gossiper, c.rrFactory, c.config), nil
	})

	groupReadQueue := grq.(*groupReadQueue)
	groupReadQueue.readNext(conn.Id(), w)
}

type ConnAwareHandle func(*TrackedConnection, http.ResponseWriter, *http.Request, httprouter.Params) error

// Gets the tokens and topics to serve, given a connection.
func logsToServe(
	state *ConsumerState,
	topologyGetter discovery.TopologyGetter,
	connId UUID,
) (string, []TokenRanges, []string) {
	group, tokenRanges, topics := state.CanConsume(connId)
	if len(tokenRanges) == 0 {
		return "", nil, nil
	}

	myOrdinal := topologyGetter.Topology().MyOrdinal()
	leaderTokens := make([]TokenRanges, 0, len(tokenRanges))

	for _, ranges := range tokenRanges {
		gen := topologyGetter.Generation(ranges.Token)
		if gen != nil && gen.Leader == myOrdinal {
			leaderTokens = append(leaderTokens, ranges)
		}
	}

	return group, leaderTokens, topics
}

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
	c.state.SetInfoFromPeer(ordinal, groups)
}

func (c *consumer) OnOffsetFromPeer(kv *OffsetStoreKeyValue) {
	log.Debug().
		Str("group", kv.Key.Group).
		Str("topic", kv.Key.Topic).
		Msgf("Received offset from peer for token %d/%d", kv.Key.Token, kv.Key.RangeIndex)
	c.offsetState.Set(kv.Key.Group, kv.Key.Topic, kv.Key.Token, kv.Key.RangeIndex, kv.Value, OffsetCommitLocal)
}

func (c *consumer) sendConsumerGroupsToPeers() {
	const sendPeriod = 1000
	for i := 0; ; i++ {
		groups := c.state.GetInfoForPeers()
		topology := c.topologyGetter.Topology()
		if len(groups) > 0 && len(topology.Brokers) > 1 {
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
		} else {
			log.Debug().Msgf("No consumer groups to send to peers")
		}
		time.Sleep(Jitter(consumerGroupsToPeersDelay))
	}
}

type readQueueKey struct {
	group string
	topic string
}
