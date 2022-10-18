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
	"github.com/barcostreams/barco/internal/metrics"
	"github.com/barcostreams/barco/internal/types"
	. "github.com/barcostreams/barco/internal/types"
	"github.com/barcostreams/barco/internal/utils"
	. "github.com/barcostreams/barco/internal/utils"
	"github.com/julienschmidt/httprouter"
	"github.com/rs/zerolog/log"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
)

const consumerGroupsToPeersDelay = 10 * time.Second

const (
	consumerNoOwnedDataDelay    = 5 // Seconds to retry after
	consumerNoDataDelay         = 1 // Seconds to retry after
	halfOpenTimerResolution     = 1 * time.Second
	consumerNotRegisteredStatus = http.StatusConflict
)

const (
	defaultMimeType = "application/vnd.barco.consumermessage"
	jsonMimeType    = "application/json"
)

// The query string parameter used to determine whether it's connection-less
const consumerQueryKey = "consumer_id"
const consumerGroupDefault = "default"

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
	// TODO: Support both HTTP/1 and HTTP/2
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

			tc := newTrackedConsumer(conn)

			router := httprouter.New()
			router.GET(conf.StatusUrl, func(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
				tc.SetAsRead()
				fmt.Fprintf(w, "Consumer server listening on %d\n", port)
			})
			router.PUT(conf.ConsumerRegisterUrl, toTrackedHandler(tc, c.postRegister))
			router.POST(conf.ConsumerRegisterUrl, toTrackedHandler(tc, c.postRegister)) // Backwards compatibility
			router.POST(conf.ConsumerPollUrl, toTrackedHandler(tc, c.postPoll))
			router.POST(conf.ConsumerManualCommitUrl, toTrackedHandler(tc, c.postManualCommit))

			// Detect half-open TCP connections from consumers by getting the last read in loop
			go func() {
				for {
					if time.Since(tc.LastRead()) > c.config.ConsumerReadTimeout() {
						log.Debug().Msgf("Stop tracking consumer '%s' due to inactivity", tc.Id())
						tc.CloseConnection()
						c.unRegister(tc.Id())
						break
					}
					time.Sleep(utils.Jitter(halfOpenTimerResolution))
				}
			}()

			// server.ServeConn() will block until the connection is not readable anymore
			go func() {
				server.ServeConn(conn, &http2.ServeConnOpts{
					Handler: h2c.NewHandler(router, server),
				})
				if c.localDb.IsShuttingDown() {
					return
				}
				if tc.CloseConnection() {
					log.Debug().Msgf(
						"Connection from consumer client with id '%s' is not readable anymore", tc.Id())
				}
			}()
		}
	}()

	<-startChan
	c.listener = listener

	log.Info().Msgf("Start listening to consumers for http requests on port %d", port)
	return nil
}

func (c *consumer) Close() {
	// Stop listening
	err := c.listener.Close()
	log.Err(err).Msgf("Consumer connection listener closed")

	connBoundConsumers := c.state.GetConnectionBoundConsumers()
	for _, conn := range connBoundConsumers {
		conn.CloseConnection()
	}
}

func (c *consumer) postRegister(
	tc *trackedConsumer,
	w http.ResponseWriter,
	r *http.Request,
	_ httprouter.Params,
) error {
	tc.SetAsRead()
	var consumerInfo ConsumerInfo
	rebalanceDelay := true
	if consumerId := r.URL.Query().Get(consumerQueryKey); consumerId != "" {
		tc.RegisterAsConnectionLess(consumerId)
		consumerInfo.Id = consumerId
		consumerInfo.Group = r.URL.Query().Get("group")
		consumerInfo.Topics = r.URL.Query()["topic"]

		// Default to "no rebalance delay" for connection-less
		if r.URL.Query().Get("delay") != "true" {
			rebalanceDelay = false
		}
	} else {
		if err := json.NewDecoder(r.Body).Decode(&consumerInfo); err != nil {
			return types.NewHttpError(http.StatusBadRequest, "Invalid ConsumerInfo payload")
		}
		tc.RegisterAsConnectionBound()
	}

	if consumerInfo.Group == "" {
		consumerInfo.Group = consumerGroupDefault
	}

	if consumerInfo.Id == "" || len(consumerInfo.Topics) == 0 {
		return types.NewHttpError(http.StatusBadRequest, "Consumer id and topics can not be empty")
	}

	added, length := c.state.AddConnection(tc, consumerInfo)
	if !added {
		return nil
	}

	metrics.ConsumerConnections.Set(float64(length))
	log.Debug().
		Str("connId", tc.Id()).
		Int("connections", length).
		Msgf("Registering new connection to consumer '%s' of group '%s' for topics %v",
			consumerInfo.Id, consumerInfo.Group, consumerInfo.Topics)

	rebalanceAndLog := func() {
		if c.state.Rebalance() {
			log.Info().Msg("Consumer topology was rebalanced after adding a new consumer connection registered")
			log.Debug().Msgf("Consumer topology contains consumer groups %v", c.state.groups)
		}
	}

	if rebalanceDelay {
		c.addDebouncer(rebalanceAndLog)
	} else {
		rebalanceAndLog()
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write([]byte("OK"))
	return nil
}

func (c *consumer) unRegister(id string) {
	removed, length := c.state.RemoveConnection(id)
	if !removed {
		return
	}

	metrics.ConsumerConnections.Set(float64(length))
	log.Debug().Int("connections", length).Msgf("Connection from consumer client closed")

	// We shouldn't rush to rebalance
	removeDebouncer(func() {
		if c.state.Rebalance() {
			log.Info().Msg("Consumer topology was rebalanced after removing a connection to a consumer client")
		}
	})
}

func (c *consumer) postPoll(
	tc *trackedConsumer,
	w http.ResponseWriter,
	r *http.Request,
	_ httprouter.Params,
) error {
	if !tc.Registered() {
		return types.NewHttpError(consumerNotRegisteredStatus, "Consumer not registered")
	}
	tc.SetAsRead()
	id := tc.Id()
	group, tokens, _ := logsToServe(c.state, c.topologyGetter, id)
	if len(tokens) == 0 {
		log.Debug().Msgf("Received consumer client poll from connection '%s' with no assigned tokens", id)
		NoContentResponse(w, consumerNoOwnedDataDelay)
		return nil
	}

	log.Debug().Msgf("Received consumer client poll from '%s'", id)
	groupReadQueue := c.getOrCreateReadQueue(group)

	format := compressedBinaryFormat
	if r.Header.Get("Accept") == ContentTypeJSON {
		format = jsonFormat
	}

	groupReadQueue.readNext(id, format, w)
	return nil
}

func (c *consumer) postManualCommit(
	tc *trackedConsumer,
	w http.ResponseWriter,
	r *http.Request,
	_ httprouter.Params,
) error {
	if !tc.Registered() {
		return types.NewHttpError(consumerNotRegisteredStatus, "Consumer not registered")
	}
	tc.SetAsRead()
	id := tc.Id()
	group, tokens, _ := logsToServe(c.state, c.topologyGetter, id)
	if len(tokens) == 0 {
		log.Debug().
			Msgf("Received consumer client manual commit from connection '%s' with no assigned tokens", id)
		NoContentResponse(w, consumerNoOwnedDataDelay)
		return nil
	}

	log.Debug().Msgf("Received consumer client manual commit from connection '%s'", id)
	groupReadQueue := c.getOrCreateReadQueue(group)
	groupReadQueue.manualCommit(id, w)
	return nil
}

func (c *consumer) getOrCreateReadQueue(group string) *groupReadQueue {
	grq, _, _ := c.readQueues.LoadOrStore(group, func() (interface{}, error) {
		return newGroupReadQueue(group, c.state, c.offsetState, c.topologyGetter, c.gossiper, c.rrFactory, c.config), nil
	})

	return grq.(*groupReadQueue)
}

type ConsumerAwareHandle func(*trackedConsumer, http.ResponseWriter, *http.Request, httprouter.Params) error

// Gets the tokens and topics to serve, given a connection.
func logsToServe(
	state *ConsumerState,
	topologyGetter discovery.TopologyGetter,
	connId string,
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

func toTrackedHandler(tc *trackedConsumer, h ConsumerAwareHandle) httprouter.Handle {
	return func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		if err := h(tc, w, r, ps); err != nil {
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
	fmt.Fprint(w, err.Error())
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
