package consuming

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"reflect"
	"sync"
	"time"

	"github.com/julienschmidt/httprouter"
	"github.com/polarstreams/polar/internal/conf"
	"github.com/polarstreams/polar/internal/data"
	"github.com/polarstreams/polar/internal/discovery"
	"github.com/polarstreams/polar/internal/interbroker"
	"github.com/polarstreams/polar/internal/localdb"
	"github.com/polarstreams/polar/internal/metrics"
	"github.com/polarstreams/polar/internal/types"
	. "github.com/polarstreams/polar/internal/types"
	. "github.com/polarstreams/polar/internal/utils"
	"github.com/rs/zerolog/log"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
)

const consumerGroupsToPeersDelay = 10 * time.Second

const (
	consumerNoOwnedDataDelay    = 5 // Seconds to retry after
	consumerNoDataDelay         = 1 // Seconds to retry after
	halfOpenTimerResolution     = 5 * time.Second
	consumerNotRegisteredStatus = http.StatusConflict
)

const (
	defaultMimeType = "application/vnd.polar.consumermessage"
	jsonMimeType    = "application/json"
)

const (
	// The query string parameters used for the consumer server
	consumerQueryKey       = "consumerId"  // Used to determine whether it's stateless-less
	consumerLegacyQueryKey = "consumer_id" // Introduced in v0.4.0, should be supported for at least until v0.7.0
	topicsQueryKey         = "topic"
	groupQueryKey          = "group"
	commitQueryKey         = "commit"
	offsetResetKey         = "onNewGroup"
)

const consumerGroupDefault = "default"

var removeDebouncer = Debounce(removeDelay, 0.4) // Debounce events that occurred in the following 2 minutes
var logLegacyConsumerIdOnce sync.Once

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
	datalog data.Datalog,
	gossiper interbroker.Gossiper,
) Consumer {
	addDelay := config.ConsumerAddDelay()
	if config.DevMode() {
		addDelay = 20 * time.Millisecond // Don't wait for rebalancing in dev mode: faster round trips
	}

	return &consumer{
		config:         config,
		topologyGetter: topologyGetter,
		datalog:        datalog,
		gossiper:       gossiper,
		localDb:        localDb,
		rrFactory:      newReplicationReaderFactory(gossiper),
		state:          NewConsumerState(config, topologyGetter),
		offsetState:    newDefaultOffsetState(localDb, topologyGetter, datalog, gossiper, config),
		readQueues:     NewCopyOnWriteMap(),
		addDebouncer:   Debounce(addDelay, 0),
	}
}

type consumer struct {
	config         conf.ConsumerConfig
	topologyGetter discovery.TopologyGetter
	datalog        data.Datalog
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
	h2s := &http2.Server{}
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
			// HTTP/1 & HTTP/2 with connection tracking
			conn, err := listener.Accept()
			if err != nil {
				if !c.localDb.IsShuttingDown() {
					log.Err(err).Msgf("Failed to accept new connections")
				}
				break
			}

			// HTTP1 Server does not support ServeConn(): https://github.com/golang/go/issues/36673
			// Implement a workaround: listener per connection
			var connListener net.Listener
			trackedConn := NewTrackedConnection(conn, func(trackedConn *TrackedConnection) {
				connListener.Close()
			})

			connListener = NewSingleConnListener(trackedConn)
			tc := newTrackedConsumerHandler(trackedConn)

			router := httprouter.New()
			router.GET(conf.StatusUrl, func(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
				tc.SetAsRead()
				fmt.Fprintf(w, "Consumer server listening on %d\n", port)
			})
			router.PUT(conf.ConsumerRegisterUrl, toTrackedHandler(tc, c.putRegister))
			router.POST(conf.ConsumerRegisterUrl, toTrackedHandler(tc, c.putRegister)) // Backwards compatibility
			router.POST(conf.ConsumerPollUrl, toTrackedHandler(tc, c.postPoll))
			router.POST(conf.ConsumerManualCommitUrl, toTrackedHandler(tc, c.postManualCommit))
			router.POST(conf.ConsumerGoodbye, toTrackedHandler(tc, c.postGoodbye))

			// server.Serve() will block until the connection is not readable anymore
			go func() {
				server := &http.Server{
					Addr:    address,
					Handler: h2c.NewHandler(router, h2s),
				}

				metrics.ConsumerOpenConnections.Inc()
				_ = server.Serve(connListener)
				metrics.ConsumerOpenConnections.Dec()
			}()
		}
	}()

	<-startChan
	c.listener = listener

	log.Info().Msgf("Start listening to consumers on %s", address)
	return nil
}

func (c *consumer) Close() {
	// Stop listening
	err := c.listener.Close()
	log.Err(err).Msgf("Consumer connection listener closed")

	for _, tc := range c.state.TrackedConsumers() {
		// Close connection-bound consumers
		tc.Close()
	}
}

func (c *consumer) detectReadTimeout(tc *trackedConsumerHandler) {
	for !tc.IsClosed() {
		if time.Since(tc.LastRead()) > c.config.ConsumerReadTimeout() {
			log.Debug().Msgf("Stop tracking consumer '%s' due to inactivity", tc.Id())
			c.unRegister(tc.Id(), true)
			tc.Close()
			break
		}
		time.Sleep(Jitter(halfOpenTimerResolution))
	}
}

func (c *consumer) putRegister(
	tc *trackedConsumerHandler,
	w http.ResponseWriter,
	r *http.Request,
	_ httprouter.Params,
) error {
	tc.SetAsRead()
	var info ConsumerInfo
	statelessConsumer := false

	statelessConsumerId := r.URL.Query().Get(consumerQueryKey)
	if statelessConsumerId == "" {
		if legacyId := r.URL.Query().Get(consumerLegacyQueryKey); legacyId != "" {
			statelessConsumerId = legacyId
			logLegacyConsumerIdOnce.Do(func() {
				log.Warn().Msgf(
					"Using '%s' for stateless consumers is DEPRECATED and will be removed in future versions, use '%s' instead",
					consumerLegacyQueryKey, statelessConsumerId)
			})
		}
	}

	if statelessConsumerId != "" {
		info.Id = statelessConsumerId
		info.Group = r.URL.Query().Get(groupQueryKey)
		info.Topics = r.URL.Query()[topicsQueryKey]
		info.OnNewGroup = DefaultOffsetResetPolicy
		offsetResetValue := r.URL.Query().Get(offsetResetKey)

		if offsetResetValue != "" {
			if policy, err := types.ParseOffsetResetPolicy(offsetResetValue); err != nil {
				return types.NewHttpError(http.StatusBadRequest, "Invalid offset reset policy value")
			} else {
				info.OnNewGroup = policy
			}
		}

		if existingTc, existingInfo := c.state.TrackedConsumerById(statelessConsumerId); existingTc != nil {
			if IfEmpty(info.Group, consumerGroupDefault) != existingInfo.Group ||
				!reflect.DeepEqual(info.Topics, existingInfo.Topics) {
				return types.NewHttpError(
					http.StatusBadRequest, "Consumer already registered with different parameters")
			}

			existingTc.SetAsRead()
			RespondText(w, "Already registered")
			return nil
		}

		tc.TrackAsStateless(statelessConsumerId)
		statelessConsumer = true
	} else {
		if err := json.NewDecoder(r.Body).Decode(&info); err != nil {
			return types.NewHttpError(http.StatusBadRequest, "Invalid ConsumerInfo payload")
		}
		tc.TrackAsConnectionBound()
	}

	// Default to "no rebalance delay" for stateless-less consumers
	if err := c.addConnectionAndRebalance(tc, info, !statelessConsumer); err != nil {
		return err
	}

	log.Info().
		Strs("topics", info.Topics).
		Bool("startFromLatest", info.OnNewGroup == StartFromLatest).
		Msgf("Registered new consumer with id %s and group %s", info.Id, info.Group)

	if statelessConsumer {
		// Auto register in peers
		peers := c.topologyGetter.Topology().Peers()
		if len(peers) > 0 {
			// Ignore dev mode
			err := AnyError(CollectErrors(InParallel(len(peers), func(i int) error {
				return c.gossiper.SendConsumerRegister(
					peers[i].Ordinal, info.Id, info.Group, info.Topics, info.OnNewGroup)
			})))

			if err != nil {
				return err
			}

			log.Debug().Msgf("Consumer '%s' registered on %d peers", info.Id, len(peers))
		}
	}

	RespondText(w, "OK")
	return nil
}

func (c *consumer) addConnectionAndRebalance(
	tc *trackedConsumerHandler,
	consumerInfo ConsumerInfo,
	rebalanceDelay bool,
) error {
	if consumerInfo.Id == "" || len(consumerInfo.Topics) == 0 {
		return types.NewHttpError(http.StatusBadRequest, "Consumer id and topics can not be empty")
	}

	consumerInfo.Group = IfEmpty(consumerInfo.Group, consumerGroupDefault)
	added, length := c.state.AddConnection(tc, consumerInfo)
	if !added {
		return nil
	}

	metrics.ActiveConsumers.Set(float64(length))
	log.Debug().
		Str("connId", tc.Id()).
		Int("connections", length).
		Msgf("Registering new connection to consumer '%s' of group '%s' for topics %v",
			consumerInfo.Id, consumerInfo.Group, consumerInfo.Topics)
	go c.detectReadTimeout(tc)

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

	return nil
}

func (c *consumer) unRegister(id string, debounce bool) {
	removed, length := c.state.RemoveConnection(id)
	if !removed {
		return
	}

	metrics.ActiveConsumers.Set(float64(length))
	log.Debug().Int("consumers", length).Msgf("Consumer '%s' unregistered", id)

	rebalanceAndLog := func() {
		if c.state.Rebalance() {
			log.Info().Msg("Consumer topology was rebalanced after removing a connection to a consumer client")
		}
	}

	if debounce {
		// We shouldn't rush to rebalance
		removeDebouncer(rebalanceAndLog)
	} else {
		rebalanceAndLog()
	}
}

// Verifies whether the consumer is tracked or, for stateless consumers, whether it can load an already
// tracked consumer.
//
// It returns an error when it's not tracked and it can't be registered.
func (c *consumer) validateRegistered(tc *trackedConsumerHandler, r *http.Request) HttpError {
	consumerId := r.URL.Query().Get(consumerQueryKey)
	if consumerId == "" {
		if legacyId := r.URL.Query().Get(consumerLegacyQueryKey); legacyId != "" {
			consumerId = legacyId
			logLegacyConsumerIdOnce.Do(func() {
				log.Warn().Msgf(
					"Using '%s' for stateless consumers is DEPRECATED and will be removed in future versions, use '%s' instead",
					consumerLegacyQueryKey, consumerId)
			})
		}
	}
	if tracked, err := tc.IsTracked(consumerId); err != nil {
		return types.NewHttpError(consumerNotRegisteredStatus, err.Error())
	} else if tracked {
		// Already tracked
		return nil
	}

	if consumerId != "" {
		if existing, _ := c.state.TrackedConsumerById(consumerId); existing != nil {
			tc.LoadFromExisting(existing)
			// Track new
			return nil
		}
	}

	return types.NewHttpError(consumerNotRegisteredStatus, "Consumer not registered")
}

func (c *consumer) postPoll(
	tc *trackedConsumerHandler,
	w http.ResponseWriter,
	r *http.Request,
	_ httprouter.Params,
) error {
	if err := c.validateRegistered(tc, r); err != nil {
		return err
	}
	tc.SetAsRead()
	id := tc.Id()
	group, tokens, _ := logsToServe(c.state, c.topologyGetter, id)
	if len(tokens) == 0 {
		log.Debug().Msgf("Received consumer client poll from connection '%s' with no assigned tokens", id)
		NoContentResponse(w, consumerNoOwnedDataDelay)
		return nil
	}

	log.Debug().
		Interface("query", r.URL.Query()).
		Msgf("Received consumer client poll from '%s'", id)
	groupReadQueue := c.getOrCreateReadQueue(group)

	format := compressedBinaryFormat
	if r.Header.Get("Accept") == ContentTypeJSON {
		format = jsonFormat
	}

	groupReadQueue.readNext(id, format, w)
	return nil
}

func (c *consumer) postManualCommit(
	tc *trackedConsumerHandler,
	w http.ResponseWriter,
	r *http.Request,
	_ httprouter.Params,
) error {
	if err := c.validateRegistered(tc, r); err != nil {
		return err
	}
	tc.SetAsRead()
	id := tc.Id()

	if tc.IsStateless() {
		// Commit on peers
		peers := c.topologyGetter.Topology().Peers()
		err := InParallelAnyError(len(peers), func(i int) error {
			return c.gossiper.SendConsumerCommit(peers[i].Ordinal, id)
		})
		if err != nil {
			return err
		}
	}
	c.manualCommitLocal(tc.Id(), "local commit request", w)

	return nil
}

func (c *consumer) postGoodbye(
	tc *trackedConsumerHandler,
	w http.ResponseWriter,
	r *http.Request,
	_ httprouter.Params,
) error {
	if err := c.validateRegistered(tc, r); err != nil {
		return err
	}

	id := tc.Id()
	statelessConsumer := tc.IsStateless()
	peers := c.topologyGetter.Topology().Peers()
	log.Debug().Msgf("Processing consumer goodbye from '%s'", id)

	if r.URL.Query().Get(commitQueryKey) != "false" {
		// Commit first
		if statelessConsumer {
			err := InParallelAnyError(len(peers), func(i int) error {
				return c.gossiper.SendConsumerCommit(peers[i].Ordinal, id)
			})
			if err != nil {
				return err
			}
		}
		c.manualCommitLocal(id, "goodbye", ignoreResponse{})
	}

	c.unRegister(id, !tc.IsStateless())
	tc.Close()

	// Unregister on peers in the background
	go func() {
		_ = InParallelAnyError(len(peers), func(i int) error {
			return c.gossiper.SendConsumerUnregister(peers[i].Ordinal, id)
		})
	}()

	RespondText(w, "OK")
	return nil
}

func (c *consumer) manualCommitLocal(id string, messageType string, w http.ResponseWriter) {
	group, tokens, _ := logsToServe(c.state, c.topologyGetter, id)
	if len(tokens) > 0 {
		log.Debug().Msgf("Committing offset as part of %s for consumer '%s'", messageType, id)
		groupReadQueue := c.getOrCreateReadQueue(group)
		groupReadQueue.manualCommit(id, w)
	}
}

func (c *consumer) getOrCreateReadQueue(group string) *groupReadQueue {
	grq, _, _ := c.readQueues.LoadOrStore(group, func() (interface{}, error) {
		return newGroupReadQueue(group, c.state, c.offsetState, c.topologyGetter, c.datalog, c.gossiper, c.rrFactory, c.config), nil
	})

	return grq.(*groupReadQueue)
}

type ConsumerAwareHandle func(*trackedConsumerHandler, http.ResponseWriter, *http.Request, httprouter.Params) error

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

	topology := topologyGetter.Topology()
	myOrdinal := topology.MyOrdinal()
	leaderTokens := make([]TokenRanges, 0, len(tokenRanges))

	for _, ranges := range tokenRanges {
		gen := topologyGetter.Generation(ranges.Token)
		if gen != nil && gen.Leader == myOrdinal {
			ranges.ClusterSize = gen.ClusterSize
			leaderTokens = append(leaderTokens, ranges)
		}
	}

	return group, leaderTokens, topics
}

func toTrackedHandler(tc *trackedConsumerHandler, h ConsumerAwareHandle) httprouter.Handle {
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
		Msgf("Received offset from peer for token %d/%d", kv.Value.Token, kv.Value.Index)
	c.offsetState.Set(kv.Key.Group, kv.Key.Topic, kv.Value, OffsetCommitLocal)
}

func (c *consumer) OnRegisterFromPeer(id string, group string, topics []string, onNewGroup OffsetResetPolicy) error {
	consumerInfo := ConsumerInfo{
		Id:         id,
		Group:      group,
		Topics:     topics,
		OnNewGroup: onNewGroup,
	}

	if tc, existingInfo := c.state.TrackedConsumerById(id); tc != nil {
		if IfEmpty(consumerInfo.Group, consumerGroupDefault) != existingInfo.Group ||
			!reflect.DeepEqual(consumerInfo.Topics, existingInfo.Topics) {
			return types.NewHttpError(
				http.StatusBadRequest, "Consumer already registered with different parameters")
		}
		tc.SetAsRead()
		return nil
	}

	tc := newTrackedConsumerHandler(nil)
	tc.TrackAsStateless(id)

	return c.addConnectionAndRebalance(tc, consumerInfo, false)
}

func (c *consumer) OnUnregisterFromPeer(id string) error {
	c.unRegister(id, false)
	return nil
}

func (c *consumer) OnCommitFromPeer(id string) error {
	c.manualCommitLocal(id, "gossip commit request", ignoreResponse{})
	return nil
}

func (c *consumer) sendConsumerGroupsToPeers() {
	if len(c.topologyGetter.Topology().Brokers) == 1 {
		// We are in dev mode, there's never going to be a peer
		return
	}

	const sendPeriod = 1000
	for i := 0; ; i++ {
		groups := c.state.GetInfoForPeers()
		topology := c.topologyGetter.Topology()
		if len(groups) > 0 {
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
