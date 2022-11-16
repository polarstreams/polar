package interbroker

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/barcostreams/barco/internal/conf"
	"github.com/barcostreams/barco/internal/data"
	"github.com/barcostreams/barco/internal/discovery"
	"github.com/barcostreams/barco/internal/localdb"
	"github.com/barcostreams/barco/internal/metrics"
	"github.com/barcostreams/barco/internal/types"
	. "github.com/barcostreams/barco/internal/types"
	"github.com/barcostreams/barco/internal/utils"
	. "github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog/log"
)

const waitForUpDelay = 200 * time.Millisecond
const waitForUpMaxWait = 10 * time.Minute
const contentType = "application/json"

// TODO: Pass Context

// Gossiper is responsible for communicating with other peers.
type Gossiper interface {
	Initializer
	Closer
	Replicator
	GenerationGossiper

	// Starts accepting connections from peers.
	AcceptConnections() error

	// Starts opening connections to known peers.
	OpenConnections()

	// Sends a message to be handled as a leader of a token
	SendToLeader(
		replicationInfo ReplicationInfo,
		topic string,
		querystring url.Values,
		contentLength int64,
		contentType string,
		body io.Reader) error

	// Sends a request to get file part to one or more peers
	StreamFile(
		peers []int,
		segmentId int64,
		topic *TopicDataId,
		startOffset int64,
		maxRecords int,
		buf []byte) (int, error)

	// Queries a peer for the state of another broker
	ReadBrokerIsUp(ordinal int, brokerUpOrdinal int) (bool, error)

	// Sends a message to the broker with the ordinal number containing the local snapshot of consumers
	SendConsumerGroups(ordinal int, groups []ConsumerGroup) error

	SendConsumerRegister(ordinal int, id string, group string, topics []string, onNewGroup OffsetResetPolicy) error

	// Sends a message to the broker with the committed offset of a consumer group
	SendCommittedOffset(ordinal int, offsetKv *OffsetStoreKeyValue) error

	// Sends a message to the next broker stating the current broker is shutting down
	SendGoobye()

	// Reads the producer offset of a certain past topic generatoin
	ReadProducerOffset(ordinal int, topic *TopicDataId) (int64, error)

	// Retrieves the file structure from the peers and merge it with the local file structure
	MergeTopicFiles(peers []int, topic *TopicDataId, offset int64) error

	// Adds a listener for consumer information
	RegisterConsumerInfoListener(listener ConsumerInfoListener)

	// Adds a listener for rerouted messages
	RegisterReroutedMessageListener(listener ReroutingListener)

	// WaitForPeersUp blocks until all peers are UP
	WaitForPeersUp()

	// Gets a snapshot information to determine whether a broker is considered as UP
	IsHostUp(ordinal int) bool
}

// GenerationGossiper is responsible for communicating actions related to generations.
type GenerationGossiper interface {
	// GetGenerations gets the generations for a given token on a peer
	GetGenerations(ordinal int, token Token) GenReadResult

	// IsTokenRangeCovered sends a request to the peer to determine whether the broker
	// has an active range containing (but not starting) the token
	IsTokenRangeCovered(ordinal int, token Token) (bool, error)

	// HasTokenHistoryForToken determines whether the broker has any history matching the token
	HasTokenHistoryForToken(ordinal int, token Token, clusterSize int) (bool, error)

	// Gets the last known generation (not necessary the active one) of a given start token
	ReadTokenHistory(ordinal int, token Token, clusterSize int) (*Generation, error)

	// Compare and sets the generation value to the proposed/accepted state
	SetGenerationAsProposed(ordinal int, newGen *Generation, newGen2 *Generation, expectedTx *UUID) error

	// Compare and sets the generation as committed
	SetAsCommitted(ordinal int, token1 Token, token2 *Token, tx UUID) error

	// Sends a request to the previous broker to start the process of splitting its token range
	RangeSplitStart(ordinal int) error

	// RegisterGenListener adds a listener for new generations received by the gossipper
	RegisterGenListener(listener GenListener)

	// RegisterHostUpDownListener adds a handler to that will be invoked when peers switch from UP->DOWN or DOWN->UP
	RegisterHostUpDownListener(listener PeerStateListener)
}

func NewGossiper(
	config conf.GossipConfig,
	discoverer discovery.Discoverer,
	localDb localdb.Client,
	datalog data.Datalog,
) Gossiper {
	return &gossiper{
		config:              config,
		discoverer:          discoverer,
		localDb:             localDb,
		datalog:             datalog,
		connectionsMutex:    sync.Mutex{},
		connections:         atomic.Value{},
		replicaWriters:      utils.NewCopyOnWriteMap(),
		hostUpDownListeners: make([]PeerStateListener, 0),
	}
}

type gossiper struct {
	config               conf.GossipConfig
	discoverer           discovery.Discoverer
	localDb              localdb.Client
	datalog              data.Datalog
	httpListener         net.Listener
	dataListener         net.Listener
	genListener          GenListener
	consumerInfoListener ConsumerInfoListener
	reroutingListener    ReroutingListener
	hostUpDownListeners  []PeerStateListener
	connectionsMutex     sync.Mutex
	connections          atomic.Value          // Map of connections with copy-on-write semantics
	replicaWriters       *utils.CopyOnWriteMap // Map of SegmentWriter to be use for replicating data as a replica
}

func (g *gossiper) Init() error {
	g.discoverer.RegisterListener(g)
	return nil
}

func (g *gossiper) OnTopologyChange(previousTopology *TopologyInfo, topology *TopologyInfo) {
	if len(topology.Brokers) > len(previousTopology.Brokers) {
		log.Info().Msgf("Scaling up detected, opening connections to new brokers")
		g.createNewClients(topology)
	} else {
		if !topology.AmIIncluded() {
			log.Info().Msgf("Scaling down detected but I'm leaving the cluster, ignoring")
			return
		}
		log.Info().
			Int("previous", len(previousTopology.Brokers)).
			Int("current", len(topology.Brokers)).
			Msgf("Scaling down detected")
		g.markAsLeaving(previousTopology, topology)
		g.waitForDownAndRemoveClients(previousTopology, topology)
		g.genListener.OnJoinRange(previousTopology, topology)
	}
}

func (g *gossiper) IsTokenRangeCovered(ordinal int, token Token) (bool, error) {
	r, err := g.requestGet(ordinal, fmt.Sprintf(conf.GossipTokenInRange, token))
	if err != nil {
		return false, err
	}
	defer r.Body.Close()
	var result bool
	err = json.NewDecoder(r.Body).Decode(&result)
	return result, err
}

func (g *gossiper) HasTokenHistoryForToken(ordinal int, token Token, clusterSize int) (bool, error) {

	url := fmt.Sprintf("%s?clusterSize=%d", fmt.Sprintf(conf.GossipTokenHasHistoryUrl, token), clusterSize)
	r, err := g.requestGet(ordinal, url)
	if err != nil {
		return false, err
	}
	defer r.Body.Close()
	var result bool
	err = json.NewDecoder(r.Body).Decode(&result)
	return result, err
}

func (g *gossiper) ReadTokenHistory(ordinal int, token Token, clusterSize int) (*Generation, error) {
	url := fmt.Sprintf("%s?clusterSize=%d", fmt.Sprintf(conf.GossipTokenGetHistoryUrl, token), clusterSize)
	r, err := g.requestGet(ordinal, url)
	if err != nil {
		return nil, err
	}
	defer r.Body.Close()
	var result Generation
	_ = json.NewDecoder(r.Body).Decode(&result)
	if result.Version == 0 {
		return nil, nil
	}
	return &result, nil
}

func (g *gossiper) RegisterGenListener(listener GenListener) {
	if g.genListener != nil {
		panic("Listener registered multiple times")
	}
	g.genListener = listener
}

func (g *gossiper) RegisterHostUpDownListener(listener PeerStateListener) {
	g.hostUpDownListeners = append(g.hostUpDownListeners, listener)
}

func (g *gossiper) RegisterConsumerInfoListener(listener ConsumerInfoListener) {
	if g.consumerInfoListener != nil {
		panic("Listener registered multiple times")
	}
	g.consumerInfoListener = listener
}

func (g *gossiper) RegisterReroutedMessageListener(listener ReroutingListener) {
	if g.reroutingListener != nil {
		panic("Listener registered multiple times")
	}
	g.reroutingListener = listener
}

func (g *gossiper) SendToLeader(
	replicationInfo ReplicationInfo,
	topic string,
	querystring url.Values,
	contentLength int64,
	contentType string,
	body io.Reader,
) error {
	c := g.getClientInfo(replicationInfo.Leader.Ordinal)
	if c == nil {
		msg := fmt.Sprintf("No routing client found for peer with ordinal %d as leader", replicationInfo.Leader.Ordinal)
		log.Error().Msg(msg)
		return fmt.Errorf(msg)
	}

	ordinal := replicationInfo.Leader.Ordinal
	urlPath := fmt.Sprintf(conf.RoutingMessageUrl+"?%s", topic, querystring.Encode())
	topology := g.discoverer.Topology()
	broker := topology.BrokerByOrdinal(ordinal)

	if broker == nil {
		log.Debug().Msgf("Broker with ordinal %d not found", ordinal)
		return fmt.Errorf("Broker with ordinal %d not found", ordinal)
	}

	metrics.ReroutedSent.With(prometheus.Labels{"target": strconv.Itoa(ordinal)}).Inc()
	req, err := http.NewRequest(http.MethodPost, g.getPeerUrl(broker, urlPath), body)
	if err != nil {
		return err
	}
	req.ContentLength = contentLength
	req.Header.Set(ContentTypeHeaderKey, contentType)
	resp, err := c.routingClient.Do(req)

	if err != nil {
		return err
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return types.NewHttpError(resp.StatusCode, resp.Status)
	}

	return nil
}

func (g *gossiper) WaitForPeersUp() {
	peers := g.discoverer.Topology().Peers()
	if len(peers) == 0 {
		log.Warn().Msg("No peer detected (dev mode)")
		return
	}

	start := time.Now()
	lastWarn := 0
	for {
		allPeersUp := false
		for _, peer := range peers {
			if g.IsHostUp(peer.Ordinal) {
				allPeersUp = true
			} else {
				allPeersUp = false
				break
			}
		}

		if allPeersUp {
			return
		}

		elapsed := int(time.Since(start).Seconds())
		if elapsed > 1 && elapsed%5 == 0 && elapsed != lastWarn {
			lastWarn = elapsed
			log.Info().Msgf("Waiting for peer after %d seconds", elapsed)
		}
		if elapsed > int(waitForUpMaxWait.Seconds()) {
			log.Fatal().Msgf("No peer up after %d seconds", elapsed)
		}

		time.Sleep(waitForUpDelay)
	}
}

func (g *gossiper) IsHostUp(ordinal int) bool {
	client := g.getClientInfo(ordinal)
	return client != nil && client.isHostUp()
}

func (g *gossiper) requestGet(ordinal int, baseUrl string) (*http.Response, error) {
	c, broker, err := g.getClientForRequest(ordinal)
	if err != nil {
		return nil, err
	}

	resp, err := c.gossipClient.Get(g.getPeerUrl(broker, baseUrl))

	if err == nil && resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		return nil, errors.New(resp.Status)
	}

	return resp, err
}

func (g *gossiper) getClientForRequest(ordinal int) (*clientInfo, *BrokerInfo, error) {
	c := g.getClientInfo(ordinal)
	if c == nil {
		return nil, nil, fmt.Errorf("No connection to broker %d", ordinal)
	}

	broker := g.discoverer.CurrentOrPastBroker(ordinal)
	if broker == nil {
		return nil, nil, fmt.Errorf("B%d not found", ordinal)
	}

	if !c.isHostUp() {
		return nil, nil, fmt.Errorf("Can't send request to B%d, as B%d is DOWN", ordinal, ordinal)
	}

	return c, broker, nil
}

func (g *gossiper) requestPost(ordinal int, baseUrl string, body []byte) (*http.Response, error) {
	c, broker, err := g.getClientForRequest(ordinal)
	if err != nil {
		return nil, err
	}

	resp, err := c.gossipClient.Post(g.getPeerUrl(broker, baseUrl), contentType, bytes.NewReader(body))

	if err == nil && resp.StatusCode != http.StatusOK {
		return nil, types.NewHttpError(resp.StatusCode, resp.Status)
	}

	return resp, err
}

func (g *gossiper) GetGenerations(ordinal int, token Token) GenReadResult {
	r, err := g.requestGet(ordinal, fmt.Sprintf(conf.GossipGenerationUrl, token))
	if err != nil {
		return GenReadResult{Error: err}
	}
	defer r.Body.Close()
	var gens []Generation
	if err = json.NewDecoder(r.Body).Decode(&gens); err != nil {
		return GenReadResult{Error: err}
	}

	result := GenReadResult{}

	if len(gens) > 0 && gens[0].Version > 0 {
		result.Committed = &gens[0]
	}
	if len(gens) > 1 && gens[1].Version > 0 {
		result.Proposed = &gens[1]
	}
	return result
}

func (g *gossiper) ReadBrokerIsUp(ordinal int, brokerUpOrdinal int) (bool, error) {
	url := fmt.Sprintf(conf.GossipHostIsUpUrl, strconv.Itoa(brokerUpOrdinal))
	r, err := g.requestGet(ordinal, url)
	if err != nil {
		return false, err
	}
	defer r.Body.Close()
	var value bool
	err = json.NewDecoder(r.Body).Decode(&value)
	return value, err
}

func (g *gossiper) ReadProducerOffset(ordinal int, topic *TopicDataId) (int64, error) {
	url := fmt.Sprintf(
		conf.GossipReadProducerOffsetUrl,
		topic.Name,
		topic.Token.String(),
		topic.RangeIndex.String(),
		topic.Version.String())
	r, err := g.requestGet(ordinal, url)
	if err != nil {
		return 0, err
	}
	defer r.Body.Close()

	if r.StatusCode == http.StatusNoContent {
		return 0, GossipGetNotFound
	}
	var value int64
	if err = json.NewDecoder(r.Body).Decode(&value); err != nil {
		return 0, err
	}
	return value, err
}

func (g *gossiper) MergeTopicFiles(peers []int, topic *TopicDataId, offset int64) error {
	url := fmt.Sprintf(
		conf.GossipReadFileStructureUrl,
		topic.Name,
		topic.Token.String(),
		topic.RangeIndex.String(),
		topic.Version.String(),
		strconv.FormatInt(offset, 10))

	for _, ordinal := range peers {
		r, err := g.requestGet(ordinal, url)
		if err == nil {
			var message TopicFileStructureMessage
			defer r.Body.Close()
			if err = json.NewDecoder(r.Body).Decode(&message); err != nil {
				log.Err(err).Msgf("There was a problem decoding TopicFileStructureMessage")
				continue
			}
			return data.MergeDataStructure(message.FileNames, topic, offset, g.config)
		} else if g.IsHostUp(ordinal) {
			log.Warn().Err(err).Msgf("There was a problem querying B%d to retrieve topic structure", ordinal)
		}
	}

	return fmt.Errorf("All queries failed")
}

func (g *gossiper) SetGenerationAsProposed(
	ordinal int,
	newGen *Generation,
	newGen2 *Generation,
	expectedTx *UUID,
) error {
	message := GenerationProposeMessage{
		Generation:  newGen,
		Generation2: newGen2,
		ExpectedTx:  expectedTx,
	}

	if newGen2 != nil && newGen2.Status != StatusAccepted {
		log.Fatal().Msgf("it is only possible to accept two generations at the same time")
	}

	jsonBody, err := json.Marshal(message)
	if err != nil {
		log.Fatal().Err(err).Msgf("json marshalling failed when setting generation as proposed")
	}

	r, err := g.requestPost(ordinal, fmt.Sprintf(conf.GossipGenerationProposeUrl, newGen.Start), jsonBody)
	defer bodyClose(r)
	return err
}

func (g *gossiper) SetAsCommitted(ordinal int, token1 Token, token2 *Token, tx UUID) error {
	message := GenerationCommitMessage{
		Tx:     tx,
		Token1: token1,
		Token2: token2,
		Origin: g.discoverer.Topology().MyOrdinal(),
	}
	jsonBody, err := json.Marshal(message)
	if err != nil {
		log.Fatal().Err(err).Msgf("json marshalling failed when setting generation as committed")
	}

	tokenParam := token1.String()
	if token2 != nil {
		tokenParam += token2.String()
	}

	r, err := g.requestPost(ordinal, fmt.Sprintf(conf.GossipGenerationCommmitUrl, tokenParam), jsonBody)
	defer bodyClose(r)
	return err
}

func (g *gossiper) SendConsumerGroups(ordinal int, groups []ConsumerGroup) error {
	message := ConsumerGroupInfoMessage{
		Groups: groups,
		Origin: g.discoverer.Topology().MyOrdinal(),
	}
	jsonBody, err := json.Marshal(message)
	if err != nil {
		log.Fatal().Err(err).Msgf("json marshalling failed when creating consumer info message")
	}

	r, err := g.requestPost(ordinal, conf.GossipConsumerGroupsInfoUrl, jsonBody)
	defer bodyClose(r)
	return err
}

func (g *gossiper) SendConsumerRegister(ordinal int, id string, group string, topics []string, onNewGroup OffsetResetPolicy) error {
	message := ConsumerRegisterMessage{
		Id:         id,
		Group:      group,
		Topics:     topics,
		OnNewGroup: onNewGroup,
	}
	jsonBody, err := json.Marshal(message)
	if err != nil {
		log.Fatal().Err(err).Msgf("json marshalling failed when creating consumer register message")
	}

	r, err := g.requestPost(ordinal, conf.GossipConsumerRegisterUrl, jsonBody)
	defer bodyClose(r)
	return err
}

func (g *gossiper) SendCommittedOffset(ordinal int, kv *OffsetStoreKeyValue) error {
	jsonBody, err := json.Marshal(kv)
	if err != nil {
		log.Fatal().Err(err).Msgf("json marshalling failed when sending offset")
	}

	r, err := g.requestPost(ordinal, conf.GossipConsumerOffsetUrl, jsonBody)
	defer bodyClose(r)
	return err
}

func (g *gossiper) SendGoobye() {
	if g.config.DevMode() {
		return
	}

	// Notify the next broker
	topology := g.discoverer.Topology()
	if !topology.AmIIncluded() {
		// I'm leaving the cluster, no point in saying goodbye
		return
	}
	peerOrdinal := topology.NextBroker().Ordinal
	jsonBody, _ := json.Marshal(topology.MyOrdinal())
	r, err := g.requestPost(peerOrdinal, conf.GossipGoodbyeUrl, jsonBody)
	defer bodyClose(r)
	if err != nil {
		log.Warn().Msgf("Goodbye message to B%d could not be sent", peerOrdinal)
	} else {
		log.Info().Msgf("Goodbye message sent to B%d", peerOrdinal)
	}
}

func (g *gossiper) RangeSplitStart(ordinal int) error {
	origin := g.discoverer.Topology().MyOrdinal()
	jsonBody, _ := json.Marshal(origin)
	r, err := g.requestPost(ordinal, conf.GossipGenerationSplitUrl, jsonBody)
	defer bodyClose(r)
	return err
}

func bodyClose(r *http.Response) {
	if r != nil && r.Body != nil {
		r.Body.Close()
	}
}
