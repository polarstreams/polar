package interbroker

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/barcostreams/barco/internal/conf"
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
	Replicator
	GenerationGossiper

	// Starts accepting connections from peers.
	AcceptConnections() error

	// Starts opening connections to known peers.
	OpenConnections() error

	// Sends a message to be handled as a leader of a token
	SendToLeader(
		replicationInfo ReplicationInfo,
		topic string,
		querystring url.Values,
		contentLength int64,
		body io.Reader) error

	// Sends a message to the broker with the ordinal number containing the local snapshot of consumers
	SendConsumerGroups(ordinal int, groups []ConsumerGroup) error

	// Sends a message to the broker with the committed offset of a consumer group
	SendCommittedOffset(ordinal int, offsetKv *OffsetStoreKeyValue) error

	// Reads the producer offset of a certain past topic generatoin
	ReadProducerOffset(ordinal int, topic *TopicDataId) (uint64, error)

	// Adds a listener for consumer information
	RegisterConsumerInfoListener(listener ConsumerInfoListener)

	// Adds a listener for rerouted messages
	RegisterReroutedMessageListener(listener ReroutingListener)

	// WaitForPeersUp blocks until all peers are UP
	WaitForPeersUp()
}

//  GenerationGossiper is responsible for communicating actions related to generations.
type GenerationGossiper interface {
	// GetGenerations gets the generations for a given token on a peer
	GetGenerations(ordinal int, token Token) GenReadResult

	// IsTokenRangeCovered sends a request to the peer to determine whether the broker
	// has an active range containing (but not starting) the token
	IsTokenRangeCovered(ordinal int, token Token) (bool, error)

	// HasTokenHistoryForToken determines whether the broker has any history matching the token
	HasTokenHistoryForToken(ordinal int, token Token) (bool, error)

	// Compare and sets the generation value to the proposed state
	SetGenerationAsProposed(ordinal int, newGen *Generation, expectedTx *UUID) error

	// Compare and sets the generation as committed
	SetAsCommitted(ordinal int, token Token, tx UUID) error

	// RegisterGenListener adds a listener for new generations received by the gossipper
	RegisterGenListener(listener GenListener)
}

type GenListener interface {
	OnRemoteSetAsProposed(newGen *Generation, expectedTx *UUID) error

	OnRemoteSetAsCommitted(token Token, tx UUID, origin int) error
}

type ConsumerInfoListener interface {
	OnConsumerInfoFromPeer(ordinal int, groups []ConsumerGroup)

	OnOffsetFromPeer(kv *OffsetStoreKeyValue)
}

type ReroutingListener interface {
	OnReroutedMessage(topic string, querystring url.Values, contentLength int64, body io.ReadCloser) error
}

type GenReadResult struct {
	Committed *Generation
	Proposed  *Generation
	Error     error
}

func NewGossiper(config conf.GossipConfig, discoverer discovery.Discoverer) Gossiper {
	return &gossiper{
		config:           config,
		discoverer:       discoverer,
		connectionsMutex: sync.Mutex{},
		connections:      atomic.Value{},
		replicaWriters:   utils.NewCopyOnWriteMap(),
	}
}

type gossiper struct {
	config               conf.GossipConfig
	discoverer           discovery.Discoverer
	localDb              localdb.Client
	genListener          GenListener
	consumerInfoListener ConsumerInfoListener
	reroutingListener    ReroutingListener
	connectionsMutex     sync.Mutex
	// Map of connections
	connections atomic.Value
	// Map of SegmentWriter to be use for replicating data as a replica
	replicaWriters *utils.CopyOnWriteMap
}

func (g *gossiper) Init() error {
	g.discoverer.RegisterListener(g.onDiscoveredTopologyChange)
	return nil
}

func (g *gossiper) onDiscoveredTopologyChange() {
	// TODO: Create new connections, refresh existing
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

func (g *gossiper) HasTokenHistoryForToken(ordinal int, token Token) (bool, error) {
	r, err := g.requestGet(ordinal, fmt.Sprintf(conf.GossipTokenHasHistoryUrl, token))
	if err != nil {
		return false, err
	}
	defer r.Body.Close()
	var result bool
	err = json.NewDecoder(r.Body).Decode(&result)
	return result, err
}

func (g *gossiper) RegisterGenListener(listener GenListener) {
	if g.genListener != nil {
		panic("Listener registered multiple times")
	}
	g.genListener = listener
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
	if len(g.discoverer.Peers()) == 0 {
		log.Warn().Msg("No peer detected (dev mode)")
		return
	}

	start := time.Now()
	lastWarn := 0
	for {
		allPeersUp := false
		for _, peer := range g.discoverer.Peers() {
			if client := g.getClientInfo(peer.Ordinal); client != nil && client.isHostUp() {
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

func (g *gossiper) requestGet(ordinal int, baseUrl string) (*http.Response, error) {
	c := g.getClientInfo(ordinal)
	if c == nil {
		return nil, fmt.Errorf("No connection to broker %d", ordinal)
	}

	brokers := g.discoverer.Brokers()
	if len(brokers) <= ordinal {
		return nil, fmt.Errorf("No broker %d obtained", ordinal)
	}

	resp, err := c.gossipClient.Get(g.getPeerUrl(&brokers[ordinal], baseUrl))

	if err == nil && resp.StatusCode != http.StatusOK {
		return nil, errors.New(resp.Status)
	}

	return resp, err
}

func (g *gossiper) requestPost(ordinal int, baseUrl string, body []byte) (*http.Response, error) {
	c := g.getClientInfo(ordinal)
	if c == nil {
		return nil, fmt.Errorf("No connection to broker %d", ordinal)
	}

	topology := g.discoverer.Topology()
	broker := topology.BrokerByOrdinal(ordinal)

	if broker == nil {
		return nil, fmt.Errorf("Broker with ordinal %d not found", ordinal)
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

func (g *gossiper) ReadProducerOffset(ordinal int, topic *TopicDataId) (uint64, error) {
	url := fmt.Sprintf(
		conf.GossipReadProducerOffsetUrl,
		topic.Name,
		topic.Token.String(),
		topic.RangeIndex.String(),
		topic.GenId.String())
	r, err := g.requestGet(ordinal, url)
	if err != nil {
		return 0, err
	}
	defer r.Body.Close()
	var value uint64
	if err = json.NewDecoder(r.Body).Decode(&value); err != nil {
		return 0, err
	}
	return value, err
}

func (g *gossiper) SetGenerationAsProposed(ordinal int, newGen *Generation, expectedTx *UUID) error {
	message := GenerationProposeMessage{
		Generation: newGen,
		ExpectedTx: expectedTx,
	}

	jsonBody, err := json.Marshal(message)
	if err != nil {
		log.Fatal().Err(err).Msgf("json marshalling failed when setting generation as proposed")
	}

	r, err := g.requestPost(ordinal, fmt.Sprintf(conf.GossipGenerationProposeUrl, newGen.Start), jsonBody)
	defer bodyClose(r)
	return err
}

func (g *gossiper) SetAsCommitted(ordinal int, token Token, tx UUID) error {
	message := GenerationCommitMessage{
		Tx:     tx,
		Origin: g.discoverer.Topology().MyOrdinal(),
	}
	jsonBody, err := json.Marshal(message)
	if err != nil {
		log.Fatal().Err(err).Msgf("json marshalling failed when setting generation as committed")
	}

	r, err := g.requestPost(ordinal, fmt.Sprintf(conf.GossipGenerationCommmitUrl, token), jsonBody)
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

func (g *gossiper) SendCommittedOffset(ordinal int, kv *OffsetStoreKeyValue) error {
	jsonBody, err := json.Marshal(kv)
	if err != nil {
		log.Fatal().Err(err).Msgf("json marshalling failed when sending offset")
	}

	r, err := g.requestPost(ordinal, conf.GossipConsumerOffsetUrl, jsonBody)
	defer bodyClose(r)
	return err
}

func bodyClose(r *http.Response) {
	if r != nil && r.Body != nil {
		r.Body.Close()
	}
}
