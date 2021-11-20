package interbroker

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	. "github.com/google/uuid"
	"github.com/jorgebay/soda/internal/conf"
	"github.com/jorgebay/soda/internal/discovery"
	"github.com/jorgebay/soda/internal/localdb"
	"github.com/jorgebay/soda/internal/types"
	. "github.com/jorgebay/soda/internal/types"
	"github.com/jorgebay/soda/internal/utils"
	"github.com/rs/zerolog/log"
)

const waitForUpDelay = 200 * time.Millisecond
const waitForUpMaxWait = 10 * time.Minute

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
	SendToLeader(replicationInfo ReplicationInfo, topic string, body []byte) error

	// Sends a message to the broker with the ordinal number containing the local snapshot of consumers
	SendConsumerGroups(ordinal int, groups []ConsumerGroup) error

	// Adds a listener
	RegisterConsumerInfoListener(listener ConsumerInfoListener)

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
	if g.genListener != nil {
		panic("Listener registered multiple times")
	}
	g.consumerInfoListener = listener
}

func (g *gossiper) SendToLeader(replicationInfo ReplicationInfo, topic string, body []byte) error {
	// TODO: Implement
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

	resp, err := c.client.Get(g.getPeerUrl(&brokers[ordinal], baseUrl))

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

	brokers := g.discoverer.Brokers()
	if len(brokers) <= ordinal {
		return nil, fmt.Errorf("No broker %d obtained", ordinal)
	}

	resp, err := c.client.Post(g.getPeerUrl(&brokers[ordinal], baseUrl), "application/json", bytes.NewReader(body))

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

func (g *gossiper) SetGenerationAsProposed(ordinal int, newGen *Generation, expectedTx *UUID) error {
	message := GenerationProposeMessage{
		Generation: newGen,
		ExpectedTx: expectedTx,
	}

	jsonBody, err := json.Marshal(message)
	if err != nil {
		log.Fatal().Err(err).Msgf("json marshalling failed when setting generation as accepted")
	}

	r, err := g.requestPost(ordinal, fmt.Sprintf(conf.GossipGenerationProposeUrl, newGen.Start), jsonBody)
	defer r.Body.Close()
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
	defer r.Body.Close()
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
	defer r.Body.Close()
	return err
}
