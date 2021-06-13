package interbroker

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/jorgebay/soda/internal/conf"
	"github.com/jorgebay/soda/internal/discovery"
	"github.com/jorgebay/soda/internal/types"
	"github.com/jorgebay/soda/internal/utils"
	"github.com/rs/zerolog/log"
)

const waitForUpDelay = 200 * time.Millisecond
const waitForUpMaxWait = 10 * time.Minute

// TODO: Pass Context

// Gossiper is responsible for communicating with other peers.
type Gossiper interface {
	types.Initializer
	types.Replicator

	// Starts accepting connections from peers.
	AcceptConnections() error

	// Starts opening connections to known peers.
	OpenConnections() error

	// Sends a message to be handled as a leader of a token
	SendToLeader(replicationInfo types.ReplicationInfo, topic string, body []byte) error

	// WaitForPeersUp blocks until at least one peer is UP
	WaitForPeersUp()

	//ProposeGeneration(follower types.BrokerInfo) err
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
	config           conf.GossipConfig
	discoverer       discovery.Discoverer
	connectionsMutex sync.Mutex
	// Map of connections
	connections atomic.Value
	// Map of SegmentWriter to be use for replicating data as a replica
	replicaWriters *utils.CopyOnWriteMap
}

func (g *gossiper) Init() error {
	g.discoverer.RegisterListener(g.OnTopologyChange)
	return nil
}

func (g *gossiper) OnTopologyChange() {
	// TODO: Create new connections, refresh existing
}

func (g *gossiper) SendToLeader(replicationInfo types.ReplicationInfo, topic string, body []byte) error {
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
		for _, peer := range g.discoverer.Peers() {
			if client := g.getClientInfo(&peer); client != nil && client.isHostUp() {
				return
			}
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
