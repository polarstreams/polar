package interbroker

import (
	"sync"
	"sync/atomic"

	"github.com/jorgebay/soda/internal/conf"
	"github.com/jorgebay/soda/internal/discovery"
	"github.com/jorgebay/soda/internal/types"
	"github.com/jorgebay/soda/internal/utils"
)

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
