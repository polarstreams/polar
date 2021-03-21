package interbroker

import (
	"github.com/jorgebay/soda/internal/conf"
	"github.com/jorgebay/soda/internal/types"
)

// TODO: Inter-broker communication should be versioned
// TODO: Pass Context

// Gossiper is responsible for communicating with other peers.
type Gossiper interface {
	types.Initializer

	// Starts accepting connections from peers.
	AcceptConnections() error

	// Starts opening connections to known peers.
	OpenConnections() error

	// Sends a message to be stored as replica of current broker's datalog
	SendToFollowers(replicationInfo types.ReplicationInfo, topic string, body []byte) error

	// Sends a message to be handled as a leader of a token
	SendToLeader(replicationInfo types.ReplicationInfo, topic string, body []byte) error
}

func NewGossiper(config conf.Config) Gossiper {
	return &gossiper{config: config}
}

type gossiper struct {
	config conf.Config
}

func (g *gossiper) Init() error {
	return nil
}

func (g *gossiper) OpenConnections() error {
	return nil
}

func (g *gossiper) SendToFollowers(replicationInfo types.ReplicationInfo, topic string, body []byte) error {
	return nil
}

func (g *gossiper) SendToLeader(replicationInfo types.ReplicationInfo, topic string, body []byte) error {
	return nil
}
