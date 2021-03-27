package discovery

import (
	"github.com/jorgebay/soda/internal/conf"
	"github.com/jorgebay/soda/internal/types"
)

// Discoverer provides the cluster topology information.
type Discoverer interface {
	types.Initializer
	LeaderGetter
	Peers() []types.BrokerInfo
	RegisterListener(l types.TopologyChangeHandler)
	Shutdown()
}

type LeaderGetter interface {
	GetLeader(partitionKey string) types.ReplicationInfo
}

func NewDiscoverer(config conf.Config) Discoverer {
	return &discoverer{
		config:    config,
		listeners: make([]types.TopologyChangeHandler, 0),
	}
}

type discoverer struct {
	config    conf.Config
	listeners []types.TopologyChangeHandler
}

func (d *discoverer) Init() error {
	return nil
}

func (d *discoverer) Peers() []types.BrokerInfo {
	return nil
}

func (d *discoverer) GetLeader(partitionKey string) types.ReplicationInfo {
	return types.ReplicationInfo{}
}

func (d *discoverer) RegisterListener(l types.TopologyChangeHandler) {
	d.listeners = append(d.listeners, l)
}

func (d *discoverer) Shutdown() {}
