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
	Shutdown()
}

type LeaderGetter interface {
	GetLeader(partitionKey string) types.ReplicationInfo
}

func NewDiscoverer(config conf.Config) Discoverer {
	return &discoverer{
		config,
	}
}

type discoverer struct {
	config conf.Config
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

func (d *discoverer) Shutdown() {}
