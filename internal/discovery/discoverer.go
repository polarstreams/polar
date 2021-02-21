package discovery

import (
	"github.com/jorgebay/soda/internal/conf"
	"github.com/jorgebay/soda/internal/types"
)

// Discoverer provides the cluster topology information.
type Discoverer interface {
	Init() error
	Peers() []types.BrokerInfo
	Shutdown()
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

func (d *discoverer) Shutdown() {}
