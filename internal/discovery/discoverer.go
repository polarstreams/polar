package discovery

import (
	"github.com/jorgebay/soda/internal/configuration"
	"github.com/jorgebay/soda/internal/types"
)

// Discoverer provides the cluster topology information.
type Discoverer interface {
	Init() error
	Peers() []types.BrokerInfo
	Shutdown()
}

func NewDiscoverer(config *configuration.Config) Discoverer {
	return &discoverer{}
}

type discoverer struct {
}

func (d *discoverer) Init() error {
	return nil
}

func (d *discoverer) Peers() []types.BrokerInfo {
	return nil
}

func (d *discoverer) Shutdown() {}