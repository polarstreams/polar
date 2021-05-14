package discovery

import (
	"os"
	"strconv"
	"strings"

	"github.com/jorgebay/soda/internal/conf"
	"github.com/jorgebay/soda/internal/types"
	"github.com/rs/zerolog/log"
)

const (
	envReplicas    = "SODA_REPLICAS"
	envOrdinal     = "SODA_ORDINAL"
	envBrokerNames = "SODA_BROKER_NAMES"
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

func NewDiscoverer(config conf.DiscovererConfig) Discoverer {
	return &discoverer{
		config:    config,
		listeners: make([]types.TopologyChangeHandler, 0),
	}
}

type discoverer struct {
	config    conf.DiscovererConfig
	listeners []types.TopologyChangeHandler
	brokers   []types.BrokerInfo
}

func (d *discoverer) Init() error {
	if fixedOrdinal, err := strconv.Atoi(os.Getenv(envOrdinal)); err == nil {
		d.brokers = parseFixedBrokers(fixedOrdinal)
	} else {
		// Use normal discovery
		replicas, _ := strconv.Atoi(os.Getenv(envReplicas))
		if replicas == 0 {
			replicas = 1
		}
		brokers := make([]types.BrokerInfo, 0, replicas)
		baseHostName := d.config.BaseHostName()
		ordinal := d.config.Ordinal()
		for i := 0; i < replicas; i++ {
			brokers = append(brokers, types.BrokerInfo{
				IsSelf:   i == ordinal,
				Ordinal:  i,
				HostName: baseHostName + strconv.Itoa(i),
			})
		}

		d.brokers = brokers
	}

	log.Info().Msgf("Discovered cluster with %d total brokers", len(d.brokers))
	return nil
}

func (d *discoverer) Peers() []types.BrokerInfo {
	peers := make([]types.BrokerInfo, 0, len(d.brokers)-1)
	for _, broker := range d.brokers {
		if !broker.IsSelf {
			peers = append(peers, broker)
		}
	}
	return peers
}

func parseFixedBrokers(ordinal int) []types.BrokerInfo {
	names := os.Getenv(envBrokerNames)
	if names == "" {
		return []types.BrokerInfo{}
	}

	parts := strings.Split(names, ",")
	brokers := make([]types.BrokerInfo, 0, len(parts))

	for i, hostName := range parts {
		brokers = append(brokers, types.BrokerInfo{
			IsSelf:   i == ordinal,
			Ordinal:  i,
			HostName: hostName,
		})
	}

	return brokers
}

func (d *discoverer) GetLeader(partitionKey string) types.ReplicationInfo {
	return types.ReplicationInfo{}
}

func (d *discoverer) RegisterListener(l types.TopologyChangeHandler) {
	d.listeners = append(d.listeners, l)
}

func (d *discoverer) Shutdown() {}
