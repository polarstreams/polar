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
//
// It emits events that others like Gossipper listens to.
type Discoverer interface {
	types.Initializer
	LeaderGetter
	// Returns a point-in-time list of all brokers except itself
	Peers() []types.BrokerInfo
	// Returns a point-in-time list of all brokers
	Brokers() []types.BrokerInfo
	RegisterListener(l types.TopologyChangeHandler)
	TokenByOrdinal(ordinal int) types.Token
	Shutdown()
}

type LeaderGetter interface {
	// GetLeader gets the current leader and followers of a given partition key.
	//
	// In case partitionKey is empty, the current node is provided
	GetLeader(partitionKey string) types.ReplicationInfo

	// GetBrokerInfo returns the information of the current broker (self)
	GetBrokerInfo() *types.BrokerInfo
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
	ring      []types.Token
	ordinal   int
}

func (d *discoverer) Init() error {
	if fixedOrdinal, err := strconv.Atoi(os.Getenv(envOrdinal)); err == nil {
		d.ordinal = fixedOrdinal
		d.brokers = parseFixedBrokers(fixedOrdinal)
	} else {
		// Use normal discovery
		replicas, _ := strconv.Atoi(os.Getenv(envReplicas))
		if replicas == 0 {
			replicas = 1
		}
		brokers := make([]types.BrokerInfo, 0, replicas)
		baseHostName := d.config.BaseHostName()
		d.ordinal = d.config.Ordinal()
		for i := 0; i < replicas; i++ {
			brokers = append(brokers, types.BrokerInfo{
				IsSelf:   i == d.ordinal,
				Ordinal:  i,
				HostName: baseHostName + strconv.Itoa(i),
			})
		}

		d.brokers = brokers
	}

	log.Info().Msgf("Discovered cluster with %d total brokers", len(d.brokers))

	d.ring = make([]types.Token, len(d.brokers))
	for i := range d.brokers {
		d.ring[i] = types.GetTokenAtIndex(len(d.brokers), i)
	}

	return nil
}

func (d *discoverer) Peers() []types.BrokerInfo {
	return d.brokersExcept(d.ordinal)
}

func (d *discoverer) Brokers() []types.BrokerInfo {
	return d.brokers
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

func (d *discoverer) GetBrokerInfo() *types.BrokerInfo {
	return &d.brokers[d.ordinal]
}

func (d *discoverer) TokenByOrdinal(ordinal int) types.Token {
	return d.ring[ordinal]
}

func (d *discoverer) GetLeader(partitionKey string) types.ReplicationInfo {
	if partitionKey == "" {
		return types.ReplicationInfo{
			Leader:    d.GetBrokerInfo(),
			Followers: d.Peers(),
			Token:     0,
		}
	}

	token := types.GetToken(partitionKey)
	leaderIndex := types.GetPrimaryTokenIndex(token, d.ring)

	// TODO: Use generation logic
	return types.ReplicationInfo{
		Leader:    &d.brokers[leaderIndex],
		Followers: d.brokersExcept(leaderIndex),
		Token:     d.ring[leaderIndex],
	}
}

func (d *discoverer) brokersExcept(index int) []types.BrokerInfo {
	result := make([]types.BrokerInfo, 0, len(d.brokers)-1)
	for i, broker := range d.brokers {
		if i != index {
			result = append(result, broker)
		}
	}
	return result
}

func (d *discoverer) RegisterListener(l types.TopologyChangeHandler) {
	d.listeners = append(d.listeners, l)
}

func (d *discoverer) Shutdown() {}
