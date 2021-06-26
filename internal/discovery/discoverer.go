package discovery

import (
	"os"
	"strconv"
	"strings"

	"github.com/jorgebay/soda/internal/conf"
	. "github.com/jorgebay/soda/internal/types"
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
	Initializer
	TopologyGetter
	RegisterListener(l TopologyChangeHandler)
	Shutdown()
}

type TopologyGetter interface {
	// Leader gets the current leader and followers of a given partition key.
	//
	// In case partitionKey is empty, the current node is provided
	Leader(partitionKey string) ReplicationInfo

	// LocalInfo returns the information of the current broker (self)
	LocalInfo() *BrokerInfo

	// Returns a point-in-time list of all brokers except itself
	Peers() []BrokerInfo

	// Returns a point-in-time list of all brokers
	Brokers() []BrokerInfo

	TokenByOrdinal(ordinal int) Token
}

type TopologyChangeHandler func()

func NewDiscoverer(config conf.DiscovererConfig) Discoverer {
	return &discoverer{
		config:    config,
		listeners: make([]TopologyChangeHandler, 0),
	}
}

type discoverer struct {
	config    conf.DiscovererConfig
	listeners []TopologyChangeHandler
	brokers   []BrokerInfo
	ring      []Token
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
		brokers := make([]BrokerInfo, 0, replicas)
		baseHostName := d.config.BaseHostName()
		d.ordinal = d.config.Ordinal()
		for i := 0; i < replicas; i++ {
			brokers = append(brokers, BrokerInfo{
				IsSelf:   i == d.ordinal,
				Ordinal:  i,
				HostName: baseHostName + strconv.Itoa(i),
			})
		}

		d.brokers = brokers
	}

	log.Info().Msgf("Discovered cluster with %d total brokers", len(d.brokers))

	d.ring = make([]Token, len(d.brokers))
	for i := range d.brokers {
		d.ring[i] = GetTokenAtIndex(len(d.brokers), i)
	}

	return nil
}

func (d *discoverer) Peers() []BrokerInfo {
	return d.brokersExcept(d.ordinal)
}

func (d *discoverer) Brokers() []BrokerInfo {
	return d.brokers
}

func parseFixedBrokers(ordinal int) []BrokerInfo {
	names := os.Getenv(envBrokerNames)
	if names == "" {
		return []BrokerInfo{}
	}

	parts := strings.Split(names, ",")
	brokers := make([]BrokerInfo, 0, len(parts))

	for i, hostName := range parts {
		brokers = append(brokers, BrokerInfo{
			IsSelf:   i == ordinal,
			Ordinal:  i,
			HostName: hostName,
		})
	}

	return brokers
}

func (d *discoverer) LocalInfo() *BrokerInfo {
	return &d.brokers[d.ordinal]
}

func (d *discoverer) TokenByOrdinal(ordinal int) Token {
	return d.ring[ordinal]
}

func (d *discoverer) Leader(partitionKey string) ReplicationInfo {
	if partitionKey == "" {
		return ReplicationInfo{
			Leader:    d.LocalInfo(),
			Followers: d.Peers(),
			Token:     0,
		}
	}

	token := GetToken(partitionKey)
	leaderIndex := GetPrimaryTokenIndex(token, d.ring)

	// TODO: Use generation logic
	return ReplicationInfo{
		Leader:    &d.brokers[leaderIndex],
		Followers: d.brokersExcept(leaderIndex),
		Token:     d.ring[leaderIndex],
	}
}

func (d *discoverer) brokersExcept(index int) []BrokerInfo {
	result := make([]BrokerInfo, 0, len(d.brokers)-1)
	for i, broker := range d.brokers {
		if i != index {
			result = append(result, broker)
		}
	}
	return result
}

func (d *discoverer) RegisterListener(l TopologyChangeHandler) {
	d.listeners = append(d.listeners, l)
}

func (d *discoverer) Shutdown() {}
