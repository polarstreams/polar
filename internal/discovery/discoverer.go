package discovery

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/jorgebay/soda/internal/conf"
	. "github.com/jorgebay/soda/internal/types"
	"github.com/jorgebay/soda/internal/utils"
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
	GenerationState
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

	// Returns a point-in-time list of all brokers.
	//
	// The slice is sorted in natural order (i.e. 0, 3, 1, 4, 2, 5)
	Brokers() []BrokerInfo

	TokenByOrdinal(ordinal int) Token
}

type TopologyChangeHandler func()

type genMap map[Token]Generation

func NewDiscoverer(config conf.DiscovererConfig) Discoverer {
	generations := atomic.Value{}
	generations.Store(genMap{})

	return &discoverer{
		config:      config,
		listeners:   make([]TopologyChangeHandler, 0),
		generations: generations,
		genProposed: genMap{},
	}
}

type discoverer struct {
	config    conf.DiscovererConfig
	listeners []TopologyChangeHandler
	brokers   []BrokerInfo
	ring      []Token
	// TODO: Remove
	ordinal     int
	genMutex    sync.Mutex
	genProposed genMap
	generations atomic.Value // copy on write semantics
}

func (d *discoverer) Init() error {
	if fixedOrdinal, err := strconv.Atoi(os.Getenv(envOrdinal)); err == nil {
		d.ordinal = fixedOrdinal
		d.brokers = parseFixedBrokers(fixedOrdinal)
	} else {
		// Use normal discovery
		// TODO: Use func and set ordinal once
		d.ordinal = d.config.Ordinal()
		// TODO: Round to 3*2^n
		// TODO: Get local index
		totalBrokers, _ := strconv.Atoi(os.Getenv(envReplicas))
		d.brokers, _ = brokersOrdered(totalBrokers, d.config)
	}

	log.Info().Msgf("Discovered cluster with %d total brokers", len(d.brokers))

	d.ring = make([]Token, len(d.brokers))
	for i := range d.brokers {
		d.ring[i] = GetTokenAtIndex(len(d.brokers), i)
	}

	return nil
}

func brokersOrdered(totalBrokers int, config conf.DiscovererConfig) (brokers []BrokerInfo, localIndex int) {
	if totalBrokers == 0 {
		totalBrokers = 1
	}
	baseHostName := config.BaseHostName()
	localOrdinal := uint32(config.Ordinal())

	// Use ring in natural order
	ordinalList := utils.OrdinalsPlacementOrder(totalBrokers)
	brokers = make([]BrokerInfo, len(ordinalList), len(ordinalList))
	for i := 0; i < len(ordinalList); i++ {
		ordinal := ordinalList[i]
		isSelf := ordinal == localOrdinal
		brokers[i] = BrokerInfo{
			IsSelf:   isSelf,
			Ordinal:  int(ordinal),
			HostName: fmt.Sprintf("%s%d", baseHostName, ordinal),
		}

		if isSelf {
			localIndex = i
		}
	}

	return
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
			Leader: d.LocalInfo(),
			// TODO: gen use current generation
			Followers: d.Peers(),
			Token:     0,
		}
	}

	token := GetToken(partitionKey)
	leaderIndex := GetPrimaryTokenIndex(token, d.ring)

	// TODO: gen use generation
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
