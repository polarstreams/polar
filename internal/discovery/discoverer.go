package discovery

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/jorgebay/soda/internal/conf"
	"github.com/jorgebay/soda/internal/localdb"
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
	GenerationState

	// Leader gets the current leader and followers of a given partition key.
	//
	// In case partitionKey is empty, the current node is provided
	Leader(partitionKey string) ReplicationInfo

	// LocalInfo returns the information of the current broker (self)
	LocalInfo() *BrokerInfo

	// Returns a point-in-time list of all brokers except itself
	Peers() []BrokerInfo

	// Returns a point-in-time list of all brokers and local info.
	Topology() *TopologyInfo

	// Returns a point-in-time list of all brokers.
	//
	// The slice is sorted in natural order (i.e. 0, 3, 1, 4, 2, 5)
	Brokers() []BrokerInfo

	TokenByOrdinal(ordinal int) Token // TODO: Probably unnecessary
}

type TopologyChangeHandler func()

type genMap map[Token]Generation

func NewDiscoverer(config conf.DiscovererConfig, localDb localdb.Client) Discoverer {
	generations := atomic.Value{}
	generations.Store(genMap{})

	return &discoverer{
		config:      config,
		localDb:     localDb,
		listeners:   make([]TopologyChangeHandler, 0),
		generations: generations,
		genProposed: genMap{},
	}
}

type discoverer struct {
	config      conf.DiscovererConfig
	localDb     localdb.Client
	listeners   []TopologyChangeHandler
	topology    TopologyInfo // Gets the current brokers, index and ring
	genMutex    sync.Mutex
	genProposed genMap
	generations atomic.Value // copy on write semantics
}

func (d *discoverer) Init() error {
	if fixedOrdinal, err := strconv.Atoi(os.Getenv(envOrdinal)); err == nil {
		d.topology = createFixedTopology(fixedOrdinal)
	} else {
		// Use normal discovery
		// TODO: Validate and round to 3*2^n
		totalBrokers, _ := strconv.Atoi(os.Getenv(envReplicas))
		d.topology = createTopology(totalBrokers, d.config)
	}

	log.Info().Msgf("Discovered cluster with %d total brokers", len(d.topology.Brokers))

	return nil
}

func (d *discoverer) Topology() *TopologyInfo {
	return &d.topology
}

func createTopology(totalBrokers int, config conf.DiscovererConfig) TopologyInfo {
	if totalBrokers == 0 {
		totalBrokers = 1
	}
	baseHostName := config.BaseHostName()
	localOrdinal := config.Ordinal()

	// Ring in sorted by ordinal
	brokers := make([]BrokerInfo, 0, totalBrokers)
	for i := 0; i < totalBrokers; i++ {
		isSelf := i == localOrdinal
		brokers = append(brokers, BrokerInfo{
			IsSelf:   isSelf,
			Ordinal:  i,
			HostName: fmt.Sprintf("%s%d", baseHostName, i),
		})
	}

	return NewTopology(brokers)
}

func createFixedTopology(ordinal int) TopologyInfo {
	names := os.Getenv(envBrokerNames)
	if names == "" {
		return TopologyInfo{}
	}

	// We expect the names to be sorted by ordinal (0, 1, 2, 3, 4, 6)
	parts := strings.Split(names, ",")
	brokers := make([]BrokerInfo, len(parts), len(parts))
	// TODO: Validate and round to 3*2^n
	// ordinalList := utils.OrdinalsPlacementOrder(len(parts))

	for i, hostName := range parts {
		brokers[i] = BrokerInfo{
			Ordinal:  i,
			IsSelf:   i == ordinal,
			HostName: hostName,
		}
	}

	return NewTopology(brokers)
}

func (d *discoverer) Peers() []BrokerInfo {
	return d.brokersExcept(d.topology.LocalIndex)
}

func (d *discoverer) Brokers() []BrokerInfo {
	return d.topology.Brokers
}

func (d *discoverer) LocalInfo() *BrokerInfo {
	topology := d.topology
	return &topology.Brokers[topology.LocalIndex]
}

func (d *discoverer) TokenByOrdinal(ordinal int) Token {
	index := d.topology.GetIndex(ordinal)
	return d.topology.GetToken(index)
}

func (d *discoverer) Leader(partitionKey string) ReplicationInfo {
	topology := d.topology
	if partitionKey == "" {
		return ReplicationInfo{
			Leader: d.LocalInfo(),
			// TODO: gen use current generation
			Followers: d.Peers(),
			Token:     0,
		}
	}

	token := GetToken(partitionKey)
	leaderIndex := GetPrimaryTokenIndex(token, len(topology.Brokers))

	// TODO: gen use generation
	return ReplicationInfo{
		Leader:    &topology.Brokers[leaderIndex],
		Followers: followers(topology.Brokers, leaderIndex),
		Token:     d.topology.GetToken(leaderIndex),
	}
}

func (d *discoverer) brokersExcept(index BrokerIndex) []BrokerInfo {
	brokers := d.topology.Brokers
	result := make([]BrokerInfo, 0, len(brokers)-1)
	for i, broker := range brokers {
		if i != int(index) {
			result = append(result, broker)
		}
	}
	return result
}

func (d *discoverer) RegisterListener(l TopologyChangeHandler) {
	d.listeners = append(d.listeners, l)
}

func (d *discoverer) Shutdown() {}

// followers gets the next two brokers according to the broker order.
func followers(brokers []BrokerInfo, index BrokerIndex) []BrokerInfo {
	brokersLength := len(brokers)
	if brokersLength < 3 {
		return []BrokerInfo{}
	}
	result := make([]BrokerInfo, 2, 2)
	for i := 0; i < 2; i++ {
		result[i] = brokers[(int(index)+i+1)%brokersLength]
	}
	return result
}
