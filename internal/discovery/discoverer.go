package discovery

import (
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/polarstreams/polar/internal/conf"
	"github.com/polarstreams/polar/internal/localdb"
	. "github.com/polarstreams/polar/internal/types"
	"github.com/polarstreams/polar/internal/utils"
	"github.com/rs/zerolog/log"
)

const (
	envOrdinal     = "POLAR_ORDINAL"
	envBrokerNames = "POLAR_BROKER_NAMES"
)

var roundRobinRangeIndex uint32 = 0
var roundRobinBrokerIndex uint32 = 0

// Discoverer provides the cluster topology information.
//
// It emits events that others like Gossipper listens to.
type Discoverer interface {
	Initializer
	Closer
	TopologyGetter
	// Adds a listener that will be invoked when there are changes in the number of replicas change.
	// The func will be invoked using in a single thread, if there are multiple changes it will be invoked sequentially
	RegisterListener(l TopologyChangeListener)
}

type TopologyGetter interface {
	GenerationState

	// Leader gets the current leader and followers of a given partition key.
	//
	// In case partitionKey is empty, the current node is provided
	Leader(partitionKey string) ReplicationInfo

	// LocalInfo returns the information of the current broker (self)
	LocalInfo() *BrokerInfo

	// Returns broker information from current or previous topology
	CurrentOrPastBroker(ordinal int) *BrokerInfo

	// Returns a point-in-time list of all brokers and local info.
	Topology() *TopologyInfo

	// Returns a point-in-time list of all brokers.
	//
	// The slice is sorted in natural order (i.e. 0, 3, 1, 4, 2, 5)
	Brokers() []BrokerInfo
}

type TopologyChangeListener interface {
	OnTopologyChange(previousTopology *TopologyInfo, newTopology *TopologyInfo)
}

type genMap map[Token]Generation

func NewDiscoverer(config conf.DiscovererConfig, localDb localdb.Client) Discoverer {
	generations := atomic.Value{}
	generations.Store(make(genMap))

	return &discoverer{
		config:           config,
		localDb:          localDb,
		listeners:        make([]TopologyChangeListener, 0),
		topology:         atomic.Value{},
		previousTopology: atomic.Value{},
		k8sClient:        newK8sClient(),
		generations:      generations,
		genProposed:      genMap{},
	}
}

type discoverer struct {
	config                conf.DiscovererConfig
	localDb               localdb.Client
	listeners             []TopologyChangeListener
	topology              atomic.Value // Gets the current brokers, index and ring
	previousTopology      atomic.Value // Stores the previous topology to try to access the peers that are leaving the cluster, if possible
	k8sClient             k8sClient
	genMutex              sync.Mutex
	genProposed           genMap
	generations           atomic.Value // copy on write semantics
	clientDiscoveryServer *http.Server
}

func (d *discoverer) Init() error {
	if d.config.DevMode() {
		if err := d.loadFixedTopology(0); err != nil {
			return err
		}
		if err := d.loadGenerations(); err != nil {
			return err
		}
		return d.startClientDiscoveryServer()
	}

	if fixedOrdinal, err := strconv.Atoi(os.Getenv(envOrdinal)); err != nil {
		// Use normal discovery
		if err := d.k8sClient.init(d.config); err != nil {
			log.Err(err).Msgf("K8s client could not be initialized")
			return err
		}

		if err := d.loadTopology(); err != nil {
			return err
		}
	} else {
		// Use env var and file system discovery
		if err := d.loadFixedTopology(fixedOrdinal); err != nil {
			return err
		}
	}

	log.Info().Msgf("Discovered cluster with %d total brokers", len(d.Topology().Brokers))

	if !d.Topology().AmIIncluded() {
		return fmt.Errorf(
			"The current broker is not included in the Topology. " +
				"PolarStreams clusters must have a size of 3*2^n for this broker to be considered.")
	}

	if err := d.loadGenerations(); err != nil {
		return err
	}

	if err := d.startClientDiscoveryServer(); err != nil {
		return err
	}

	go func() {
		time.Sleep(1 * time.Minute)
		for {
			time.Sleep(30 * time.Second)
			existingMap := d.generations.Load().(genMap)
			if len(existingMap) == 0 {
				log.Debug().Msgf("No generations")
				continue
			}
			message := ""
			for _, gen := range existingMap {
				if len(message) > 0 {
					message += ", "
				}
				message += fmt.Sprintf("%s (B%d)", gen.Id(), gen.Leader)
			}
			log.Debug().Msgf("Generations: [%s]", message)
		}
	}()

	return nil
}

func (d *discoverer) Topology() *TopologyInfo {
	value := d.topology.Load()
	if value == nil {
		return nil
	}
	return value.(*TopologyInfo)
}

func (d *discoverer) CurrentOrPastBroker(ordinal int) *BrokerInfo {
	topology := d.Topology()
	broker := topology.BrokerByOrdinal(ordinal)

	if broker != nil {
		return broker
	}

	if previousTopology := d.previousTopology.Load(); previousTopology != nil {
		return previousTopology.(*TopologyInfo).BrokerByOrdinal(ordinal)
	}
	return nil
}

// Gets the desired replicas from k8s api and watches for changes in the statefulset
func (d *discoverer) loadTopology() error {
	totalBrokers, err := d.k8sClient.getDesiredReplicas()
	if err != nil {
		return err
	}
	normalizedLen := utils.ValidRingLength(totalBrokers)
	if normalizedLen != totalBrokers {
		log.Error().Msgf("Not a valid ring size %d, using %d instead", totalBrokers, normalizedLen)
		totalBrokers = normalizedLen
	}

	go d.k8sClient.startWatching(totalBrokers)

	d.topology.Store(createTopology(totalBrokers, d.config))

	go func() {
		for replicasChanged := range d.k8sClient.replicasChangeChan() {
			previousTopology := d.Topology()
			log.Info().Msgf("Topology changed from %d to %d brokers", len(previousTopology.Brokers), replicasChanged)
			normalizedLen := utils.ValidRingLength(replicasChanged)
			if normalizedLen != replicasChanged {
				log.Error().Msgf("Not a valid ring size %d, using %d instead", replicasChanged, normalizedLen)
				replicasChanged = normalizedLen
			}
			topology := createTopology(replicasChanged, d.config)

			// Check whether the normalized number of replicas changed
			if len(topology.Brokers) != len(previousTopology.Brokers) {
				d.swapTopology(topology)
				d.emitTopologyChangeEvent(previousTopology, topology)
			}
		}
	}()
	return nil
}

func createTopology(totalBrokers int, config conf.DiscovererConfig) *TopologyInfo {
	baseHostName := config.BaseHostName()
	localOrdinal := config.Ordinal()
	serviceName := config.ServiceName()
	namespace := config.PodNamespace()

	if namespace != "" {
		// Include the dot to be appended to the host name
		namespace = "." + namespace
	}

	// Ring in sorted by ordinal
	brokers := make([]BrokerInfo, 0, totalBrokers)
	for i := 0; i < totalBrokers; i++ {
		isSelf := i == localOrdinal
		brokers = append(brokers, BrokerInfo{
			IsSelf:   isSelf,
			Ordinal:  i,
			HostName: fmt.Sprintf("%s%d.%s%s", baseHostName, i, serviceName, namespace),
		})
	}

	result := NewTopology(brokers, localOrdinal)
	return &result
}

// Gets the number of topology from env vars and updates it based on file system changes
func (d *discoverer) loadFixedTopology(ordinal int) error {
	names := os.Getenv(envBrokerNames)
	t, err := d.createFixedTopology(ordinal, names)
	if err != nil {
		return err
	}

	d.topology.Store(t)

	go func() {
		// Start watching changes in the file system in the background
		for {
			time.Sleep(d.config.FixedTopologyFilePollDelay())
			previousTopology := d.Topology()
			contents, err := os.ReadFile(filepath.Join(d.config.HomePath(), conf.TopologyFileName))
			if err != nil {
				continue
			}

			names := string(contents)
			if names == "" {
				log.Debug().Msgf("Topology file is empty")
				continue
			}

			topology, err := d.createFixedTopology(ordinal, names)
			if err != nil {
				log.Warn().Err(err).Msgf("There was an error reading file-based topology from file contents")
				continue
			}

			if len(topology.Brokers) != len(previousTopology.Brokers) {
				log.Info().Msgf(
					"Topology changed from %d to %d brokers based on file information",
					len(previousTopology.Brokers),
					len(topology.Brokers))
				d.swapTopology(topology)
				d.emitTopologyChangeEvent(previousTopology, topology)
			}
		}
	}()

	return nil
}

func (d *discoverer) swapTopology(topology *TopologyInfo) {
	previousTopology := d.topology.Swap(topology)
	d.previousTopology.Store(previousTopology)
}

func (d *discoverer) createFixedTopology(ordinal int, names string) (*TopologyInfo, error) {
	if d.config.DevMode() {
		return NewDevTopology(), nil
	}

	if names == "" {
		return nil, fmt.Errorf(
			"When fixed topology is used, you need to define both %s and %s env variables", envOrdinal, envBrokerNames)
	}

	// We expect the names or addresses to be sorted by ordinal
	// e.g. polar-0, polar-1, polar-2, polar-3, polar-4, polar-5
	parts := strings.Split(names, ",")
	if len(parts) < 3 && !d.config.DevMode() {
		return nil, fmt.Errorf("Topology information can't contain less than 3 broker names, obtained %v", parts)
	}

	length := utils.ValidRingLength(len(parts))
	brokers := make([]BrokerInfo, length)

	for i := 0; i < length; i++ {
		brokers[i] = BrokerInfo{
			Ordinal:  i,
			IsSelf:   i == ordinal,
			HostName: parts[i],
		}
	}

	result := NewTopology(brokers, ordinal)
	return &result, nil
}

func (d *discoverer) Brokers() []BrokerInfo {
	return d.Topology().Brokers
}

func (d *discoverer) LocalInfo() *BrokerInfo {
	topology := d.Topology()
	return &topology.Brokers[topology.LocalIndex]
}

func (d *discoverer) Leader(partitionKey string) ReplicationInfo {
	topology := d.Topology()

	if partitionKey == "" {
		return d.primaryTokenDefault(topology)
	}

	// Calculate the token based on the partition key
	token, brokerIndex, rangeIndex := topology.PrimaryToken(HashToken(partitionKey), d.config.ConsumerRanges())

	gen := d.Generation(token)
	if gen == nil {
		// We don't have information about it and it's OK
		// Send it to the natural owner or the natural owner followers
		return ReplicationInfo{
			Leader:     &topology.Brokers[brokerIndex],
			Followers:  topology.NextBrokers(brokerIndex, 2),
			Token:      token,
			RangeIndex: rangeIndex,
		}
	}

	return newReplication(topology, gen, rangeIndex)
}

// Gets the primary token and broker information when there's no partition key
// In most cases, it will point to my token
func (d *discoverer) primaryTokenDefault(topology *TopologyInfo) ReplicationInfo {
	// Use round robin to avoid overloading a range
	rangeIndex := RangeIndex(atomic.AddUint32(&roundRobinRangeIndex, 1) % uint32(d.config.ConsumerRanges()))

	if !topology.AmIIncluded() {
		// Try to route it using the previous topology
		if p := d.previousTopology.Load(); p != nil {
			previousTopology := p.(*TopologyInfo)
			if previousTopology.AmIIncluded() {
				token := previousTopology.MyToken()
				if gen := d.Generation(token); gen != nil {
					// My previous generation is still alive
					// Continue using it
					return newReplication(previousTopology, gen, rangeIndex)
				}
			}
		}

		// Route to any valid broker in the current topology
		brokerIndex := BrokerIndex(atomic.AddUint32(&roundRobinBrokerIndex, 1) % uint32(len(topology.Brokers)))

		return ReplicationInfo{
			Leader:     &topology.Brokers[brokerIndex],
			Followers:  topology.NextBrokers(brokerIndex, 2),
			Token:      topology.GetToken(brokerIndex),
			RangeIndex: rangeIndex,
		}
	}

	// Use my token as default
	gen := d.Generation(topology.MyToken())
	if gen == nil {
		return ReplicationInfo{
			Leader:     nil, // To hint that it could not be found
			Followers:  []BrokerInfo{},
			Token:      topology.MyToken(),
			RangeIndex: rangeIndex,
		}
	}

	return newReplication(topology, gen, rangeIndex)
}

// Uses the leader and follower from the generation, validating that the topology already contains it.
func newReplication(topology *TopologyInfo, gen *Generation, rangeIndex RangeIndex) ReplicationInfo {
	brokersLength := len(topology.Brokers)

	if gen.Leader >= brokersLength {
		return ReplicationInfo{
			Leader:     nil, // Hints that it could not be found
			Followers:  []BrokerInfo{},
			Token:      gen.Start,
			RangeIndex: rangeIndex,
		}
	}

	followers := make([]int, 0, 2)
	for _, ordinal := range gen.Followers {
		if ordinal >= brokersLength {
			continue
		}
		followers = append(followers, ordinal)
	}

	return NewReplicationInfo(topology, gen.Start, gen.Leader, followers, rangeIndex)
}

func (d *discoverer) RegisterListener(l TopologyChangeListener) {
	d.listeners = append(d.listeners, l)
}

func (d *discoverer) emitTopologyChangeEvent(previousTopology *TopologyInfo, newTopology *TopologyInfo) {
	if len(d.listeners) == 0 {
		return
	}

	for _, listener := range d.listeners {
		listener.OnTopologyChange(previousTopology, newTopology)
	}
}

func (d *discoverer) Close() {
	if d.clientDiscoveryServer != nil {
		d.clientDiscoveryServer.Close()
	}
}
