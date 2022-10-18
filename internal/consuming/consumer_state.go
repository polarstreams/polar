package consuming

import (
	"fmt"
	"math"
	"reflect"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/barcostreams/barco/internal/conf"
	"github.com/barcostreams/barco/internal/discovery"
	. "github.com/barcostreams/barco/internal/types"
)

const staleInfoThreshold = 5 * time.Minute
const removeDelay = 1 * time.Minute
const timerPrecision = 20 * time.Millisecond

type consumerKey string

// Represents a local view of the consumer instances.
type ConsumerState struct {
	config         conf.BasicConfig
	topologyGetter discovery.TopologyGetter
	removeDelay    time.Duration

	mu                 sync.Mutex
	connections        map[string]ConsumerInfo     // Consumers by connection id
	trackedConnections map[string]*trackedConsumer // Connections by id // TODO: Reevaluate
	peerGroups         map[int]peerGroupInfo       // Group information provided by a peer, by ordinal
	recentlyRemoved    map[consumerKey]removedInfo // Consumers which connections were recently removed by key

	// Snapshot information recalculated periodically
	groups    atomic.Value // Precalculated info of consumer groups for peers
	consumers atomic.Value // Precalculated info of consumers by connection uuid
}

type peerGroupInfo struct {
	groups    []ConsumerGroup
	timestamp time.Time
}

type removedInfo struct {
	consumer  ConsumerInfo
	timestamp time.Time
}

// The global unique identifier of the Consumer
func (c *ConsumerInfo) key() consumerKey {
	return consumerKey(fmt.Sprintf("%s-%s", c.Group, c.Id))
}

func NewConsumerState(config conf.BasicConfig, topologyGetter discovery.TopologyGetter) *ConsumerState {
	return &ConsumerState{
		config:             config,
		topologyGetter:     topologyGetter,
		mu:                 sync.Mutex{},
		connections:        map[string]ConsumerInfo{},
		trackedConnections: map[string]*trackedConsumer{},
		peerGroups:         map[int]peerGroupInfo{},
		recentlyRemoved:    map[consumerKey]removedInfo{},
		removeDelay:        removeDelay - timerPrecision,
	}
}

// Add the new connection and returns the new number of connections
func (m *ConsumerState) AddConnection(tc *trackedConsumer, consumer ConsumerInfo) (bool, int) {
	m.mu.Lock()
	defer m.mu.Unlock()

	id := tc.Id()
	_, found := m.connections[id]

	m.connections[id] = consumer
	m.trackedConnections[id] = tc

	return !found, len(m.connections)
}

// Removes the connection when found and returns the new number of connections.
func (m *ConsumerState) RemoveConnection(id string) (bool, int) {
	m.mu.Lock()
	defer m.mu.Unlock()

	consumer, found := m.connections[id]
	if found {
		delete(m.connections, id)
		delete(m.trackedConnections, id)

		// Add it to a pending remove list
		m.recentlyRemoved[consumer.key()] = removedInfo{
			consumer:  consumer,
			timestamp: time.Now(),
		}
	}
	return found, len(m.connections)
}

// Gets a copy of the current open connections
func (m *ConsumerState) GetConnections() []*trackedConsumer {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]*trackedConsumer, 0, len(m.trackedConnections))
	for _, conn := range m.trackedConnections {
		result = append(result, conn)
	}
	return result
}

func (m *ConsumerState) SetInfoFromPeer(ordinal int, groups []ConsumerGroup) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.peerGroups[ordinal] = peerGroupInfo{
		groups:    groups,
		timestamp: time.Now(),
	}
}

func (m *ConsumerState) GetInfoForPeers() []ConsumerGroup {
	value := m.groups.Load()

	if value == nil {
		return []ConsumerGroup{}
	}
	return value.([]ConsumerGroup)
}

// Returns the tokens and topics that a consumer should read
func (m *ConsumerState) CanConsume(id string) (string, []TokenRanges, []string) {
	value := m.consumers.Load()

	if value == nil {
		return "", nil, nil
	}

	connections, found := value.(map[string]ConsumerInfo)

	if !found {
		return "", nil, nil
	}

	info := connections[id]

	// TODO: Revisit token/ranges assignment algorithm
	// Assign all ranges to the same consumer for now
	ranges := make([]TokenRanges, 0, len(info.assignedTokens))
	for _, t := range info.assignedTokens {
		indices := make([]RangeIndex, 0, m.config.ConsumerRanges())
		for i := 0; i < m.config.ConsumerRanges(); i++ {
			indices = append(indices, RangeIndex(i))
		}
		ranges = append(ranges, TokenRanges{Token: t, Indices: indices})
	}

	return info.Group, ranges, info.Topics
}

func (m *ConsumerState) Rebalance() bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	// This logic could be vastly improved (i.e. nested structures, unnecessary iterations, ...)
	// when we have more time.
	// In the meantime, this is not a hot path and shouldn't affect performance
	consumers := map[consumerKey]ConsumerInfo{}
	consumerGroups := map[string]StringSet{} // Consumer keys by group
	topics := map[string]StringSet{}         // Topics set by consumer group

	// From the local connections, create the consumer groups
	for _, info := range m.connections {
		addToGroup(consumerGroups, consumers, info)
	}

	// From the recently removed, add it to active consumers
	for k, removed := range m.recentlyRemoved {
		if time.Since(removed.timestamp) > m.removeDelay {
			delete(m.recentlyRemoved, k)
			continue
		}
		addToGroup(consumerGroups, consumers, removed.consumer)
	}

	// From the consumer groups, add the topics
	for group, keys := range consumerGroups {
		// Union of topic for consumer in the group
		topicSet := StringSet{}
		for k := range keys {
			c := consumers[consumerKey(k)]
			for _, topic := range c.Topics {
				topicSet.Add(topic)
			}
		}

		topics[group] = topicSet
	}

	for k, peerInfo := range m.peerGroups {
		if time.Since(peerInfo.timestamp) > staleInfoThreshold {
			delete(m.peerGroups, k)
			continue
		}

		for _, group := range peerInfo.groups {
			// Set topic subscriptions
			topicSet, found := topics[group.Name]
			if !found {
				topicSet = StringSet{}
			}
			topicSet.Add(group.Topics...)
			topics[group.Name] = topicSet

			for _, id := range group.Ids {
				info := ConsumerInfo{
					Id:    id,
					Group: group.Name,
				}
				key := info.key()
				_, exists := consumers[key]
				if !exists {
					consumers[key] = info
					// append the list of consumer keys per group
					keys := consumerGroups[info.Group]
					keys.Add(string(key))
					consumerGroups[info.Group] = keys
				}
			}
		}
	}

	// Prepare snapshot values: groups
	fullConsumerInfo := map[consumerKey]ConsumerInfo{}
	topology := m.topologyGetter.Topology()
	groupsForPeers := []ConsumerGroup{}

	for group := range consumerGroups {
		keySet := consumerGroups[group]
		topicSet := topics[group]

		keys := keySet.ToSortedSlice()
		topics := topicSet.ToSortedSlice()

		setConsumerAssignment(fullConsumerInfo, topology, keys, topics, consumers)

		groupsForPeers = append(groupsForPeers, ConsumerGroup{
			Name:   group,
			Ids:    toIds(keys, consumers),
			Topics: topics,
		})
	}

	// Prepare snapshot values: consumer by connection
	consumersByConnection := make(map[string]ConsumerInfo, len(m.connections))
	for id, info := range m.connections {
		consumersByConnection[id] = fullConsumerInfo[info.key()]
	}

	sort.Slice(groupsForPeers, func(i, j int) bool {
		return groupsForPeers[i].Name < groupsForPeers[j].Name
	})

	//TODO: Before swapping, if hasChanged, reset offset to lastStoredPosition

	prevGroups := m.groups.Swap(groupsForPeers)
	m.consumers.Swap(consumersByConnection)

	// Determine if there was a change
	hasChanged := !reflect.DeepEqual(prevGroups, groupsForPeers)

	return hasChanged
}

func toIds(keys []string, consumers map[consumerKey]ConsumerInfo) []string {
	result := make([]string, len(keys))

	for i, k := range keys {
		result[i] = consumers[consumerKey(k)].Id
	}

	return result
}

func addToGroup(
	consumerGroups map[string]StringSet,
	consumers map[consumerKey]ConsumerInfo,
	info ConsumerInfo,
) {
	key := info.key()
	_, exists := consumers[key]
	if !exists {
		consumers[key] = info
		// append the list of consumer keys per group
		keys, found := consumerGroups[info.Group]
		if !found {
			keys = StringSet{}
		}
		keys.Add(string(key))
		consumerGroups[info.Group] = keys
	}
}

// Calculates token assignment for a given set of consumers (in a group).
//
// From the consumer index an ordinal is computed and used to assign broker
// tokens to consumers.
func setConsumerAssignment(
	result map[consumerKey]ConsumerInfo,
	topology *TopologyInfo,
	keys []string,
	topics []string, // topics assigned to a group
	consumers map[consumerKey]ConsumerInfo,
) {
	// Sort the keys within a group
	sort.Strings(keys)
	brokerLength := len(topology.Brokers)
	consumerLength := len(keys)

	// Distribute fairly in a theoretical ring with 3*2^n shape
	baseLength := consumerBaseLength(consumerLength)
	consumerTokens := make([][]Token, baseLength)

	index := 0
	for brokerIndex, broker := range topology.Brokers {
		if len(consumerTokens) > broker.Ordinal {
			// The alphabetical index is considered maps to the broker ordinal
			index = broker.Ordinal
		}

		consumerTokens[index] = append(consumerTokens[index], GetTokenAtIndex(brokerLength, brokerIndex))
	}

	// Assign the tokens to the keys that do exist
	for i, key := range keys {
		info := consumers[consumerKey(key)]
		c := result[consumerKey(key)]
		c.Id = info.Id
		c.Group = info.Group
		c.assignedTokens = consumerTokens[i]
		c.Topics = topics

		result[consumerKey(key)] = c
	}

	if baseLength > consumerLength {
		// Assign the tokens from the theoretical ring to the actual ring
		remainingTokens := make([]Token, 0)
		for i := consumerLength; i < baseLength; i++ {
			remainingTokens = append(remainingTokens, consumerTokens[i]...)
		}

		for i, token := range remainingTokens {
			// Assign backwards to avoid overloading the consumer at zero
			index := int(math.Abs(float64((consumerLength - 1 - i) % consumerLength)))
			key := keys[index]
			c := result[consumerKey(key)]
			c.assignedTokens = append(c.assignedTokens, token)
			result[consumerKey(key)] = c
		}
	}

	// Fill the unused consumers
	for i := brokerLength; i < len(keys); i++ {
		key := consumerKey(keys[i])
		result[key] = consumers[key]
	}
}

// For a given real consumers length, it returns the ring length that
// can contain it.
// For example: given 3 it returns 3; for 4 -> 6; for 5 -> 6; for 7 -> 12
func consumerBaseLength(length int) int {
	if length < 3 {
		return 3
	}
	// Rings are 3 * 2^n
	exponent := math.Ceil(math.Log2(float64(length) / 3))
	return int(3 * math.Exp2(exponent))
}
