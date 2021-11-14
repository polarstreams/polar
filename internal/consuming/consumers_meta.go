package consuming

import (
	"fmt"
	"math"
	"reflect"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	. "github.com/google/uuid"
	"github.com/jorgebay/soda/internal/discovery"
	. "github.com/jorgebay/soda/internal/types"
)

const staleInfoThreshold = 10 * time.Minute

type consumerKey string

// Represents a single consumer instance
type ConsumerInfo struct {
	Id     string // A unique id within the consumer group
	Group  string // A group unique id
	Topics []string

	// Only used internally
	assignedTokens []Token
}

type peerGroupInfo struct {
	groups    []ConsumerGroup
	timestamp time.Time
}

// The global unique identifier of the Consumer
func (c *ConsumerInfo) key() consumerKey {
	return consumerKey(fmt.Sprintf("%s-%s", c.Group, c.Id))
}

// A topology and group metadata of the consumers
type ConsumersMeta struct {
	topologyGetter discovery.TopologyGetter
	mu             sync.Mutex
	connections    map[UUID]ConsumerInfo // Consumers by connection id
	peerGroups     map[int]peerGroupInfo // Group information provided by a peer, by ordinal

	// Snapshot information recalculated periodically
	groups    atomic.Value // Precalculated info of consumer groups for peers
	consumers atomic.Value // Precalculated info of consumers by connection uuid
}

func NewConsumersMeta(topologyGetter discovery.TopologyGetter) *ConsumersMeta {
	return &ConsumersMeta{
		topologyGetter: topologyGetter,
		mu:             sync.Mutex{},
		connections:    map[UUID]ConsumerInfo{},
		peerGroups:     map[int]peerGroupInfo{},
	}
}

func (m *ConsumersMeta) AddConnection(id UUID, consumer ConsumerInfo) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.connections[id] = consumer
}

func (m *ConsumersMeta) RemoveConnection(id UUID) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	_, found := m.connections[id]
	delete(m.connections, id)
	return found
}

func (m *ConsumersMeta) SetInfoFromPeer(ordinal int, groups []ConsumerGroup) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.peerGroups[ordinal] = peerGroupInfo{
		groups:    groups,
		timestamp: time.Now(),
	}
}

func (m *ConsumersMeta) GetInfoForPeers() []ConsumerGroup {
	value := m.groups.Load()

	if value == nil {
		return []ConsumerGroup{}
	}
	return value.([]ConsumerGroup)
}

// Returns the tokens and topics that a consumer should read
func (m *ConsumersMeta) CanConsume(id UUID) ([]Token, []string) {
	value := m.consumers.Load()

	if value == nil {
		return nil, nil
	}

	connections, found := value.(map[UUID]ConsumerInfo)

	if !found {
		return nil, nil
	}

	info := connections[id]
	return info.assignedTokens, info.Topics
}

func (m *ConsumersMeta) Rebalance() bool {
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

		keys := keySet.ToSlice()
		topics := topicSet.ToSlice()

		setConsumerAssignment(fullConsumerInfo, topology, keys, topics, consumers)

		groupsForPeers = append(groupsForPeers, ConsumerGroup{
			Name:   group,
			Ids:    toIds(keys, consumers),
			Topics: topics,
		})
	}

	// Prepare snapshot values: consumer by connection
	consumersByConnection := make(map[UUID]ConsumerInfo, len(m.connections))
	for id, info := range m.connections {
		consumersByConnection[id] = fullConsumerInfo[info.key()]
	}

	prevGroups := m.groups.Swap(groupsForPeers)
	m.consumers.Swap(consumersByConnection)

	// Determine if there was a change
	result := reflect.DeepEqual(prevGroups, groupsForPeers)

	return result
}

func toIds(keys []string, consumers map[consumerKey]ConsumerInfo) []string {
	result := make([]string, len(keys))

	for i, k := range keys {
		result[i] = consumers[consumerKey(k)].Id
	}

	return result
}

// Calculates token assignment for a given set of consumers (in a group).
//
// From the consumer index an ordinal is computed.
//
// When the number of consumers is greater or equal than the number of brokers
// the consumer will get the token assigned based on the consumer "ordinal".
// When there are fewer consumers than brokers it will be assigned in the following way:
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

	prevKey := keys[0]
	for index, broker := range topology.Brokers {
		key := prevKey
		if len(keys) > broker.Ordinal {
			key = keys[broker.Ordinal]
			prevKey = key
		}

		info := consumers[consumerKey(key)]
		c := result[consumerKey(key)]
		c.Id = info.Id
		c.Group = info.Group
		c.assignedTokens = append(c.assignedTokens, GetTokenAtIndex(brokerLength, index))
		c.Topics = topics

		result[consumerKey(key)] = c
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
func consumerOrdinalLength(length int) int {
	if length < 3 {
		return 3
	}
	// Rings are 3 * 2^n
	exponent := math.Ceil(math.Log2(float64(length) / 3))
	return int(3 * math.Exp2(exponent))
}
