package consuming

import (
	"fmt"
	"math"
	"sort"
	"sync"

	. "github.com/google/uuid"
	"github.com/jorgebay/soda/internal/discovery"
	. "github.com/jorgebay/soda/internal/types"
)

type consumerKey string

// Represents a single consumer instance
type ConsumerInfo struct {
	Id     string // A unique id within the consumer group
	Group  string // A group unique id
	Topics []string

	// Only used internally
	assignedTokens []Token
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
}

func NewConsumersMeta(topologyGetter discovery.TopologyGetter) *ConsumersMeta {
	return &ConsumersMeta{
		topologyGetter: nil,
		mu:             sync.Mutex{},
		connections:    map[UUID]ConsumerInfo{},
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

// Returns the tokens and topics that a consumer should read
func (m *ConsumersMeta) CanConsume(id UUID) ([]Token, []string) {
	// Use atomic value that contains a map of consumer -> token
	return nil, nil
}

func (m *ConsumersMeta) Rebalance() bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	consumers := map[consumerKey]ConsumerInfo{}
	consumerGroups := map[string][]string{} // Consumer keys by group
	topics := map[string][]string{} // Topics by consumer group

	for _, info := range m.connections {
		key := info.key()
		_, exists := consumers[key]
		if !exists {
			consumers[key] = info
			// append the list of consumer keys per group
			keyList := consumerGroups[info.Group]
			consumerGroups[info.Group] = append(keyList, string(key))
		}
	}

	for group, keys := range consumerGroups {
		// Union of topic for consumer in the group
		topicSet := map[string]bool{}
		for _, k := range keys {
			c := consumers[consumerKey(k)]
			for _, topic := range c.Topics {
				topicSet[topic] = true
			}
		}

		topicList := make([]string, 0, len(topicSet))
		for t := range topicSet {
			topicList = append(topicList, t)
		}

		topics[group] = topicList




		//TODO: SetConsumerAssignment()
		// m.topologyGetter.Topology()
	}

	return false
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
)  {
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