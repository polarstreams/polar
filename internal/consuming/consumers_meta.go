package consuming

import (
	"fmt"
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
	consumers      map[consumerKey]ConsumerInfo

	//TODO: Consumer by consumer id (group+id)
}

func NewConsumersMeta(topologyGetter discovery.TopologyGetter) *ConsumersMeta {
	return &ConsumersMeta{
		topologyGetter: nil,
		mu:             sync.Mutex{},
		connections:    map[UUID]ConsumerInfo{},
		consumers:      map[consumerKey]ConsumerInfo{},
	}
}

func (m *ConsumersMeta) AddConnection(id UUID, consumer ConsumerInfo) {

}

func (m *ConsumersMeta) RemoveConnection(id UUID) bool {
	return false
}

func (m *ConsumersMeta) CanConsume(id UUID) (*ConsumerInfo, []Token) {
	// Use atomic value that contains a map of consumer -> token
	return nil, nil
}

func (m *ConsumersMeta) Rebalance() bool {
	return false
}
