package types

import (
	"fmt"
)

const NotFoundIndex BrokerIndex = -1

// BrokerInfo contains information about a broker
type BrokerInfo struct {
	// IsSelf determines whether the broker refers to this instance
	IsSelf bool
	// Ordinal represents the unique identifier of the broker in a cluster
	Ordinal int
	// HostName contains the reachable host name of the broker, i.e. "broker-1"
	HostName string
}

// ConsumerGroup contains info about a single group of consumers.
// It's used as an interbroker message to send snapshot of the local view of consumers to other brokers.
type ConsumerGroup struct {
	Name   string   `json:"name"`
	Ids    []string `json:"ids"`
	Topics []string `json:"topics"`
}

// BrokerIndex represents the position of a broker in the current broker list.
// It's exposed as different type to avoid mixing it up w/ Ordinal (replica number)
//
// e.g. in a cluster composed of {0, 3, 1, 4, 2, 3}, the index of 3 is 1.
type BrokerIndex int

func (b *BrokerInfo) String() string {
	return fmt.Sprintf("%s (%d)", b.HostName, b.Ordinal)
}

type TopicInfo struct {
	Name string
}

type ReplicationInfo struct {
	Leader     *BrokerInfo
	Followers  []BrokerInfo
	Token      Token
	RangeIndex RangeIndex
}

// SegmentChunk represents a group of compressed records.
type SegmentChunk interface {
	DataBlock() []byte
	StartOffset() uint64
	RecordLength() uint32
}

// TopologyInfo represents a snapshot of the current placement of the brokers
type TopologyInfo struct {
	Brokers        []BrokerInfo        // Brokers ordered by index (e.g. 0,3,1,4,2,5)
	LocalIndex     BrokerIndex         // Index of the current broker relative to this topology instance
	indexByOrdinal map[int]BrokerIndex // Map of key ordinals and value indexes
}

// NewTopology creates a Topology struct using brokers in ordinal order.
func NewTopology(brokersByOrdinal []BrokerInfo) TopologyInfo {
	totalBrokers := len(brokersByOrdinal)
	ordinalList := OrdinalsPlacementOrder(totalBrokers)
	brokers := make([]BrokerInfo, 0, totalBrokers)
	// It can be a slice but let's make it a map
	indexByOrdinal := make(map[int]BrokerIndex, totalBrokers)
	localIndex := 0
	for index, ordinal := range ordinalList {
		b := brokersByOrdinal[ordinal]
		indexByOrdinal[int(ordinal)] = BrokerIndex(index)
		if b.IsSelf {
			localIndex = index
		}
		brokers = append(brokers, b)
	}
	return TopologyInfo{
		Brokers:        brokers,
		LocalIndex:     BrokerIndex(localIndex),
		indexByOrdinal: indexByOrdinal,
	}
}

// GetToken gets the token by the broker index.
func (t *TopologyInfo) GetToken(index BrokerIndex) Token {
	return GetTokenAtIndex(len(t.Brokers), int(index))
}

// MyToken gets the natural token based on the current broker index.
func (t *TopologyInfo) MyToken() Token {
	return GetTokenAtIndex(len(t.Brokers), int(t.LocalIndex))
}

// MyOrdinal gets the current broker's index.
func (t *TopologyInfo) MyOrdinal() int {
	return t.Brokers[t.LocalIndex].Ordinal
}

// GetIndex gets the position of the broker in the broker slice.
//
// It returns NotFoundIndex when not found.
func (t *TopologyInfo) GetIndex(ordinal int) BrokerIndex {
	v, found := t.indexByOrdinal[ordinal]
	if !found {
		return NotFoundIndex
	}
	return v
}

// BrokerByOrdinal gets the broker by a given ordinal.
//
// It returns nil when not found.
func (t *TopologyInfo) BrokerByOrdinal(ordinal int) *BrokerInfo {
	index := t.GetIndex(ordinal)
	if index == NotFoundIndex {
		return nil
	}
	return &t.Brokers[int(index)]
}

// BrokerByOrdinal gets the broker by a given ordinal.
func (t *TopologyInfo) BrokerByOrdinalList(ordinals []int) []BrokerInfo {
	result := make([]BrokerInfo, len(ordinals), len(ordinals))
	for i, v := range ordinals {
		result[i] = t.Brokers[int(t.GetIndex(v))]
	}
	return result
}

// PreviousBroker returns the broker in the position n-1
func (t *TopologyInfo) PreviousBroker() *BrokerInfo {
	index := len(t.Brokers) - 1
	if t.LocalIndex > 0 {
		index = int(t.LocalIndex) - 1
	}

	return &t.Brokers[index]
}

// NextBroker returns the broker in the position n+1
func (t *TopologyInfo) NextBroker() *BrokerInfo {
	return &t.Brokers[(int(t.LocalIndex)+1)%len(t.Brokers)]
}

// NextBrokers returns the broker in the position n+1, n+2, n...
func (t *TopologyInfo) NextBrokers(index BrokerIndex, length int) []BrokerInfo {
	totalBrokers := len(t.Brokers)
	result := make([]BrokerInfo, length, length)
	for i := 0; i < length; i++ {
		result[i] = t.Brokers[(int(index)+1+i)%totalBrokers]
	}

	return result
}

// NaturalFollowers gets the ordinals of the brokers at position n+1 and n+2
func (t *TopologyInfo) NaturalFollowers() []int {
	totalBrokers := len(t.Brokers)
	localIndex := int(t.LocalIndex)
	return []int{
		t.Brokers[(localIndex+1)%totalBrokers].Ordinal,
		t.Brokers[(localIndex+2)%totalBrokers].Ordinal,
	}
}

// Returns the primary token (start of the range), BrokerIndex and Range index for a given token
func (t *TopologyInfo) PrimaryToken(token Token, ranges int) (Token, BrokerIndex, RangeIndex) {
	brokerIndex, rangeIndex := GetPrimaryTokenIndex(token, len(t.Brokers), ranges)
	return t.GetToken(brokerIndex), brokerIndex, rangeIndex
}

// TopicDataId contains information to locate a certain piece of data.
//
// Specifies a topic, for a token, for a defined gen id.
type TopicDataId struct {
	Name       string
	Token      Token
	GenId      GenVersion
	RangeIndex RangeIndex
}

func (t *TopicDataId) String() string {
	return fmt.Sprintf("'%s' %d/%d v%d", t.Name, t.Token, t.RangeIndex, t.GenId)
}

// Replicator contains logic to send data to replicas
type Replicator interface {
	// Sends a message to be stored as replica of current broker's datalog
	SendToFollowers(
		replicationInfo ReplicationInfo,
		topic TopicDataId,
		segmentId uint64,
		chunk SegmentChunk) error
}
