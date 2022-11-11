package types

import (
	"fmt"
	"math"
	"time"
)

type OffsetCommitType int

const (
	OffsetCommitNone OffsetCommitType = iota
	OffsetCommitLocal
	OffsetCommitAll
)

const OffsetCompleted = math.MaxInt64

type OffsetSource struct {
	Id        GenId `json:"id"` // Gen id of the source
	Timestamp int64 `json:"ts"` // Timestamp in Unix Micros
}

func NewOffsetSource(id GenId) OffsetSource {
	return OffsetSource{
		Id:        id,
		Timestamp: time.Now().UnixMicro(),
	}
}

// Represents a topic offset for a given token.
type Offset struct {
	Version     GenVersion   `json:"version"`     // Generation version of the offset
	ClusterSize int          `json:"clusterSize"` // Cluster size of the gen version
	Offset      int64        `json:"value"`       // Numerical offset value
	Token       Token        `json:"token"`       // The start token of the offset generation
	Index       RangeIndex   `json:"index"`       // The range index of the offset
	Source      OffsetSource `json:"source"`      // The point-in-time when the offset was recorded.
}

func NewOffset(topic *TopicDataId, clusterSize int, source GenId, value int64) Offset {
	return Offset{
		Token:       topic.Token,
		Index:       topic.RangeIndex,
		Version:     topic.Version,
		ClusterSize: clusterSize,
		Offset:      value,
		Source:      NewOffsetSource(source),
	}
}

func NewDefaultOffset(topic *TopicDataId, clusterSize int, value int64) Offset {
	return Offset{
		Token:       topic.Token,
		Index:       topic.RangeIndex,
		Version:     topic.Version,
		ClusterSize: clusterSize,
		Offset:      value,
		Source:      OffsetSource{},
	}
}

func (o *Offset) GenId() GenId {
	return GenId{
		Start:   o.Token,
		Version: o.Version,
	}
}

func (o *Offset) String() string {
	return fmt.Sprintf("v%d %d", o.Version, o.Offset)
}

// Represents an identifier of an offset to be persisted
type OffsetStoreKey struct {
	Group string `json:"group"`
	Topic string `json:"topic"`
}

// Represents an identifier and a value of an offset
type OffsetStoreKeyValue struct {
	Key   OffsetStoreKey `json:"key"`
	Value Offset         `json:"value"`
}

// Represents a local view of the consumer group offsets
type OffsetState interface {
	Initializer
	fmt.Stringer

	// Gets the offset value for a given group and range.
	// Returns nil, false when not found
	//
	// The caller MUST check whether the current broker can serve the data when ranges don't match
	// The caller MUST check whether the consumer is assigned when ranges don't match
	Get(group string, topic string, token Token, index RangeIndex, clusterSize int) (offset *Offset, rangesMatch bool)

	// Gets offset values in order for a given group and range with defaults values.
	//
	// The caller MUST check whether the current broker can serve the data and that the consumer is assigned.
	GetAllWithDefaults(
		group string,
		topic string,
		token Token,
		rangeIndex RangeIndex,
		clusterSize int,
		policy OffsetResetPolicy) []Offset

	// Sets the known offset value in memory, optionally committing it to the data store
	Set(group string, topic string, value Offset, commit OffsetCommitType) bool

	// Returns the max produced offset from local and peers.
	// When it can not be found, it returns a negative value.
	// When there's an unexpected  error on local and peers, it returns an error
	MaxProducedOffset(topicId *TopicDataId) (int64, error)
}
