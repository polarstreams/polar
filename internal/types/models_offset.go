package types

type OffsetCommitType int

const (
	OffsetCommitNone OffsetCommitType = iota
	OffsetCommitLocal
	OffsetCommitAll
)

// Represents a topic offset for a given token.
type Offset struct {
	Offset  int64      `json:"offsetValue"`
	Version GenVersion `json:"version"`
	Source  GenId      `json:"source"` // The point-in-time when the offset was recorded.
}

// Represents an identifier of an offset to be persisted
type OffsetStoreKey struct {
	Group      string     `json:"group"`
	Topic      string     `json:"topic"`
	Token      Token      `json:"token"`
	RangeIndex RangeIndex `json:"rangeIndex"`
}

// Represents an identifier and a value of an offset
type OffsetStoreKeyValue struct {
	Key   OffsetStoreKey `json:"key"`
	Value Offset         `json:"value"`
}

// Represents a local view of the consumer group offsets
type OffsetState interface {
	Initializer

	// Gets the offset value for a given group and token.
	// Returns nil when not found
	Get(group string, topic string, token Token, rangeIndex RangeIndex) *Offset

	// Sets the known offset value in memory, optionally commiting it to the data store
	Set(group string, topic string, token Token, rangeIndex RangeIndex, value Offset, commit OffsetCommitType)

	// Reads the local max producer offset from disk
	ProducerOffsetLocal(topic *TopicDataId) (int64, error)
}
