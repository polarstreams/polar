package types

type OffsetCommitType int
type CompareResult int

const (
	OffsetCommitNone OffsetCommitType = iota
	OffsetCommitLocal
	OffsetCommitAll
)

const (
	CompareEqual CompareResult = iota
	CompareLessThan
	CompareGreaterThan
)

// Represents a topic offset for a given token.
type Offset struct {
	Offset  int64      `json:"offsetValue"`
	Version GenVersion `json:"version"`
	Source  GenId      `json:"source"` // The point-in-time when the offset was recorded.
}

func (o *Offset) Compare(other *Offset) CompareResult {
	if other == nil {
		return CompareGreaterThan
	}

	if o.Version < other.Version {
		return CompareLessThan
	}
	if o.Version > other.Version {
		return CompareGreaterThan
	}
	if o.Offset < other.Offset {
		return CompareLessThan
	}
	if o.Offset > other.Offset {
		return CompareGreaterThan
	}
	return CompareEqual
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

	// Get the lowest offset value of any group for a given topic+token+index
	MinOffset(topic string, token Token, index RangeIndex) *Offset
}
