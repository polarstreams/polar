package consuming

import . "github.com/jorgebay/soda/internal/types"

// Represents a local view of the consumer group offsets
type OffsetState interface {
	// Gets the offset value for a given group and token.
	// Returns nil when not found
	Get(group string, topic string, token Token, rangeIndex RangeIndex) *Offset

	// Determines whether the consumer group can be served with token data.
	// It navigates through the generation tree, looking for parents.
	//
	// Only called one per consumer group reader.
	CanConsumeToken(group string, topic string, gen Generation) bool
}

type defaultOffsetState struct {
}

func (s *defaultOffsetState) Get(group string, topic string, token Token, rangeIndex RangeIndex) *Offset {
	return &Offset{
		Offset:  0,
		Version: 1,

		Source: 0,
	}
}

func (s *defaultOffsetState) CanConsumeToken(group string, topic string, gen Generation) bool {
	//TODO: Implement
	return true
}
