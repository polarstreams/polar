package consuming

import . "github.com/jorgebay/soda/internal/types"

// Represents a local view of the consumer group offsets
type OffsetState interface {
	// Gets the offset value for a given group and token.
	// Returns nil when not found
	Get(group string, token Token) *Offset
}

type defaultOffsetState struct {
}

func (s *defaultOffsetState) Get(group string, token Token) *Offset {
	return &Offset{
		Offset:  0,
		Version: 0,
	}
}
