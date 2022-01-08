package consuming

import . "github.com/jorgebay/soda/internal/types"

// Represents a local view of the consumer group offsets
type OffsetState interface {
	Get(group string, token Token) Offset
}

type defaultOffsetState struct {
}

func (s *defaultOffsetState) Get(group string, token Token) Offset {
	return Offset{
		Offset:  0,
		Version: 0,
	}
}
