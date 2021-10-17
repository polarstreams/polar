package interbroker

import (
	. "github.com/google/uuid"
	. "github.com/jorgebay/soda/internal/types"
)

// GenerationProposeMessage the interbroker api json message for
// proposing a generation to another broker.
type GenerationProposeMessage struct {
	Generation *Generation `json:"generation"`
	ExpectedTx *UUID       `json:"tx,omitempty"`
}
