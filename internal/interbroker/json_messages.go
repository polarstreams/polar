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

// GenerationCommitMessage the interbroker api json message for
// committing a generation to another broker.
type GenerationCommitMessage struct {
	Tx     UUID `json:"tx"`
	Origin int  `json:"origin"` // The ordinal of the originator of the transaction
}
