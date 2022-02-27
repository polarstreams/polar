package interbroker

import (
	. "github.com/barcostreams/barco/internal/types"
	. "github.com/google/uuid"
)

// Represents the interbroker api json message for proposing and accepting a generation to another broker.
//
// It's possible to accept multiple generations as part of the same transaction.
type GenerationProposeMessage struct {
	Generation  *Generation `json:"gen"`
	Generation2 *Generation `json:"gen2"`
	ExpectedTx  *UUID       `json:"tx,omitempty"`
}

// GenerationCommitMessage the interbroker api json message for
// committing a generation to another broker.
type GenerationCommitMessage struct {
	Tx     UUID   `json:"tx"`
	Token1 Token  `json:"token1"`
	Token2 *Token `json:"token2,omitempty"`
	Origin int    `json:"origin"` // The ordinal of the originator of the transaction
}

type ConsumerGroupInfoMessage struct {
	Groups []ConsumerGroup `json:"groups"`
	Origin int             `json:"origin"` // The ordinal of the sender
}
