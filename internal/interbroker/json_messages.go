package interbroker

import (
	. "github.com/google/uuid"
	. "github.com/jorgebay/soda/internal/types"
)

type GenerationProposeMessage struct {
	Generation Generation `json:"generation"`
	ExpectedTx *UUID      `json:"tx,omitempty"`
}
