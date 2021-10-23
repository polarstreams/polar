package ownership

import (
	. "github.com/google/uuid"
	. "github.com/jorgebay/soda/internal/types"
)

// genMessage represents an internal queued items of
// generations to process them sequentially and only 1 at a time.
type genMessage interface {
	setResult(err error)
}

type localGenMessage struct {
	reason startReason
	result chan error
}

func (m *localGenMessage) setResult(err error) {
	m.result <- err
}

type remoteGenProposedMessage struct {
	gen        *Generation
	expectedTx *UUID
	result     chan error
}

func (m *remoteGenProposedMessage) setResult(err error) {
	m.result <- err
}

type remoteGenCommittedMessage struct {
	token  Token
	tx     UUID
	origin int
	result chan error
}

func (m *remoteGenCommittedMessage) setResult(err error) {
	m.result <- err
}
