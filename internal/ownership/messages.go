package ownership

import . "github.com/jorgebay/soda/internal/types"

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

type remoteGenMessage struct {
	existing *Generation
	new      *Generation
	result   chan error
}

func (m *remoteGenMessage) setResult(err error) {
	m.result <- err
}
