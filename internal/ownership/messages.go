package ownership

import "github.com/jorgebay/soda/internal/types"

type genMessage interface {
	setResult(err error)
}

type localGenMessage struct {
	generation types.Generation
	result     chan error
}

func (m *localGenMessage) setResult(err error) {
	m.result <- err
}

type remoteGenMessage struct {
	existing *types.Generation
	new      *types.Generation
	result   chan error
}

func (m *remoteGenMessage) setResult(err error) {
	m.result <- err
}
