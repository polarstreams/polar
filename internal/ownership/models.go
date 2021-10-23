package ownership

import (
	"fmt"

	. "github.com/google/uuid"
	. "github.com/jorgebay/soda/internal/types"
)

// genMessage represents an internal queued items of
// generations to process them sequentially and only 1 at a time.
type genMessage interface {
	setResult(err creationError)
}

type localGenMessage struct {
	reason startReason
	result chan creationError
}

func (m *localGenMessage) setResult(err creationError) {
	m.result <- err
}

type remoteGenProposedMessage struct {
	gen        *Generation
	expectedTx *UUID
	result     chan error
}

func (m *remoteGenProposedMessage) setResult(err creationError) {
	m.result <- err
}

type remoteGenCommittedMessage struct {
	token  Token
	tx     UUID
	origin int
	result chan error
}

func (m *remoteGenCommittedMessage) setResult(err creationError) {
	m.result <- err
}

// Represents an error when trying to create (propose+accept+commit) a generation
type creationError interface {
	error
	canBeRetried() bool
}

// newCreationError returns a retryable error
func newCreationError(format string, a ...interface{}) creationError {
	return &simpleCreationError{
		message:          fmt.Sprintf(format, a...),
		canBeRetriedFlag: true,
	}
}

// wrapIfErr returns a retryable error when err is not nil. It returns nil otherwise.
func wrapIfErr(err error) creationError {
	if err == nil {
		return nil
	}
	return &simpleCreationError{
		message:          err.Error(),
		canBeRetriedFlag: true,
	}
}

func newNonRetryableError(format string, a ...interface{}) creationError {
	return &simpleCreationError{
		message:          fmt.Sprintf(format, a...),
		canBeRetriedFlag: false,
	}
}

type simpleCreationError struct {
	message string
	canBeRetriedFlag bool
}

func (e *simpleCreationError) Error() string {
	return e.message
}

func (e *simpleCreationError) canBeRetried() bool {
	return e.canBeRetriedFlag
}