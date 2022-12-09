package ownership

import (
	"fmt"

	. "github.com/polarstreams/polar/internal/types"
	. "github.com/google/uuid"
)

// genMessage represents an internal queued items of
// generations to process them sequentially and only 1 at a time.
type genMessage interface {
	setResult(err creationError)
}

type localGenMessage struct {
	isNew    bool          // When is new, it's expected to be generation 1 of a token
	topology *TopologyInfo // Point in time topology info
	result   chan creationError
}

func (m *localGenMessage) setResult(err creationError) {
	m.result <- err
}

type localFailoverGenMessage struct {
	broker         BrokerInfo
	topology       *TopologyInfo // Point in time topology info
	isShuttingDown bool          // Notes that the failover message is created due to a goodbye message from the peer
	result         chan creationError
}

func (m *localFailoverGenMessage) setResult(err creationError) {
	m.result <- err
}

type localSplitRangeGenMessage struct {
	topology *TopologyInfo // Point in time topology info
	origin   int           // Ordinal of the broker requesting the split
	result   chan creationError
}

func (m *localSplitRangeGenMessage) setResult(err creationError) {
	m.result <- err
}

type localJoinRangeGenMessage struct {
	topology         *TopologyInfo // Point in time topology info
	previousTopology *TopologyInfo // Previous topology info
	result           chan creationError
}

func (m *localJoinRangeGenMessage) setResult(err creationError) {
	m.result <- err
}

type remoteGenProposedMessage struct {
	gen        *Generation
	gen2       *Generation
	expectedTx *UUID
	result     chan error
}

func (m *remoteGenProposedMessage) setResult(err creationError) {
	m.result <- err
}

type remoteGenCommittedMessage struct {
	token1 Token
	token2 *Token
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
	return wrapCreationError(err)
}

func wrapCreationError(err error) creationError {
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
	message          string
	canBeRetriedFlag bool
}

func (e *simpleCreationError) Error() string {
	return e.message
}

func (e *simpleCreationError) canBeRetried() bool {
	return e.canBeRetriedFlag
}
