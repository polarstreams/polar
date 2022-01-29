package types

import (
	"fmt"
	"time"

	"github.com/google/uuid"
)

type Generation struct {
	Start     Token       `json:"start"`
	End       Token       `json:"end"`
	Version   GenVersion  `json:"version"`
	Timestamp int64       `json:"timestamp"` // In unix micros
	Leader    int         `json:"leader"`    // The ordinal of the leader
	Followers []int       `json:"followers"` // Follower ordinals
	TxLeader  int         `json:"txLeader"`  // The originator of the transaction
	Tx        uuid.UUID   `json:"tx"`
	Status    GenStatus   `json:"status"`
	ToDelete  bool        `json:"toDelete"`
	Parents   []GenParent `json:"parents"`
}

type GenParent struct {
	Start   Token      `json:"start"`
	Version GenVersion `json:"version"`
}

// Time() returns the timestamp expressed as a time.Time
func (o *Generation) Time() time.Time {
	// Timestamp is expressed in micros
	nanos := o.Timestamp * 1000
	return time.Unix(0, nanos)
}

type GenVersion uint32

func (v GenVersion) String() string {
	return fmt.Sprintf("%d", v)
}

// GenStatus determines the state (proposed, accepted, ...) of the status
type GenStatus int

var genStatusNames = [...]string{"Cancelled", "Proposed", "Accepted", "Committed"}

func (s GenStatus) String() string {
	return genStatusNames[s]
}

const (
	StatusCancelled GenStatus = iota
	StatusProposed
	StatusAccepted
	StatusCommitted
)

type TransactionStatus int

const (
	TransactionStatusCancelled TransactionStatus = iota
	TransactionStatusCommitted
)
