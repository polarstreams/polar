package types

import "fmt"

// BrokerInfo contains information about a broker
type BrokerInfo struct {
	// IsSelf determines whether the broker refers to this instance
	IsSelf bool
	// Ordinal represents the unique identifier of the broker in a cluster
	Ordinal int
	// HostName contains the reachable host name of the broker, i.e. "broker-1"
	HostName string
}

func (b *BrokerInfo) String() string {
	return fmt.Sprintf("%s (%d)", b.HostName, b.Ordinal)
}

type TopicInfo struct {
	Name string
}

type ReplicationInfo struct {
	Leader    *BrokerInfo
	Followers []BrokerInfo
	Token     Token
}

// TopicDataId contains information to locate a certain piece of data.
//
// Specifies a topic, for a token, for a defined gen id.
type TopicDataId struct {
	Name  string
	Token Token
	GenId uint16
}

// Replicator contains logic to send data to replicas
type Replicator interface {
	// Sends a message to be stored as replica of current broker's datalog
	SendToFollowers(replicationInfo ReplicationInfo, topic TopicDataId, segmentId int64, body []byte) error
}

type Generation struct {
	Start     Token     `json:"start"`
	End       Token     `json:"end"`
	Version   int       `json:"version"`
	Leader    int       `json:"leader"`
	Followers []int     `json:"followers"`
	Tx        []byte    `json:"tx"`
	Status    GenStatus `json:"status"`
}

type GenStatus int

const (
	StatusCancelled GenStatus = iota
	StatusProposed
	StatusAccepted
)
