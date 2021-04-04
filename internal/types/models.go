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

type TopologyChangeHandler func()
