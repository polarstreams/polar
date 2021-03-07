package types

// BrokerInfo contains information about a broker
type BrokerInfo struct {
	// Determines whether the broker refers to this instance
	IsSelf bool
}

type TopicInfo struct {
	Name string
}

type ReplicationInfo struct {
	Leader    *BrokerInfo
	Followers []BrokerInfo
	Token     *Token
}

type Token string
