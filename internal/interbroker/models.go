package interbroker

import (
	"io"
	"net/url"

	. "github.com/barcostreams/barco/internal/types"
	. "github.com/google/uuid"
)

// Represents a gossip listener to generation-related messages
type GenListener interface {
	OnRemoteSetAsProposed(newGen *Generation, newGen2 *Generation, expectedTx *UUID) error

	OnRemoteSetAsCommitted(token1 Token, token2 *Token, tx UUID, origin int) error

	OnRemoteRangeSplitStart(origin int) error

	// Invoked when scaling down is detected and ranges need to be joined
	OnJoinRange(previousTopology *TopologyInfo, topology *TopologyInfo)
}

type ConsumerInfoListener interface {
	OnConsumerInfoFromPeer(ordinal int, groups []ConsumerGroup)

	OnOffsetFromPeer(kv *OffsetStoreKeyValue)

	// Invoked when a consumer should be registered as a result of a peer request
	OnRegisterFromPeer(id string, group string, topics []string) error
}

type ReroutingListener interface {
	OnReroutedMessage(
		topic string,
		querystring url.Values,
		contentLength int64,
		contentType string,
		body io.ReadCloser) error
}

type PeerStateListener interface {
	OnHostUp(broker BrokerInfo)
	OnHostDown(broker BrokerInfo)

	// Invoked as a result of a peer sending goodbye message
	OnHostShuttingDown(broker BrokerInfo)
}

type GenReadResult struct {
	Committed *Generation
	Proposed  *Generation
	Error     error
}

type clientMap map[int]*clientInfo

// Creates a shallow clone of the map
func (m clientMap) clone() clientMap {
	result := make(clientMap, len(m))
	for k, v := range m {
		result[k] = v
	}
	return result
}
