package interbroker

import (
	"io"
	"net/url"

	. "github.com/barcostreams/barco/internal/types"
	. "github.com/google/uuid"
)

type GenListener interface {
	OnRemoteSetAsProposed(newGen *Generation, expectedTx *UUID) error

	OnRemoteSetAsCommitted(token Token, tx UUID, origin int) error
}

type ConsumerInfoListener interface {
	OnConsumerInfoFromPeer(ordinal int, groups []ConsumerGroup)

	OnOffsetFromPeer(kv *OffsetStoreKeyValue)
}

type ReroutingListener interface {
	OnReroutedMessage(topic string, querystring url.Values, contentLength int64, body io.ReadCloser) error
}

type HostUpDownListener interface {
	OnHostUp(broker BrokerInfo)
	OnHostDown(broker BrokerInfo)
}

type GenReadResult struct {
	Committed *Generation
	Proposed  *Generation
	Error     error
}
