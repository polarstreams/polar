package interbroker

import (
	"sync/atomic"
	"time"

	confMock "github.com/barcostreams/barco/internal/test/conf/mocks"
	"github.com/barcostreams/barco/internal/test/discovery/mocks"
	. "github.com/barcostreams/barco/internal/types"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("SendToFollowers()", func() {
	config := new(confMock.Config)
	config.On("DevMode").Return(false)
	config.On("ReplicationTimeout").Return(1 * time.Second)
	config.On("ReplicationWriteTimeout").Return(500 * time.Millisecond)
	config.On("DevMode").Return(false)

	g := &gossiper{
		config:      config,
		discoverer:  new(mocks.Discoverer),
		connections: atomic.Value{},
	}
	topic := TopicDataId{Name: "abc"}
	body := make([]byte, 10)
	chunk := &fakeChunk{body}
	replication := ReplicationInfo{
		Leader: nil,
		Followers: []BrokerInfo{
			{Ordinal: 1},
			{Ordinal: 2},
		},
		Token: 0,
	}

	It("should error when there's no client for ordinals", func() {
		clients := make(clientMap)
		g.connections.Store(clients)
		err := g.SendToFollowers(replication, topic, 0, chunk)
		Expect(err).To(MatchError("Chunk for topic abc (0) could not be sent to replicas"))
	})

	It("should error when both fail", func() {
		clients := make(clientMap)
		clients[1] = &clientInfo{dataMessages: make(chan dataRequest)}
		clients[2] = &clientInfo{dataMessages: make(chan dataRequest)}
		g.connections.Store(clients)
		done := make(chan error, 1)
		go func() {
			done <- g.SendToFollowers(replication, topic, 0, chunk)
		}()

		request := <-clients[2].dataMessages
		request.SetResponse(&errorResponse{"test error", 0})

		request = <-clients[1].dataMessages
		request.SetResponse(&errorResponse{"test error", 0})

		var err error
		select {
		case <-time.After(2 * time.Second):
			Fail("Timed out")
		case err = <-done:
			//
		}

		Expect(err).To(MatchError("Received error when replicating: test error"))
	})

	It("should succeed when there's a single valid response", func() {
		clients := make(clientMap)
		clients[1] = &clientInfo{dataMessages: make(chan dataRequest)}
		clients[2] = &clientInfo{dataMessages: make(chan dataRequest)}
		g.connections.Store(clients)
		done := make(chan error, 1)
		go func() {
			done <- g.SendToFollowers(replication, topic, 0, chunk)
		}()

		request := <-clients[2].dataMessages
		request.SetResponse(&emptyResponse{op: chunkReplicationResponseOp})

		var err error
		select {
		case <-time.After(2 * time.Second):
			Fail("Timed out")
		case err = <-done:
			//
		}

		Expect(err).NotTo(HaveOccurred())
	})
})

type fakeChunk struct {
	body []byte
}

func (c *fakeChunk) DataBlock() []byte {
	return c.body
}

func (c *fakeChunk) StartOffset() int64 {
	return 0
}

func (c *fakeChunk) RecordLength() uint32 {
	return 0
}
