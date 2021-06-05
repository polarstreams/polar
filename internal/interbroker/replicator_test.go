package interbroker

import (
	"sync/atomic"
	"time"

	"github.com/jorgebay/soda/internal/types"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("SendToFollowers()", func() {
	g := &gossiper{connections: atomic.Value{}}
	topic := types.TopicDataId{Name: "abc"}
	body := make([]byte, 10)
	replication := types.ReplicationInfo{
		Leader: nil,
		Followers: []types.BrokerInfo{
			{Ordinal: 1},
			{Ordinal: 2},
		},
		Token: 0,
	}

	It("should error when there's no client for ordinals", func() {
		clients := make(clientMap)
		g.connections.Store(clients)
		err := g.SendToFollowers(replication, topic, 0, body)
		Expect(err).To(MatchError("Chunk for topic abc (0) could not be sent to replicas"))
	})

	It("should error when both fail", func() {
		clients := make(clientMap)
		clients[1] = &clientInfo{dataMessages: make(chan *dataRequest)}
		clients[2] = &clientInfo{dataMessages: make(chan *dataRequest)}
		g.connections.Store(clients)
		done := make(chan error, 1)
		go func() {
			done <- g.SendToFollowers(replication, topic, 0, body)
		}()

		request := <-clients[2].dataMessages
		request.response <- &errorResponse{"test error", 0}

		request = <-clients[1].dataMessages
		request.response <- &errorResponse{"test error", 0}

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
		clients[1] = &clientInfo{dataMessages: make(chan *dataRequest)}
		clients[2] = &clientInfo{dataMessages: make(chan *dataRequest)}
		g.connections.Store(clients)
		done := make(chan error, 1)
		go func() {
			done <- g.SendToFollowers(replication, topic, 0, body)
		}()

		request := <-clients[2].dataMessages
		request.response <- &emptyResponse{op: dataResponseOp}

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
