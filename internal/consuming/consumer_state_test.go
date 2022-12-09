package consuming

import (
	"fmt"
	"sort"
	"testing"
	"time"

	. "github.com/polarstreams/polar/internal/test/conf/mocks"
	"github.com/polarstreams/polar/internal/test/discovery/mocks"
	"github.com/polarstreams/polar/internal/test/fakes"
	. "github.com/polarstreams/polar/internal/types"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

const consumerRanges = 8

func Test(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Consumer Suite")
}

var _ = Describe("ConsumerState", func() {
	Describe("Rebalance()", func() {
		const brokerLength = 6

		When("consumers of the same group have different topics", func() {
			It("should merge topics", func() {
				state := newConsumerState(brokerLength)
				id1 := addConnection(state, "a", "g1", DefaultOffsetResetPolicy, "tA")
				id2 := addConnection(state, "b", "g1", DefaultOffsetResetPolicy, "tB")
				id3 := addConnection(state, "c", "g1", DefaultOffsetResetPolicy, "tA", "tC")

				state.Rebalance()
				expectedTopics := []string{"tA", "tB", "tC"}
				assertTopics(state, expectedTopics, id1, id2, id3)

				_, tokens1, _ := state.CanConsume(id1)
				Expect(tokens1).To(HaveLen(brokerLength))
				_, tokens2, _ := state.CanConsume(id2)
				Expect(tokens2).To(HaveLen(brokerLength))
				_, tokens3, _ := state.CanConsume(id3)
				Expect(tokens3).To(HaveLen(brokerLength))

				Expect(state.GetInfoForPeers()).To(HaveLen(1))
				Expect(state.GetInfoForPeers()[0].Name).To(Equal("g1"))
				Expect(state.GetInfoForPeers()[0].Topics).To(ConsistOf(expectedTopics))
				Expect(state.GetInfoForPeers()[0].Ids).To(ConsistOf("a", "b", "c"))
			})
		})

		When("multiple connections have the same id", func() {
			It("should merge the consumer info", func() {
				state := newConsumerState(brokerLength)
				id1a := addConnection(state, "a", "g1", StartFromEarliest, "topic1")
				id1b := addConnection(state, "a", "g1", StartFromEarliest, "topic1")
				id2 := addConnection(state, "b", "g1", StartFromEarliest, "topic1")
				id3 := addConnection(state, "c", "g1", StartFromEarliest, "topic1")

				rebalanced := state.Rebalance()
				Expect(rebalanced).To(BeTrue())
				assertTopics(state, []string{"topic1"}, id1a, id1b, id2, id3)

				_, tokens1, _ := state.CanConsume(id1a)
				Expect(tokens1).To(HaveLen(brokerLength))
				_, tokens2, _ := state.CanConsume(id2)
				Expect(tokens2).To(HaveLen(brokerLength))
				_, tokens3, _ := state.CanConsume(id3)
				Expect(tokens3).To(HaveLen(brokerLength))

				Expect(state.GetInfoForPeers()).To(HaveLen(1))
				Expect(state.GetInfoForPeers()[0].Name).To(Equal("g1"))
				Expect(state.GetInfoForPeers()[0].Topics).To(ConsistOf("topic1"))
				Expect(state.GetInfoForPeers()[0].Ids).To(ConsistOf("a", "b", "c"))
				Expect(state.GetInfoForPeers()[0].OnNewGroup).To(Equal(StartFromEarliest))
			})

			It("should return true/false depending when there's a change in consumer topology", func() {
				state := newConsumerState(brokerLength)
				id1a := addConnection(state, "a", "g1", DefaultOffsetResetPolicy, "topic1")
				id1b := addConnection(state, "a", "g1", DefaultOffsetResetPolicy, "topic1")
				id2 := addConnection(state, "b", "g1", DefaultOffsetResetPolicy, "topic1")
				id3 := addConnection(state, "c", "g1", DefaultOffsetResetPolicy, "topic1")

				rebalanced := state.Rebalance()
				Expect(rebalanced).To(BeTrue())
				assertTopics(state, []string{"topic1"}, id1a, id1b, id2, id3)

				_, tokens1, _ := state.CanConsume(id1a)
				Expect(tokens1).To(HaveLen(brokerLength))
				_, tokens2, _ := state.CanConsume(id2)
				Expect(tokens2).To(HaveLen(brokerLength))
				_, tokens3, _ := state.CanConsume(id3)
				Expect(tokens3).To(HaveLen(brokerLength))

				state.RemoveConnection(id1b)

				rebalanced = state.Rebalance()
				Expect(rebalanced).To(BeFalse()) // No change

				_, tokens1, _ = state.CanConsume(id1a)
				Expect(tokens1).To(HaveLen(brokerLength))

				id1b = addConnection(state, "a", "g1", DefaultOffsetResetPolicy, "topic1")
				rebalanced = state.Rebalance()
				Expect(rebalanced).To(BeFalse()) // No change

				// Add a new consumer to group g1
				addConnection(state, "d", "g1", DefaultOffsetResetPolicy, "topic1", "topic2")
				rebalanced = state.Rebalance()
				Expect(rebalanced).To(BeTrue()) // There's a change

				// Add a new group g2
				addConnection(state, "e", "g2", DefaultOffsetResetPolicy, "topic1")
				rebalanced = state.Rebalance()
				Expect(rebalanced).To(BeTrue()) // There's a change
				Expect(state.GetInfoForPeers()[0].OnNewGroup).To(Equal(DefaultOffsetResetPolicy))
			})
		})

		When("all connections to a consumer are removed", func() {
			When("before the remove delay", func() {
				It("should still include it", func() {
					state := newConsumerState(brokerLength)
					id1 := addConnection(state, "a", "g1", StartFromEarliest, "topic1")
					id2 := addConnection(state, "b", "g1", StartFromEarliest, "topic1")
					id3 := addConnection(state, "c", "g1", StartFromEarliest, "topic1")

					// Remove b
					state.RemoveConnection(id2)

					rebalanced := state.Rebalance()
					Expect(rebalanced).To(BeTrue())

					_, tokens1, _ := state.CanConsume(id1)
					Expect(tokens1).To(HaveLen(brokerLength))
					_, tokens2, _ := state.CanConsume(id2)
					// The id does not map to any tokens
					Expect(tokens2).To(HaveLen(0))
					_, tokens3, _ := state.CanConsume(id3)
					Expect(tokens3).To(HaveLen(brokerLength))

					Expect(state.GetInfoForPeers()).To(HaveLen(1))
					Expect(state.GetInfoForPeers()[0].Name).To(Equal("g1"))
					Expect(state.GetInfoForPeers()[0].Topics).To(ConsistOf("topic1"))
					// b should still be there
					Expect(state.GetInfoForPeers()[0].Ids).To(ConsistOf("a", "b", "c"))
					Expect(state.GetInfoForPeers()[0].OnNewGroup).To(Equal(StartFromEarliest))
				})
			})

			When("after the remove delay", func() {
				It("should not include it", func() {
					state := newConsumerState(brokerLength)
					state.removeDelay = 2 * time.Millisecond
					id1 := addConnection(state, "a", "g1", DefaultOffsetResetPolicy, "topic1")
					id2 := addConnection(state, "b", "g1", DefaultOffsetResetPolicy, "topic1")
					id3 := addConnection(state, "c", "g1", DefaultOffsetResetPolicy, "topic1")

					// Remove b
					state.RemoveConnection(id2)

					time.Sleep(20 * time.Millisecond)

					state.Rebalance()

					_, tokens2, _ := state.CanConsume(id2)
					Expect(tokens2).To(HaveLen(0))

					_, tokens1, _ := state.CanConsume(id1)
					Expect(tokens1).To(HaveLen(brokerLength))
					_, tokens3, _ := state.CanConsume(id3)
					Expect(tokens3).To(HaveLen(brokerLength))

					Expect(state.GetInfoForPeers()).To(HaveLen(1))
					Expect(state.GetInfoForPeers()[0].Name).To(Equal("g1"))
					// b should be gone
					Expect(state.GetInfoForPeers()[0].Ids).To(ConsistOf("a", "c"))
					Expect(state.GetInfoForPeers()[0].Topics).To(ConsistOf("topic1"))
					Expect(state.GetInfoForPeers()[0].OnNewGroup).To(Equal(DefaultOffsetResetPolicy))
				})
			})
		})

		It("should merge info from local and peers", func() {
			state := newConsumerState(brokerLength)
			id1 := addConnection(state, "a", "g1", StartFromLatest, "tA")
			state.peerGroups = map[int]peerGroupInfo{
				1: {
					groups: []ConsumerGroup{
						{
							Name:       "g1",
							Ids:        []string{"b"},
							Topics:     []string{"tB"},
							OnNewGroup: StartFromLatest,
						},
					},
					timestamp: time.Now(),
				},
			}

			state.Rebalance()
			expectedTopics := []string{"tA", "tB"}
			assertTopics(state, expectedTopics, id1)

			Expect(state.GetInfoForPeers()).To(HaveLen(1))
			Expect(state.GetInfoForPeers()[0].Name).To(Equal("g1"))
			Expect(state.GetInfoForPeers()[0].Topics).To(ConsistOf(expectedTopics))
			Expect(state.GetInfoForPeers()[0].Ids).To(ConsistOf("a", "b"))
		})
	})
})

var _ = Describe("setConsumerAssignment()", func() {
	topics := []string{"abc", "ced"}

	It("should set the token ranges when consumers have the same length", func() {
		const clusterSize = 3
		const rangesPerToken = 2
		topology := newTestTopology(3, 0)
		result := map[consumerKey]ConsumerInfo{}
		keys, consumers := createTestConsumers(6)

		setConsumerAssignment(result, &topology, keys, topics, consumers, rangesPerToken)

		for consumerIndex, k := range keys {
			c := result[consumerKey(k)]
			token := GetTokenAtIndex(clusterSize, consumerIndex/rangesPerToken)
			rangeIndex := RangeIndex(consumerIndex % rangesPerToken)
			Expect(c.Topics).To(Equal(topics))
			Expect(c.assignedTokens).To(HaveLen(1))
			Expect(c.assignedTokens[0]).To(Equal(TokenRanges{
				Token: token, Indices: []RangeIndex{rangeIndex}, ClusterSize: clusterSize}))
		}
	})

	It("should set the tokens for when consumers are more than brokers times ranges", func() {
		const clusterSize = 3
		const rangesPerToken = 4
		topology := newTestTopology(3, 0)
		result := map[consumerKey]ConsumerInfo{}
		keys, consumers := createTestConsumers(14)

		setConsumerAssignment(result, &topology, keys, topics, consumers, rangesPerToken)

		for consumerIndex, k := range keys {
			c := result[consumerKey(k)]
			token := GetTokenAtIndex(clusterSize, consumerIndex/rangesPerToken)
			rangeIndex := RangeIndex(consumerIndex % rangesPerToken)
			Expect(c.Topics).To(Equal(topics))
			if consumerIndex < clusterSize*rangesPerToken {
				Expect(c.assignedTokens).To(Equal([]TokenRanges{
					{Token: token, Indices: []RangeIndex{rangeIndex}, ClusterSize: clusterSize}}))
			} else {
				// The rest of the consumers are unused
				Expect(c.assignedTokens).To(HaveLen(0))
			}
		}
	})

	It("should set the tokens when consumers are less than brokers (3s)", func() {
		const clusterSize = 3
		const rangesPerToken = 2
		const totalRanges = clusterSize * rangesPerToken
		topology := newTestTopology(3, 0)
		result := map[consumerKey]ConsumerInfo{}
		keys, consumers := createTestConsumers(4)

		setConsumerAssignment(result, &topology, keys, topics, consumers, rangesPerToken)
		for consumerIndex, k := range keys {
			c := result[consumerKey(k)]
			token := GetTokenAtIndex(clusterSize, consumerIndex/rangesPerToken)
			rangeIndex := RangeIndex(consumerIndex % rangesPerToken)
			Expect(c.Topics).To(Equal(topics))
			if consumerIndex < 2 {
				lastToken := GetTokenAtIndex(clusterSize, clusterSize-1)
				Expect(c.assignedTokens).To(ConsistOf([]TokenRanges{
					{Token: token, Indices: []RangeIndex{rangeIndex}, ClusterSize: clusterSize},
					{Token: lastToken, Indices: []RangeIndex{rangeIndex}, ClusterSize: clusterSize}}))
			} else {
				Expect(c.assignedTokens).To(ConsistOf([]TokenRanges{
					{Token: token, Indices: []RangeIndex{rangeIndex}, ClusterSize: clusterSize}}))
			}
		}
	})

	It("should set the tokens for when there's only one consumer", func() {
		const brokerLength = 6
		const rangesPerToken = 4
		topology := newTestTopology(brokerLength, 0)
		result := map[consumerKey]ConsumerInfo{}
		keys, consumers := createTestConsumers(1)

		setConsumerAssignment(result, &topology, keys, topics, consumers, rangesPerToken)

		c := result[consumerKey(keys[0])]
		Expect(c.Topics).To(Equal(topics))
		// Assign all tokens to the consumer
		Expect(c.assignedTokens).To(HaveLen(6))
		sort.Slice(c.assignedTokens, func(i, j int) bool {
			return c.assignedTokens[i].Token < c.assignedTokens[j].Token
		})

		for i := 0; i < brokerLength; i++ {
			Expect(c.assignedTokens[i].Token).To(Equal(topology.GetToken(BrokerIndex(i))))
			Expect(c.assignedTokens[i].Indices).To(Equal([]RangeIndex{0, 1, 2, 3}))
		}
	})
})

func newConsumerState(brokerLength int) *ConsumerState {
	topology := newTestTopology(brokerLength, 3)
	discoverer := new(mocks.Discoverer)
	discoverer.On("Topology").Return(&topology)
	config := new(Config)
	config.On("ConsumerRanges").Return(8)

	return NewConsumerState(config, discoverer)
}

func newTestTopology(length int, ordinal int) TopologyInfo {
	brokers := make([]BrokerInfo, length, length)
	for i := 0; i < length; i++ {
		brokers[i] = BrokerInfo{
			IsSelf:   i == ordinal,
			Ordinal:  i,
			HostName: fmt.Sprintf("test-%d", i),
		}
	}

	return NewTopology(brokers, ordinal)
}

func createTestConsumers(length int) ([]string, map[consumerKey]ConsumerInfo) {
	keys := make([]string, 0)
	consumers := map[consumerKey]ConsumerInfo{}

	for i := 0; i < length; i++ {
		c := ConsumerInfo{
			Id:    fmt.Sprintf("c%02d", i),
			Group: "one",
		}
		consumers[c.key()] = c
		keys = append(keys, string(c.key()))
	}

	return keys, consumers
}

func assertTopics(state *ConsumerState, topics []string, consumerIds ...string) {
	for _, id := range consumerIds {
		_, _, topics := state.CanConsume(id)
		Expect(topics).To(ConsistOf(topics))
	}
}

func addConnection(state *ConsumerState, id string, group string, policy OffsetResetPolicy, topics ...string) string {
	tc := newTrackedConsumerHandler(&fakes.Connection{})
	tc.TrackAsConnectionBound()
	state.AddConnection(tc, ConsumerInfo{
		Id:         id,
		Group:      group,
		Topics:     topics,
		OnNewGroup: policy,
	})
	return tc.Id()
}
