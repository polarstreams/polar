package consuming

import (
	"fmt"
	"testing"
	"time"

	. "github.com/barcostreams/barco/internal/test/conf/mocks"
	"github.com/barcostreams/barco/internal/test/discovery/mocks"
	. "github.com/barcostreams/barco/internal/types"
	"github.com/google/uuid"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

const consumerRanges = 8

func Test(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Consumer Suite")
}

var _ = Describe("consumerBaseLength()", func() {
	It("should return the expected size", func() {
		values := [][]int{
			{1, 3},
			{3, 3},
			{4, 6},
			{6, 6},
			{7, 12},
			{12, 12},
			{13, 24},
			{24, 24},
			{25, 48},
		}

		for _, item := range values {
			Expect(consumerBaseLength(item[0])).To(Equal(item[1]))
		}
	})
})

var _ = Describe("ConsumerState", func() {
	Describe("Rebalance()", func() {
		const brokerLength = 6

		When("consumers of the same group have different topics", func() {
			It("should merge topics", func() {
				state := newConsumerState(brokerLength)
				id1 := addConnection(state, "a", "g1", "tA")
				id2 := addConnection(state, "b", "g1", "tB")
				id3 := addConnection(state, "c", "g1", "tA", "tC")

				state.Rebalance()
				expectedTopics := []string{"tA", "tB", "tC"}
				assertTopics(state, expectedTopics, id1, id2, id3)

				_, tokens1, _ := state.CanConsume(id1)
				Expect(tokens1).To(Equal(getTokens(brokerLength, 0, 2)))
				_, tokens2, _ := state.CanConsume(id2)
				Expect(tokens2).To(Equal(getTokens(brokerLength, 2, 2)))
				_, tokens3, _ := state.CanConsume(id3)
				Expect(tokens3).To(Equal(getTokens(brokerLength, 4, 2)))

				Expect(state.GetInfoForPeers()).To(HaveLen(1))
				Expect(state.GetInfoForPeers()[0].Name).To(Equal("g1"))
				Expect(state.GetInfoForPeers()[0].Topics).To(ConsistOf(expectedTopics))
				Expect(state.GetInfoForPeers()[0].Ids).To(ConsistOf("a", "b", "c"))
			})
		})

		When("multiple connections have the same id", func() {
			It("should merge the consumer info", func() {
				state := newConsumerState(brokerLength)
				id1a := addConnection(state, "a", "g1", "topic1")
				id1b := addConnection(state, "a", "g1", "topic1")
				id2 := addConnection(state, "b", "g1", "topic1")
				id3 := addConnection(state, "c", "g1", "topic1")

				rebalanced := state.Rebalance()
				Expect(rebalanced).To(BeTrue())
				assertTopics(state, []string{"topic1"}, id1a, id1b, id2, id3)

				_, tokens1, _ := state.CanConsume(id1a)
				Expect(tokens1).To(Equal(getTokens(brokerLength, 0, 2)))
				_, tokens2, _ := state.CanConsume(id2)
				Expect(tokens2).To(Equal(getTokens(brokerLength, 2, 2)))
				_, tokens3, _ := state.CanConsume(id3)
				Expect(tokens3).To(Equal(getTokens(brokerLength, 4, 2)))

				Expect(state.GetInfoForPeers()).To(HaveLen(1))
				Expect(state.GetInfoForPeers()[0].Name).To(Equal("g1"))
				Expect(state.GetInfoForPeers()[0].Topics).To(ConsistOf("topic1"))
				Expect(state.GetInfoForPeers()[0].Ids).To(ConsistOf("a", "b", "c"))
			})

			It("should return true/false depending when there's a change in consumer topology", func() {
				state := newConsumerState(brokerLength)
				id1a := addConnection(state, "a", "g1", "topic1")
				id1b := addConnection(state, "a", "g1", "topic1")
				id2 := addConnection(state, "b", "g1", "topic1")
				id3 := addConnection(state, "c", "g1", "topic1")

				rebalanced := state.Rebalance()
				Expect(rebalanced).To(BeTrue())
				assertTopics(state, []string{"topic1"}, id1a, id1b, id2, id3)

				_, tokens1, _ := state.CanConsume(id1a)
				Expect(tokens1).To(Equal(getTokens(brokerLength, 0, 2)))
				_, tokens2, _ := state.CanConsume(id2)
				Expect(tokens2).To(Equal(getTokens(brokerLength, 2, 2)))
				_, tokens3, _ := state.CanConsume(id3)
				Expect(tokens3).To(Equal(getTokens(brokerLength, 4, 2)))

				state.RemoveConnection(id1b)

				rebalanced = state.Rebalance()
				Expect(rebalanced).To(BeFalse()) // No change

				_, tokens1, _ = state.CanConsume(id1a)
				Expect(tokens1).To(Equal(getTokens(brokerLength, 0, 2)))

				id1b = addConnection(state, "a", "g1", "topic1")
				rebalanced = state.Rebalance()
				Expect(rebalanced).To(BeFalse()) // No change

				// Add a new consumer to group g1
				addConnection(state, "d", "g1", "topic1", "topic2")
				rebalanced = state.Rebalance()
				Expect(rebalanced).To(BeTrue()) // There's a change

				// Add a new group g2
				addConnection(state, "e", "g2", "topic1")
				rebalanced = state.Rebalance()
				Expect(rebalanced).To(BeTrue()) // There's a change
			})
		})

		When("all connections to a consumer are removed", func() {
			When("before the remove delay", func() {
				It("should still include it", func() {
					state := newConsumerState(brokerLength)
					id1 := addConnection(state, "a", "g1", "topic1")
					id2 := addConnection(state, "b", "g1", "topic1")
					id3 := addConnection(state, "c", "g1", "topic1")

					// Remove b
					state.RemoveConnection(id2)

					rebalanced := state.Rebalance()
					Expect(rebalanced).To(BeTrue())

					_, tokens1, _ := state.CanConsume(id1)
					Expect(tokens1).To(Equal(getTokens(brokerLength, 0, 2)))
					_, tokens2, _ := state.CanConsume(id2)
					// The id does not map to any tokens
					Expect(tokens2).To(HaveLen(0))
					_, tokens3, _ := state.CanConsume(id3)
					Expect(tokens3).To(Equal(getTokens(brokerLength, 4, 2)))

					Expect(state.GetInfoForPeers()).To(HaveLen(1))
					Expect(state.GetInfoForPeers()[0].Name).To(Equal("g1"))
					Expect(state.GetInfoForPeers()[0].Topics).To(ConsistOf("topic1"))
					// b should still be there
					Expect(state.GetInfoForPeers()[0].Ids).To(ConsistOf("a", "b", "c"))
				})
			})

			When("after the remove delay", func() {
				It("should not include it", func() {
					state := newConsumerState(brokerLength)
					state.removeDelay = 2 * time.Millisecond
					id1 := addConnection(state, "a", "g1", "topic1")
					id2 := addConnection(state, "b", "g1", "topic1")
					id3 := addConnection(state, "c", "g1", "topic1")

					// Remove b
					state.RemoveConnection(id2)

					time.Sleep(20 * time.Millisecond)

					state.Rebalance()

					_, tokens2, _ := state.CanConsume(id2)
					Expect(tokens2).To(HaveLen(0))

					_, tokens1, _ := state.CanConsume(id1)
					Expect(tokens1).To(HaveLen(3))
					_, tokens3, _ := state.CanConsume(id3)
					Expect(tokens3).To(HaveLen(3))

					Expect(state.GetInfoForPeers()).To(HaveLen(1))
					Expect(state.GetInfoForPeers()[0].Name).To(Equal("g1"))
					// b should be gone
					Expect(state.GetInfoForPeers()[0].Ids).To(ConsistOf("a", "c"))
					Expect(state.GetInfoForPeers()[0].Topics).To(ConsistOf("topic1"))
				})
			})
		})
	})
})

var _ = Describe("setConsumerAssignment()", func() {
	topics := []string{"abc", "ced"}

	It("should set the tokens for consumers with the same length", func() {
		topology := newTestTopology(6, 0)
		result := map[consumerKey]ConsumerInfo{}
		keys, consumers := createTestConsumers(6)

		setConsumerAssignment(result, &topology, keys, topics, consumers)

		for ordinal, k := range keys {
			c := result[consumerKey(k)]
			// Use the topology to calculate the expected token
			ringIndex := topology.GetIndex(ordinal)
			Expect(c.Topics).To(Equal(topics))
			Expect(c.assignedTokens).To(HaveLen(1))
			Expect(c.assignedTokens[0]).To(Equal(topology.GetToken(BrokerIndex(ringIndex))))
		}
	})

	It("should set the tokens for when consumers are more than brokers", func() {
		const brokerLength = 6
		topology := newTestTopology(brokerLength, 0)
		result := map[consumerKey]ConsumerInfo{}
		keys, consumers := createTestConsumers(8)

		// rearrange keys to make sure that it reorders it
		temp := keys[0]
		keys[0] = keys[1]
		keys[1] = temp

		setConsumerAssignment(result, &topology, keys, topics, consumers)

		for ordinal, k := range keys {
			c := result[consumerKey(k)]
			ringIndex := topology.GetIndex(ordinal)
			if ordinal < brokerLength {
				Expect(c.Topics).To(Equal(topics))
				Expect(c.assignedTokens).To(HaveLen(1))
				Expect(c.assignedTokens[0]).To(Equal(topology.GetToken(BrokerIndex(ringIndex))))
			} else {
				// No topics or tokens assigned
				Expect(c.Topics).To(HaveLen(0))
				Expect(c.assignedTokens).To(HaveLen(0))
			}
		}
	})

	It("should set the tokens for when consumers are less than brokers (3s)", func() {
		topology := newTestTopology(6, 0)
		result := map[consumerKey]ConsumerInfo{}
		keys, consumers := createTestConsumers(3)

		setConsumerAssignment(result, &topology, keys, topics, consumers)

		for ordinal, k := range keys {
			c := result[consumerKey(k)]
			ringIndex := topology.GetIndex(ordinal)
			Expect(c.Topics).To(Equal(topics))
			// Assign 2 tokens per consumers
			Expect(c.assignedTokens).To(HaveLen(2))
			Expect(c.assignedTokens[0]).To(Equal(topology.GetToken(ringIndex)))
			Expect(c.assignedTokens[1]).To(Equal(topology.GetToken(ringIndex + 1)))
		}
	})

	It("should set the tokens for when consumers are a lot less than brokers (3s)", func() {
		topology := newTestTopology(48, 0)
		result := map[consumerKey]ConsumerInfo{}
		keys, consumers := createTestConsumers(6)

		setConsumerAssignment(result, &topology, keys, topics, consumers)

		for ordinal, k := range keys {
			c := result[consumerKey(k)]
			ringIndex := topology.GetIndex(ordinal)
			Expect(c.Topics).To(Equal(topics))
			Expect(c.assignedTokens).To(HaveLen(8))
			for i := 0; i < 8; i++ {
				Expect(c.assignedTokens[i]).To(Equal(topology.GetToken(ringIndex + BrokerIndex(i))))
			}
		}
	})

	It("should set the tokens for when there's only one consumer", func() {
		const brokerLength = 6
		topology := newTestTopology(brokerLength, 0)
		result := map[consumerKey]ConsumerInfo{}
		keys, consumers := createTestConsumers(1)

		setConsumerAssignment(result, &topology, keys, topics, consumers)

		c := result[consumerKey(keys[0])]
		Expect(c.Topics).To(Equal(topics))
		// Assign all tokens to the consumer
		Expect(c.assignedTokens).To(HaveLen(6))
		for i := 0; i < brokerLength; i++ {
			Expect(c.assignedTokens[i]).To(Equal(topology.GetToken(BrokerIndex(i))))
		}
	})

	It("should set the tokens for when there are 2 consumers", func() {
		const brokerLength = 6
		topology := newTestTopology(brokerLength, 0)
		result := map[consumerKey]ConsumerInfo{}
		keys, consumers := createTestConsumers(2)

		setConsumerAssignment(result, &topology, keys, topics, consumers)

		for ordinal, k := range keys {
			c := result[consumerKey(k)]
			ringIndex := topology.GetIndex(ordinal)
			Expect(c.Topics).To(Equal(topics))
			Expect(c.assignedTokens).To(HaveLen(3))
			// The first two tokens are the natural tokens
			// The other one is the "remaining one"
			for i := 0; i < 2; i++ {
				Expect(c.assignedTokens[i]).To(Equal(topology.GetToken(ringIndex + BrokerIndex(i))))
			}
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

	return NewTopology(brokers)
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

func getTokens(brokerLength int, index int, n int) []TokenRanges {
	result := []TokenRanges{}

	for i := 0; i < n; i++ {
		t := GetTokenAtIndex(brokerLength, index+i)
		indices := make([]RangeIndex, 0, consumerRanges)
		for j := 0; j < consumerRanges; j++ {
			indices = append(indices, RangeIndex(j))
		}
		result = append(result, TokenRanges{Token: t, Indices: indices})
	}

	return result
}

func assertTopics(state *ConsumerState, topics []string, consumerIds ...uuid.UUID) {
	for _, id := range consumerIds {
		_, _, topics := state.CanConsume(id)
		Expect(topics).To(ConsistOf(topics))
	}
}

func addConnection(state *ConsumerState, consumerId string, group string, topics ...string) uuid.UUID {
	id := uuid.New()
	state.AddConnection(id, ConsumerInfo{
		Id:     consumerId,
		Group:  group,
		Topics: topics,
	})
	return id
}
