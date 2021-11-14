package consuming

import (
	"fmt"
	"testing"

	. "github.com/jorgebay/soda/internal/types"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func Test(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Consumer Suite")
}

var _ = Describe("consumerOrdinalLength()", func ()  {
	It("should return the expected size", func ()  {
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
			Expect(consumerOrdinalLength(item[0])).To(Equal(item[1]))
		}
	})
})

var _ = Describe("setConsumerAssignment()", func ()  {
	topics := []string{"abc", "ced"}

	It("should set the tokens for consumers with the same length", func ()  {
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

	It("should set the tokens for when consumers are more than brokers", func ()  {
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

	It("should set the tokens for when consumers are less than brokers", func ()  {
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
			Expect(c.assignedTokens[0]).To(Equal(topology.GetToken(BrokerIndex(ringIndex))))
			Expect(c.assignedTokens[1]).To(Equal(topology.GetToken(BrokerIndex(ringIndex+1))))
		}
	})

	It("should set the tokens for when there's only one consumer", func ()  {
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
})

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
			Id:             fmt.Sprintf("c%02d", i),
			Group:          "one",
		}
		consumers[c.key()] = c
		keys = append(keys, string(c.key()))
	}

	return keys, consumers
}