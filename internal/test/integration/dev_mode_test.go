//go:build integration
// +build integration

package integration_test

import (
	"fmt"
	"net/http"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/polarstreams/polar/internal/test/integration"
	. "github.com/polarstreams/polar/internal/types"
	"github.com/rs/zerolog/log"
)

var _ = Describe("Dev mode", func() {
	var b0 *TestBroker

	AfterEach(func() {
		log.Debug().Msgf("Shutting down dev test cluster")

		if b0 != nil {
			b0.Shutdown()
		}
	})

	It("produces and consumes", func() {
		b0 = NewTestBroker(0, &TestBrokerOptions{DevMode: true})
		b0.WaitForStart().WaitForVersion1()

		client := NewTestClient(nil)
		message := `{"hello": "world"}`
		expectOk(client.ProduceJson(0, "abc", message, ""), "should produce json")
		client.RegisterAsConsumer(1, `{"id": "c1", "group": "g1", "topics": ["abc"]}`)

		// Wait for the consumer to be considered
		time.Sleep(1 * time.Second)

		resp := client.ConsumerPoll(0)
		messages := readConsumerResponse(resp)
		expectFindRecord(messages, message)

		time.Sleep(500 * time.Millisecond)
		b0.LookForErrors(30)
	})

	It("supports restarting without cleaning the directory", func() {
		b0 = NewTestBroker(0, &TestBrokerOptions{DevMode: true})
		b0.WaitForStart().WaitForVersion1()
		b0.Shutdown()

		// Restart
		b0.Start()
		b0.WaitForStart()
		time.Sleep(50 * time.Millisecond)

		client := NewTestClient(nil)
		expectOk(client.ProduceJson(0, "abc", `{"hello": "world"}`, ""), "should produce json")
		time.Sleep(200 * time.Millisecond)
		b0.LookForErrors(30)
	})

	It("produces and consumes using REST API", func() {
		b0 = NewTestBroker(0, &TestBrokerOptions{DevMode: true})
		b0.WaitForStart().WaitForVersion1()
		pClient := NewTestClient(nil)

		const messagesByGroup = 5
		const topic = "topic1"
		const message = `{"id": %d}`

		// Produce a few messages initially
		for i := 0; i < messagesByGroup; i++ {
			expectOk(pClient.ProduceJson(0, topic, fmt.Sprintf(message, i), ""))
		}

		time.Sleep(SegmentFlushInterval * 2)

		consumerClient := &http.Client{
			Transport: &http.Transport{MaxConnsPerHost: 1, MaxIdleConns: 1},
		}

		registerStatelessConsumer(consumerClient, "c1", "group1", topic, StartFromEarliest)

		// Start from earliest
		messages := pollJsonUntil(1, consumerClient, "c1", messagesByGroup)
		for i := 0; i < messagesByGroup; i++ {
			Expect(messages).To(ContainElement(map[string]any{"id": float64(i)}))
		}

		// Produce some more messages
		for i := messagesByGroup; i < messagesByGroup*2; i++ {
			expectOk(pClient.ProduceJson(0, topic, fmt.Sprintf(message, i), ""))
		}
		time.Sleep(SegmentFlushInterval * 2)

		req, _ := http.NewRequest(http.MethodPost, "http://127.0.0.1:9252/v1/consumer/commit?consumerId=c1", nil)
		doRequest(consumerClient, req, http.StatusNoContent)

		// Consumer: Continue reading
		messages = pollJsonUntil(1, consumerClient, "c1", messagesByGroup)
		for i := messagesByGroup; i < messagesByGroup*2; i++ {
			Expect(messages).To(ContainElement(map[string]any{"id": float64(i)}))
		}

		req, _ = http.NewRequest(http.MethodPost, "http://127.0.0.1:9252/v1/consumer/goodbye?consumerId=c1", nil)
		doRequest(consumerClient, req, http.StatusOK)
	})

	It("produces using the binary protocol", func() {
		const topic = "binary-topic1"
		b0 = NewTestBroker(0, &TestBrokerOptions{DevMode: true})
		b0.WaitForStart().WaitForVersion1()

		consumerClient := NewTestClient(nil)
		consumerClient.RegisterAsConsumer(1,
			fmt.Sprintf(`{"id": "c_%s", "group": "g_%s", "topics": ["%s"], "onNewGroup": 1}`, topic, topic, topic))

		client := NewBinaryProducerClient(1)
		defer client.Close()

		message1 := `{"hello": "ABCDEF"}`
		message2 := `{"hello": "GHIJKL"}`
		client.Send(0, topic, message1, "")
		client.Send(0, topic, message2, "")

		time.Sleep(SegmentFlushInterval)

		resp := consumerClient.ConsumerPoll(0)
		messages := readConsumerResponse(resp)
		expectFindRecord(messages, message1)
		expectFindRecord(messages, message2)

		b0.LookForErrors(30)
	})
})
