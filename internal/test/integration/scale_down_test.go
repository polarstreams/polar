//go:build integration
// +build integration

package integration_test

import (
	"fmt"
	"net/http"
	"time"

	. "github.com/barcostreams/barco/internal/test/integration"
	. "github.com/onsi/ginkgo"
	"github.com/rs/zerolog/log"
)

var _ = Describe("Scale down with a non-reusable cluster", func ()  {
	var b0, b1, b2, b3, b4, b5 *TestBroker

	BeforeEach(func ()  {
		b0 = nil
		b1 = nil
		b2 = nil
		b3 = nil
		b4 = nil
		b5 = nil
	})

	AfterEach(func ()  {
		log.Debug().Msgf("Shutting down test cluster")
		brokers := []*TestBroker{b0, b1, b2, b3, b4, b5}
		for _, b := range brokers {
			if b != nil {
				b.Shutdown()
			}
		}
	})

	It("should scale down", func () {
		b0 = NewTestBroker(0, &TestBrokerOptions{InitialClusterSize: 6})
		b1 = NewTestBroker(1, &TestBrokerOptions{InitialClusterSize: 6})
		b2 = NewTestBroker(2, &TestBrokerOptions{InitialClusterSize: 6})
		b3 = NewTestBroker(3, &TestBrokerOptions{InitialClusterSize: 6})
		b4 = NewTestBroker(4, &TestBrokerOptions{InitialClusterSize: 6})
		b5 = NewTestBroker(5, &TestBrokerOptions{InitialClusterSize: 6})

		b0.WaitForStart().WaitForVersion1()
		b1.WaitForStart().WaitForVersion1()
		b2.WaitForStart().WaitForVersion1()
		b3.WaitForStart().WaitForVersion1()
		b4.WaitForStart().WaitForVersion1()
		b5.WaitForStart().WaitForVersion1()

		client := NewTestClient(nil)
		client.RegisterAsConsumer(6, `{"id": "c1", "group": "g1", "topics": ["abc"]}`)

		// Produced a message in gen v1
		expectOk(client.ProduceJson(0, "abc", `{"hello": "world_before_0_0"}`, partitionKeyT0Range))
		expectOk(client.ProduceJson(3, "abc", `{"hello": "world_before_3_0"}`, ""))

		time.Sleep(1 * time.Second)
		fmt.Println("------------------Updating the topology")

		b0.UpdateTopologyFile(3)
		b1.UpdateTopologyFile(3)
		b2.UpdateTopologyFile(3)
		b3.UpdateTopologyFile(3)
		b4.UpdateTopologyFile(3)
		b5.UpdateTopologyFile(3)

		// // Check b3-b5 can still get traffic
		// for i := 1; i < 500; i++ {
		// 	for ordinal := 3; ordinal < 6; ordinal++ {
		// 		message := fmt.Sprintf(`{"hello": "world_while_%d_%d"}`, ordinal, i)
		// 		expectOkOrMessage(
		// 			client.ProduceJson(ordinal, "abc", message, ""),
		// 			"Leader for token [\\-\\d]+ could not be found",
		// 			"Producing on B%d", ordinal)
		// 	}
		// }

		const commitJoinMessage = "Committing \\[-9223372036854775808, -3074457345618259968\\] v2 with B0 as leader for joined ranges"
		b0.WaitOutput(commitJoinMessage)
		b1.WaitOutput(commitJoinMessage)
		b2.WaitOutput(commitJoinMessage)

		b0.LookForErrors(30)
		b1.LookForErrors(30)
		b2.LookForErrors(30)

		// // Check b3-b5 can still route traffic
		// for i := 0; i < 500; i++ {
		// 	for ordinal := 3; ordinal < 6; ordinal++ {
		// 		message := fmt.Sprintf(`{"hello": "world_route_%d_%d"}`, ordinal, i)
		// 		expectOkOrMessage(
		// 			client.ProduceJson(ordinal, "abc", message, ""),
		// 			"Leader for token [\\-\\d]+ could not be found",
		// 			"Producing on B%d", ordinal)
		// 	}
		// }

		fmt.Println("--Shutting down instances")

		b3.Shutdown()
		b3 = nil
		time.Sleep(200 * time.Millisecond)
		b4.Shutdown()
		b4 = nil
		time.Sleep(200 * time.Millisecond)
		b5.Shutdown()
		b5 = nil

		time.Sleep(2 * time.Second)
		b0.WaitOutput("Gossip now contains 2 clients for 2 peers")
		b1.WaitOutput("Gossip now contains 2 clients for 2 peers")
		b2.WaitOutput("Gossip now contains 2 clients for 2 peers")

		// Produce a new message in the new generation
		expectOk(client.ProduceJson(0, "abc", `{"hello": "world_after_0_0"}`, partitionKeyT0Range))

		// Produce more messages with different ranges on B0
		for i := 1; i < 8; i++ {
			expectOk(client.ProduceJson(0, "abc", fmt.Sprintf(`{"hello": "world_after_0_%d"}`, i), ""))
		}

		allMessages := make([]consumerResponseItem, 0)
		// Start polling B0 to obtain data from T0 and T3
		for i := 0; i < 8; i++ {
			fmt.Println("--Sending poll", i)
			resp := client.ConsumerPoll(0)
			if resp.StatusCode == http.StatusNoContent {
				fmt.Println("--Messages", i, "<empty>")
				resp.Body.Close()
				continue
			}
			messages := readConsumerResponse(resp)
			fmt.Println("--Messages", i, messages)
			allMessages = append(allMessages, messages...)
		}

		expectFindRecord(allMessages, `{"hello": "world_before_0_0"}`)
		expectFindRecord(allMessages, `{"hello": "world_before_3_0"}`)

		for i := 0; i < 8; i++ {
			expectFindRecord(allMessages, fmt.Sprintf(`{"hello": "world_after_0_%d"}`, i))
		}
	})
})
