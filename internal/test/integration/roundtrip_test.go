//go:build integration
// +build integration

package integration_test

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/barcostreams/barco/internal/conf"
	. "github.com/barcostreams/barco/internal/test/integration"
	. "github.com/barcostreams/barco/internal/types"
	"github.com/klauspost/compress/zstd"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/rs/zerolog/log"
)

const consumerContentType = "application/vnd.barco.consumermessage"

// Precalculated partition keys that will fall under a certain range
const (
	partitionKeyT0Range = "123"
	partitionKeyT1Range = "567"
	partitionKeyT2Range = "234"
)

var _ = Describe("A 3 node cluster", func() {
	// Note that on macos you need to manually create the alias for the loopback addresses, for example
	// for i in {2..6}; do sudo ifconfig lo0 alias 127.0.0.$i up; done

	Describe("Producing and consuming", func() {
		var b0, b1, b2, b3, b4, b5 *TestBroker

		BeforeEach(func ()  {
			b0 = NewTestBroker(0)
			b1 = NewTestBroker(1)
			b2 = NewTestBroker(2)
			b3 = nil
			b4 = nil
			b5 = nil
		})

		AfterEach(func ()  {
			log.Debug().Msgf("Shutting down test cluster")
			b0.Shutdown()
			b1.Shutdown()
			b2.Shutdown()

			if b3 != nil {
				b3.Shutdown()
			}
			if b4 != nil {
				b4.Shutdown()
			}
			if b5 != nil {
				b5.Shutdown()
			}
		})

		It("should work with a healthy cluster", func() {
			start := time.Now()
			b0.WaitForStart()
			b1.WaitForStart()
			b2.WaitForStart()

			log.Debug().Msgf("All brokers started successfully")

			b0.WaitForVersion1()
			b1.WaitForVersion1()
			b2.WaitForVersion1()

			message := `{"hello": "world"}`

			// Test with HTTP/2
			client := NewTestClient(nil)
			resp := client.ProduceJson(0, "abc", message, "")
			expectOk(resp)

			// Use different partition keys
			// expectResponseOk(client.ProduceJson(0, "abc", message, partitionKeyT0Range)) // B0
			expectOk(client.ProduceJson(0, "abc", message, partitionKeyT1Range)) // Re-routed to B1
			expectOk(client.ProduceJson(0, "abc", message, partitionKeyT2Range)) // Re-routed to B2

			client.RegisterAsConsumer(3, `{"id": "c1", "group": "g1", "topics": ["abc"]}`)
			log.Debug().Msgf("Registered as consumer")

			// Wait for the consumer to be considered
			time.Sleep(500 * time.Millisecond)

			resp = client.ConsumerPoll(0)
			defer resp.Body.Close()
			Expect(resp.StatusCode).To(Equal(http.StatusOK))
			Expect(resp.Header.Get("Content-Type")).To(Equal(consumerContentType))
			var messageLength uint16
			binary.Read(resp.Body, conf.Endianness, &messageLength)
			Expect(messageLength).To(Equal(uint16(1)))
			item := unmarshalConsumerResponseItem(resp.Body)
			Expect(*item.topic).To(Equal(TopicDataId{
				Name:       "abc",
				Token:      -9223372036854775808,
				RangeIndex: 1,
				Version: 1,
			}))

			Expect(item.records).To(HaveLen(1))
			Expect(item.records[0].timestamp.UnixMilli()).To(BeNumerically(">=", start.UnixMilli()))
			Expect(item.records[0].timestamp.UnixMilli()).To(BeNumerically("<=", time.Now().UnixMilli()))
			Expect(item.records[0].body).To(Equal(message))

			// Test with HTTP/1
			expectOk(NewTestClient(&TestClientOptions{HttpVersion: 1}).ProduceJson(0, "abc", message, ""))

			client.Close()
		})

		It("should work with brokers going down", func() {
			b0.WaitForStart()
			b1.WaitForStart()
			b2.WaitForStart()

			log.Debug().Msgf("All brokers started successfully")

			b0.WaitForVersion1()
			b1.WaitForVersion1()
			b2.WaitForVersion1()

			// Test with HTTP/2
			client := NewTestClient(nil)
			// Send messages to all brokers
			expectOk(client.ProduceJson(0, "abc", `{"hello": "world0"}`, ""))
			expectOk(client.ProduceJson(1, "abc", `{"hello": "world1_1"}`, ""))
			expectOk(client.ProduceJson(2, "abc", `{"hello": "world2_1"}`, ""))

			log.Debug().Msgf("Consuming from B2")
			client.RegisterAsConsumer(3, `{"id": "c1", "group": "g1", "topics": ["abc"]}`)
			log.Debug().Msgf("Registered as consumer")

			time.Sleep(1 * time.Second)

			b1.Shutdown()

			b0.WaitOutput("Broker 127.0.0.2 considered DOWN")
			b2.WaitOutput("Broker 127.0.0.2 considered DOWN")

			b2.WaitOutput("Accepting myself as leader of T1 \\(-3074457345618259968\\) in v2")
			b2.WaitOutput("Committing \\[-3074457345618259968, 3074457345618255872\\] v2 with B2 as leader")
			b0.WaitOutput("Setting generation for token -3074457345618259968 tx .* as committed")

			time.Sleep(1 * time.Second)

			// B2 should ingest data in T1-T2 range
			expectOk(client.ProduceJson(2, "abc", `{"hello": "world1_2"}`, partitionKeyT1Range))
			time.Sleep(1 * time.Second)


			// Wait for the consumer to be considered
			time.Sleep(500 * time.Millisecond)

			// Try to find the record for T1 v2
			resp := client.ConsumerPoll(2)
			messages := readConsumerResponse(resp)
			topicId, r := findRecord(messages, `{"hello": "world1_1"}`)
			Expect(r).NotTo(BeNil())
			Expect(topicId.Token).To(Equal(GetTokenAtIndex(3, 1)), "Token should be T1")


			// Try to find the record for T1 v2
			resp = client.ConsumerPoll(2)
			messages = readConsumerResponse(resp)
			topicId, r = findRecord(messages, `{"hello": "world1_2"}`)
			Expect(r).NotTo(BeNil())
			Expect(topicId.Token).To(Equal(GetTokenAtIndex(3, 1)), "Token should be T1")

			b1.Start()
			b1.WaitForStart()

			// There should be a version 3 of T1
			b1.WaitOutput("Committing \\[-3074457345618259968, 3074457345618255872\\] v3 with B1 as leader")
			expectOk(client.ProduceJson(1, "abc", `{"hello": "world1_2"}`, partitionKeyT1Range))
			time.Sleep(1 * time.Second)

			client.Close()
		})

		It("should get topology changes and resize the ring", func () {
			b0.WaitForStart()
			b1.WaitForStart()
			b2.WaitForStart()

			b0.WaitForVersion1()
			b1.WaitForVersion1()
			b2.WaitForVersion1()

			b0.UpdateTopologyFile(6)
			b1.UpdateTopologyFile(6)
			b2.UpdateTopologyFile(6)

			b3 = NewTestBroker(3, &TestBrokerOptions{InitialClusterSize: 6})
			b4 = NewTestBroker(4, &TestBrokerOptions{InitialClusterSize: 6})
			b5 = NewTestBroker(5, &TestBrokerOptions{InitialClusterSize: 6})

			b0.WaitOutput("Topology changed from 3 to 6 brokers")
			b1.WaitOutput("Topology changed from 3 to 6 brokers")
			b2.WaitOutput("Topology changed from 3 to 6 brokers")
			b0.WaitOutput("Creating initial peer request to 127\\.0\\.0\\.6")

			const commitMultipleMessage = "Committing both \\[-9223372036854775808, -6148914691236517888\\] v2 with B0 as leader and \\[-6148914691236517888, -3074457345618259968\\] v1 with B3 as leader"
			b0.WaitOutput(commitMultipleMessage)
			b3.WaitOutput(commitMultipleMessage)
			b1.WaitOutput(commitMultipleMessage)
			b4.WaitOutput(commitMultipleMessage)

			b0.LookForErrors(10)
			b3.LookForErrors(10)
		})
	})
})

var _ = Describe("With a non-reusable cluster", func ()  {
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

	// TODO: Uncomment
	XIt("should scale down", func () {
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
		expectOk(client.ProduceJson(0, "abc", `{"hello": "world_before_0_0"}`, ""))
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
		expectOk(client.ProduceJson(0, "abc", `{"hello": "world_after_0_0"}`, ""))

		fmt.Println("--Start consuming")

		allMessages := make([]consumerResponseItem, 0)
		// Start polling B0 to obtain data from T0 and T3
		for i := 0; i < 4; i++ {
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
		expectFindRecord(allMessages, `{"hello": "world_after_0_0"}`)
	})
})

func expectOk(resp *http.Response, description... interface{}) {
	defer resp.Body.Close()
	Expect(resp.StatusCode).To(Equal(200), description...)
	Expect(ReadBody(resp)).To(Equal("OK"))
}

func expectOkOrMessage(resp *http.Response, message string, description... interface{}) {
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusMisdirectedRequest {
		Expect(ReadBody(resp)).To(MatchRegexp(message))
		return
	}
	Expect(resp.StatusCode).To(Equal(200), description...)
	Expect(ReadBody(resp)).To(Equal("OK"))
}

type consumerResponseItem struct {
	topic *TopicDataId
	records []record
}

type record struct {
	timestamp time.Time
	body string
}

func unmarshalConsumerResponseItem(r io.Reader) consumerResponseItem {
	item := consumerResponseItem{}
	item.topic = unmarshalTopicId(r)
	payloadLength := int32(0)
	binary.Read(r, conf.Endianness, &payloadLength)
	payload := make([]byte, payloadLength)
	n, err := r.Read(payload)
	Expect(err).NotTo(HaveOccurred())
	Expect(n).To(Equal(int(payloadLength)))

	payloadReader, err := zstd.NewReader(bytes.NewReader(payload))
	Expect(err).NotTo(HaveOccurred())
	uncompressed, err := io.ReadAll(payloadReader)
	Expect(err).NotTo(HaveOccurred())
	recordsReader := bytes.NewReader(uncompressed)
	item.records = make([]record, 0)
	// for recordsReader.Len()
	item.records = append(item.records, unmarshalRecord(recordsReader))

	return item
}

func readConsumerResponse(resp *http.Response) []consumerResponseItem {
	defer resp.Body.Close()
	Expect(resp.StatusCode).To(Equal(http.StatusOK))
	Expect(resp.Header.Get("Content-Type")).To(Equal(consumerContentType))
	var messageLength uint16
	binary.Read(resp.Body, conf.Endianness, &messageLength)
	result := make([]consumerResponseItem, 0)
	for i := 0; i < int(messageLength); i++ {
		item := unmarshalConsumerResponseItem(resp.Body)
		result = append(result, item)
	}
	return result
}

func findRecord(items []consumerResponseItem, value string) (*TopicDataId, *record) {
	for _, responseItem := range items {
		for _, r := range responseItem.records {
			if strings.Contains(r.body, value) {
				return responseItem.topic, &r
			}
		}
	}
	return nil, nil
}

func expectFindRecord(items []consumerResponseItem, value string) {
	_, r := findRecord(items, value)
	Expect(r).NotTo(BeNil())
}

func unmarshalTopicId(r io.Reader) *TopicDataId {
	topic := TopicDataId{}
	topicLength := uint8(0)
	err := binary.Read(r, conf.Endianness, &topic.Token)
	Expect(err).NotTo(HaveOccurred())
	err = binary.Read(r, conf.Endianness, &topic.RangeIndex)
	Expect(err).NotTo(HaveOccurred())
	err = binary.Read(r, conf.Endianness, &topic.Version)
	Expect(err).NotTo(HaveOccurred())
	err = binary.Read(r, conf.Endianness, &topicLength)
	Expect(err).NotTo(HaveOccurred())
	topicName := make([]byte, topicLength)
	_, err = r.Read(topicName)
	Expect(err).NotTo(HaveOccurred())
	topic.Name = string(topicName)

	return &topic
}

func unmarshalRecord(r io.Reader) record {
	length := uint32(0)
	timestamp := int64(0)
	result := record{}
	err := binary.Read(r, conf.Endianness, &timestamp)
	Expect(err).NotTo(HaveOccurred())
	result.timestamp = time.UnixMicro(timestamp)
	err = binary.Read(r, conf.Endianness, &length)
	body := make([]byte, length)
	n, err := r.Read(body)
	Expect(err).NotTo(HaveOccurred())
	Expect(n).To(Equal(int(length)))
	result.body = string(body)
	return result
}
