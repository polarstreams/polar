//go:build integration
// +build integration

package integration_test

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/klauspost/compress/zstd"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/polarstreams/polar/internal/conf"
	. "github.com/polarstreams/polar/internal/test/integration"
	. "github.com/polarstreams/polar/internal/types"
	"github.com/rs/zerolog/log"
)

const consumerContentType = "application/vnd.polar.consumermessage"

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

		BeforeEach(func() {
			b0 = NewTestBroker(0)
			b1 = NewTestBroker(1)
			b2 = NewTestBroker(2)
			b3 = nil
			b4 = nil
			b5 = nil
		})

		AfterEach(func() {
			log.Debug().Msgf("Shutting down test cluster")

			brokers := []*TestBroker{b0, b1, b2}
			if b3 != nil {
				brokers = append(brokers, b3)
			}
			if b4 != nil {
				brokers = append(brokers, b4)
			}
			if b5 != nil {
				brokers = append(brokers, b5)
			}
			ShutdownInParallel(brokers...)
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

			// Test with HTTP
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
				Version:    1,
			}))

			Expect(item.records).To(HaveLen(1))
			Expect(item.records[0].timestamp.UnixMilli()).To(BeNumerically(">=", start.UnixMilli()))
			Expect(item.records[0].timestamp.UnixMilli()).To(BeNumerically("<=", time.Now().UnixMilli()))
			Expect(item.records[0].body).To(Equal(message))

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

			client := NewTestClient(nil)
			// Send messages to all brokers
			expectOk(client.ProduceJson(0, "abc", `{"hello": "world0"}`, ""))
			expectOk(client.ProduceJson(1, "abc", `{"hello": "world1_1"}`, ""))
			expectOk(client.ProduceJson(2, "abc", `{"hello": "world2_1"}`, ""))

			log.Debug().Msgf("Consuming from B2")
			client.RegisterAsConsumer(3, `{"id": "c1", "group": "g1", "topics": ["abc"], "onNewGroup": 1}`)
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

			// Use the Producer API while the server is restarting
			const totalRequests = 100
			c := make(chan httpResponseResult, totalRequests)
			go func() {
				url := client.ProducerUrl(1, "abc", partitionKeyT1Range)
				message := `{"hello": "world1_restarting"}`
				for i := 0; i < totalRequests; i++ {
					resp, err := http.Post(url, "application/json", strings.NewReader(message))
					c <- httpResponseResult{resp, err}
					if resp != nil {
						resp.Body.Close()
					}
					time.Sleep(5 * time.Millisecond)
				}
				close(c)
			}()

			b1.WaitForStart()

			// There should be a version 3 of T1
			b1.WaitOutput("Committing \\[-3074457345618259968, 3074457345618255872\\] v3 with B1 as leader")
			expectOk(client.ProduceJson(1, "abc", `{"hello": "world1_2"}`, partitionKeyT1Range))

			for httpResult := range c {
				if httpResult.err != nil {
					Expect(httpResult.err).To(MatchError(MatchRegexp("connection refused")))
				} else {
					if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusMisdirectedRequest {
						Fail(fmt.Sprintf("Expected 200 or 421 HTTP status, obtained %d", resp.StatusCode))
					}
				}
			}

			client.Close()
		})

		It("should create and expose multiple segments", func() {
			b0.WaitForStart().WaitForVersion1()
			b1.WaitForStart().WaitForVersion1()
			b2.WaitForStart().WaitForVersion1()

			client := NewTestClient(nil)
			const totalMessages = 50
			for i := 0; i < totalMessages; i++ {
				// Generate a message each time to make it harder to compress
				message := fmt.Sprintf(`{"long_message": "%s", "id": %d}`, generateString(500*1024), i)
				resp := client.ProduceJson(0, "abc", message, partitionKeyT0Range)
				expectOk(resp)
			}

			client.RegisterAsConsumer(3, `{"id": "c1", "group": "g1", "topics": ["abc"], "onNewGroup": 1}`)
			time.Sleep(ConsumerAddDelay)

			records := make([]string, 0, totalMessages)
			expectedOffset := int64(0)
			for {
				resp := client.ConsumerPoll(0)
				if resp.StatusCode == http.StatusOK {
					consumerResponses := readConsumerResponse(resp)
					for _, c := range consumerResponses {
						Expect(c.startOffset).To(Equal(expectedOffset))
						for _, r := range c.records {
							// Store only the last characters to avoid blowing up memory in the test
							records = append(records, r.body[len(r.body)-10:])
						}
						// Increase the next one
						expectedOffset += int64(len(c.records))
					}
				}
				if resp.StatusCode == http.StatusNoContent {
					break
				}
			}

			Expect(records).To(HaveLen(totalMessages))
			for n, r := range records {
				Expect(r).To(ContainSubstring(fmt.Sprintf(`"id": %d`, n)))
			}
		})

		It("should support producing in ndjson", func() {
			b0.WaitForStart().WaitForVersion1()
			b1.WaitForStart().WaitForVersion1()
			b2.WaitForStart().WaitForVersion1()

			client := NewTestClient(nil)
			client.RegisterAsConsumer(3, `{"id": "c1", "group": "g1", "topics": ["abc"], "onNewGroup": 1}`)
			const totalMessages = 9

			// Message with multiple lines
			message := `{"id": %d}
{"id": %d}

{"id": %d}`
			// Produce messages 0..6
			expectOk(client.ProduceNDJson(0, "abc", fmt.Sprintf(message, 0, 1, 2), partitionKeyT0Range))
			expectOk(client.ProduceNDJson(0, "abc", fmt.Sprintf(message, 3, 4, 5), partitionKeyT0Range))

			// Produce messages 6..9 via another broker into the same partition
			expectOk(client.ProduceNDJson(1, "abc", fmt.Sprintf(message, 6, 7, 8), partitionKeyT0Range))

			time.Sleep(SegmentFlushInterval)

			records := make([]string, 0, totalMessages)
			for {
				resp := client.ConsumerPoll(0)
				if resp.StatusCode == http.StatusOK {
					messages := readConsumerResponse(resp)
					for _, m := range messages {
						for _, r := range m.records {
							records = append(records, r.body)
						}
					}
				}
				if resp.StatusCode == http.StatusNoContent {
					break
				}
			}

			Expect(records).To(HaveLen(totalMessages))
			for n, r := range records {
				Expect(r).To(Equal(fmt.Sprintf(`{"id": %d}`, n)))
			}
		})

		It("should support consuming in JSON", func() {
			b0.WaitForStart().WaitForVersion1()
			b1.WaitForStart().WaitForVersion1()
			b2.WaitForStart().WaitForVersion1()

			client := NewTestClient(&TestClientOptions{MaxConnsPerHost: 4})
			client.RegisterAsConsumer(3,
				`{"id": "c1", "group": "g1", "topics": ["topic1", "topic2", "topic3"], "onNewGroup": 1}`)
			const totalMessages = 100

			// Message with multiple lines
			message := `{"id": %d}
{"id": %d}`

			var wg sync.WaitGroup
			// Produce messages in a way to make sure that the coalescer will group some of
			for i := 0; i < totalMessages; i += 2 {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					expectOk(client.ProduceNDJson(0, "topic1", fmt.Sprintf(message, i, i+1), partitionKeyT0Range))
					expectOk(client.ProduceNDJson(0, "topic2", fmt.Sprintf(message, i, i+1), partitionKeyT0Range))
				}(i)
			}
			wg.Wait()
			time.Sleep(SegmentFlushInterval)

			responseBodies := make([]string, 0)
			for i := 0; i < 20; i++ {
				resp := client.ConsumerPollJson(0)
				responseBodies = append(responseBodies, ReadBody(resp))
			}

			// Make sure is not grouped to validate single
			expectOk(client.ProduceNDJson(0, "topic3", fmt.Sprintf(message, 100, 101), partitionKeyT0Range))
			time.Sleep(SegmentFlushInterval * 2)
			for i := 0; i < 5; i++ {
				responseBodies = append(responseBodies, ReadBody(client.ConsumerPollJson(0)))
			}

			messagesPerTopic := map[string][]int{
				"topic1": {},
				"topic2": {},
				"topic3": {},
			}
			for _, b := range responseBodies {
				if b == "" {
					continue
				}
				jsonDecoder := json.NewDecoder(strings.NewReader(b))
				var items []map[string]interface{}
				err := jsonDecoder.Decode(&items)
				Expect(err).NotTo(HaveOccurred())
				for _, item := range items {
					messages := messagesPerTopic[fmt.Sprint(item["topic"])]
					values := item["values"].([]interface{})
					for _, v := range values {
						msg := v.(map[string]interface{})
						messages = append(messages, int(msg["id"].(float64)))
					}
					messagesPerTopic[fmt.Sprint(item["topic"])] = messages
				}
			}

			// Check only the first 25% of items as there are no guarantees it got polled in time
			expected := make([]interface{}, totalMessages/4)
			for i := range expected {
				expected[i] = i
			}

			// Validate topic1 and topic2
			for i := 1; i <= 2; i++ {
				Expect(messagesPerTopic[fmt.Sprintf("topic%d", i)]).To(ContainElements(expected...))
			}

			// Validate topic3
			Expect(messagesPerTopic["topic3"]).To(ContainElements(100, 101))
		})

		It("should support stateless consumers", func() {
			b0.WaitForStart().WaitForVersion1()
			b1.WaitForStart().WaitForVersion1()
			b2.WaitForStart().WaitForVersion1()

			pClient := NewTestClient(nil)

			const totalMessages = 12
			const topic = "topic1"
			produceOrderedJson(pClient, topic, 0, totalMessages)
			time.Sleep(SegmentFlushInterval * 2)

			client := &http.Client{
				Transport: &http.Transport{
					MaxConnsPerHost: 1,
					MaxIdleConns:    1,
				},
			}

			const group = "default"
			registerStatelessConsumer(client, "c1", group, topic, StartFromEarliest)

			// Try register on the second one: it should be a noop
			// Use legacy "consumer_id" querystring parameter
			const registerUrl = "http://127.0.0.1:9252/v1/consumer/register?consumer_id=%s&group=%s&topic=%s&onNewGroup=%s"
			req, _ := http.NewRequest(http.MethodPut, fmt.Sprintf(registerUrl, "c1", group, topic, StartFromEarliest), nil)
			resp, err := client.Do(req)
			Expect(err).NotTo(HaveOccurred())
			Expect(ReadBody(resp)).To(Equal("Already registered"))
			expectStatusOk(resp)

			client.CloseIdleConnections() // Close the connection pools

			// Poll from all the brokers
			messages := pollJsonUntil(3, client, "c1", totalMessages)
			for i := 0; i < totalMessages; i++ {
				Expect(messages).To(ContainElement(map[string]any{"id": float64(i)}))
			}

			req, _ = http.NewRequest(http.MethodPost, "http://127.0.0.1:9252/v1/consumer/goodbye?consumerId=c1", nil)
			doRequest(client, req, http.StatusOK)
		})

		It("should get topology changes and resize the ring", func() {
			b0.WaitForStart().WaitForVersion1()
			b1.WaitForStart().WaitForVersion1()
			b2.WaitForStart().WaitForVersion1()

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

		It("should support starting from earliest and latest depending on the group policy", func() {
			b0.WaitForStart().WaitForVersion1()
			b1.WaitForStart().WaitForVersion1()
			b2.WaitForStart().WaitForVersion1()

			pClient := NewTestClient(nil)

			const messagesByGroup = 12
			const topic = "topic1"
			const clusterSize = 3

			// Produce a few messages initially
			produceOrderedJson(pClient, topic, 0, messagesByGroup)
			time.Sleep(SegmentFlushInterval * 2)

			consumer1Client := &http.Client{
				Transport: &http.Transport{MaxConnsPerHost: 1, MaxIdleConns: 1},
			}
			consumer2Client := &http.Client{
				Transport: &http.Transport{MaxConnsPerHost: 1, MaxIdleConns: 1},
			}

			// Register both consumers
			registerStatelessConsumer(consumer1Client, "c1", "group1", topic, StartFromEarliest)
			registerStatelessConsumer(consumer2Client, "c2", "group2", topic, StartFromLatest)

			// Consumer1: Start from earliest
			messages := pollJsonUntil(clusterSize, consumer1Client, "c1", messagesByGroup)
			for i := 0; i < messagesByGroup; i++ {
				Expect(messages).To(ContainElement(map[string]any{"id": float64(i)}))
			}

			// Consumer2: Start from latest since subscribed
			messages = pollTimes(clusterSize, consumer2Client, "c2", 2)
			Expect(messages).To(HaveLen(0), "Consumer 2 is starting from latest, there shouldn't be any messages")

			// Produce some more messages
			produceOrderedJson(pClient, topic, messagesByGroup, messagesByGroup)
			time.Sleep(SegmentFlushInterval * 2)

			// Consumer2: Start from latest since subscribed
			messages = pollJsonUntil(clusterSize, consumer2Client, "c2", messagesByGroup)
			for i := messagesByGroup; i < messagesByGroup*2; i++ {
				Expect(messages).To(ContainElement(map[string]any{"id": float64(i)}))
			}

			// Consumer1: Continue reading
			messages = pollJsonUntil(clusterSize, consumer1Client, "c1", messagesByGroup)
			for i := messagesByGroup; i < messagesByGroup*2; i++ {
				Expect(messages).To(ContainElement(map[string]any{"id": float64(i)}))
			}
		})

		It("should reroute messages using binary protocol", func ()  {
			const topic = "binary-topic-reroute1"
			b0.WaitForStart().WaitForVersion1()
			b1.WaitForStart().WaitForVersion1()
			b2.WaitForStart().WaitForVersion1()
			consumerClient := NewTestClient(nil)
			consumerClient.RegisterAsConsumer(3,
				fmt.Sprintf(`{"id": "c_%s", "group": "g_%s", "topics": ["%s"], "onNewGroup": 1}`, topic, topic, topic))

			client := NewBinaryProducerClient(3)
			defer client.Close()

			message := `{"ordinal": %d, "message": %d}`
			// Send to B0
			client.Send(0, topic, fmt.Sprintf(message, 0, 0), partitionKeyT0Range)

			// Send to B0 a message for B1
			client.Send(0, topic, fmt.Sprintf(message, 1, 1), partitionKeyT1Range)

			// Send to B1
			client.Send(1, topic, fmt.Sprintf(message, 1, 2), partitionKeyT1Range)

			time.Sleep(SegmentFlushInterval)

			// Poll B0 to find message 0
			resp := consumerClient.ConsumerPoll(0)
			messages := readConsumerResponse(resp)
			expectFindRecord(messages, fmt.Sprintf(message, 0, 0))

			// Poll B1 several times to find message 1 and 2
			messages = readConsumerResponse(consumerClient.ConsumerPoll(1))
			messages = append(messages, readConsumerResponse(consumerClient.ConsumerPoll(1))...)
			expectFindRecord(messages, fmt.Sprintf(message, 1, 1))
			expectFindRecord(messages, fmt.Sprintf(message, 1, 2))
		})
	})
})

func pollJsonUntil(clusterSize int, client *http.Client, consumerId string, totalMessages int) []map[string]any {
	// Poll from all the brokers
	messages := []map[string]any{}
	finished := false
	for i := 0; i < 20 && !finished; i++ {
		messages = append(messages, pollTimes(clusterSize, client, consumerId, 1)...)
		if len(messages) == totalMessages {
			finished = true
			log.Debug().Msgf("Polled %d messages, finishing polling", totalMessages)
			break
		}

		client.CloseIdleConnections() // Close the connection pools
		time.Sleep(50 * time.Millisecond)
	}

	if !finished {
		log.Warn().Msgf("Poll until returning before retrieving all the messages")
	}

	return messages
}

// Polls on each broker for a given amount of times
func pollTimes(clusterSize int, client *http.Client, consumerId string, times int) []map[string]any {
	messages := []map[string]any{}
	for i := 0; i < times; i++ {
		for brokerIp := 1; brokerIp <= clusterSize; brokerIp++ {
			pollUrl := fmt.Sprintf("http://127.0.0.%d:9252/v1/consumer/poll?consumerId=%s", brokerIp, consumerId)
			req, _ := http.NewRequest(http.MethodPost, pollUrl, nil)
			req.Header.Add("Accept", "application/json")
			resp, err := client.Do(req)
			Expect(err).NotTo(HaveOccurred())
			if resp.ContentLength == 0 {
				_ = resp.Body.Close()
				continue
			}
			if resp.StatusCode != http.StatusOK {
				panic(fmt.Sprintf("Unexpected status: %s", resp.Status))
			}

			response := []ConsumerPollResponseJson{}
			err = json.NewDecoder(resp.Body).Decode(&response)
			resp.Body.Close()
			Expect(err).NotTo(HaveOccurred())
			for _, m := range response {
				messages = append(messages, m.Values...)
			}
		}
	}

	return messages
}

// Produces messages using partition keys on B0, B1 and B2.
// The messages have the shape: {"id": %d}. Total messages must be multiples of 3.
func produceOrderedJson(client *TestClient, topic string, startIndex int, totalMessages int) {
	if totalMessages%3 != 0 {
		panic("totalMessages must be multiples of 3")
	}
	const message = `{"id": %d}`

	var wg sync.WaitGroup
	// Produce messages in a way to make sure that the coalescer will group some of them
	for i := startIndex; i < startIndex+totalMessages; i += 3 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			expectOk(client.ProduceJson(0, topic, fmt.Sprintf(message, i), partitionKeyT0Range))
			expectOk(client.ProduceJson(1, topic, fmt.Sprintf(message, i+1), partitionKeyT1Range))
			expectOk(client.ProduceJson(2, topic, fmt.Sprintf(message, i+2), partitionKeyT2Range))
		}(i)
	}
	wg.Wait()
}

func registerStatelessConsumer(client *http.Client, consumerId, group, topic string, onNewGroup OffsetResetPolicy) {
	const registerUrl = "http://127.0.0.1:9252/v1/consumer/register?consumerId=%s&group=%s&topic=%s&onNewGroup=%s"
	req, _ := http.NewRequest(http.MethodPut, fmt.Sprintf(registerUrl, consumerId, group, topic, onNewGroup), nil)
	resp, err := client.Do(req)
	Expect(err).NotTo(HaveOccurred())
	expectOk(resp)
}

func expectOk(resp *http.Response, description ...interface{}) {
	defer resp.Body.Close()
	Expect(resp.StatusCode).To(Equal(http.StatusOK), description...)
	Expect(ReadBody(resp)).To(Equal("OK"))
}

func expectStatusOk(resp *http.Response) {
	defer resp.Body.Close()
	Expect(resp.StatusCode).To(Equal(http.StatusOK))
}

// Calls client.Do() and check that the response is has the provided status
func doRequest(client *http.Client, req *http.Request, status int) {
	resp, err := client.Do(req)
	Expect(err).NotTo(HaveOccurred())
	defer resp.Body.Close()
	Expect(resp.StatusCode).To(Equal(status))
}

func expectOkOrMessage(resp *http.Response, message string, description ...interface{}) {
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusMisdirectedRequest {
		Expect(ReadBody(resp)).To(MatchRegexp(message))
		return
	}
	Expect(resp.StatusCode).To(Equal(http.StatusOK), description...)
	Expect(ReadBody(resp)).To(Equal("OK"))
}

type consumerResponseItem struct {
	topic       *TopicDataId
	startOffset int64
	records     []record
}

type record struct {
	timestamp time.Time
	body      string
}

type httpResponseResult struct {
	resp *http.Response
	err  error
}

// Use pseudo random value that is hard to compress
func generateString(length int) string {
	result := ""
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for len(result) < length {
		result += fmt.Sprint(r.Int())
	}
	return result
}

func unmarshalConsumerResponseItem(r io.Reader) consumerResponseItem {
	item := consumerResponseItem{}
	item.topic = unmarshalTopicId(r)
	err := binary.Read(r, conf.Endianness, &item.startOffset)
	Expect(err).NotTo(HaveOccurred())
	payloadLength := int32(0)
	err = binary.Read(r, conf.Endianness, &payloadLength)
	Expect(err).NotTo(HaveOccurred())
	payload := make([]byte, payloadLength)
	n, err := io.ReadFull(r, payload)
	Expect(n).To(Equal(int(payloadLength)))
	Expect(err).NotTo(HaveOccurred())

	payloadReader, err := zstd.NewReader(bytes.NewReader(payload))
	Expect(err).NotTo(HaveOccurred())
	uncompressed, err := io.ReadAll(payloadReader)
	Expect(err).NotTo(HaveOccurred())
	recordsReader := bytes.NewReader(uncompressed)
	item.records = make([]record, 0)
	for recordsReader.Len() > 0 {
		item.records = append(item.records, unmarshalRecord(recordsReader))
	}

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
	Expect(r).NotTo(BeNil(), "item should be found: %s", value)
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
