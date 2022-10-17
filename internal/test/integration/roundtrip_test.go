//go:build integration
// +build integration

package integration_test

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math/rand"
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

			// Use the Producer API while the server is restarting
			const totalRequests = 100
			c := make(chan httpResponseResult, totalRequests)
			go func ()  {
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

		It("should create and expose multiple segments", func ()  {
			b0.WaitForStart().WaitForVersion1()
			b1.WaitForStart().WaitForVersion1()
			b2.WaitForStart().WaitForVersion1()

			client := NewTestClient(nil)
			const totalMessages = 50
			for i := 0; i < totalMessages; i++ {
				// Generate a message each time to make it harder to compress
				message := fmt.Sprintf(`{"long_message": "%s", "id": %d}`, generateString(500 * 1024), i)
				resp := client.ProduceJson(0, "abc", message, partitionKeyT0Range)
				expectOk(resp)
			}

			client.RegisterAsConsumer(3, `{"id": "c1", "group": "g1", "topics": ["abc"]}`)
			time.Sleep(ConsumerAddDelay)

			records := make([]string, 0, totalMessages)
			expectedOffset := int64(0)
			for {
				resp := client.ConsumerPoll(0);
				if resp.StatusCode == http.StatusOK {
					consumerResponses := readConsumerResponse(resp)
					for _, c := range consumerResponses {
						Expect(c.startOffset).To(Equal(expectedOffset))
						for _, r := range c.records {
							// Store only the last characters to avoid blowing up memory in the test
							records = append(records, r.body[len(r.body) - 10:])
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

		It("should support ndjson", func ()  {
			b0.WaitForStart().WaitForVersion1()
			b1.WaitForStart().WaitForVersion1()
			b2.WaitForStart().WaitForVersion1()

			client := NewTestClient(nil)
			client.RegisterAsConsumer(3, `{"id": "c1", "group": "g1", "topics": ["abc"]}`)
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
				resp := client.ConsumerPoll(0);
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

func expectOk(resp *http.Response, description... interface{}) {
	defer resp.Body.Close()
	Expect(resp.StatusCode).To(Equal(http.StatusOK), description...)
	Expect(ReadBody(resp)).To(Equal("OK"))
}

func expectOkOrMessage(resp *http.Response, message string, description... interface{}) {
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
	body string
}

type httpResponseResult struct {
	resp *http.Response
	err error
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
