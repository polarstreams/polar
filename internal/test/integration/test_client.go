//go:build integration
// +build integration

package integration

import (
	"crypto/tls"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/barcostreams/barco/internal/conf"
	. "github.com/onsi/gomega"
	"github.com/rs/zerolog/log"
	"golang.org/x/net/http2"
)

// Represents a Barco Client used for integration tests
type TestClient struct {
	consumerClient *http.Client
	producerClient *http.Client
}

type TestClientOptions struct {

}

const (
	producerPort = conf.DefaultProducerPort
	consumerPort = conf.DefaultConsumerPort
)

// Creates a htt
func NewTestClient(options *TestClientOptions) *TestClient {
	if options == nil {
		options = &TestClientOptions{}
	}

	consumerClient := &http.Client{
		Transport: &http2.Transport{
			StrictMaxConcurrentStreams: true, // Do not create additional connections
			AllowHTTP:                  true,
			DialTLS: func(network, addr string, cfg *tls.Config) (net.Conn, error) {
				// Pretend we are dialing a TLS endpoint.
				log.Debug().Msgf("Creating test client connection to %s", addr)

				return net.Dial(network, addr)
			},
			ReadIdleTimeout: 200 * time.Millisecond,
			PingTimeout:     400 * time.Millisecond,
		},
	}


	producerClient := &http.Client{
		Transport: &http.Transport{
			MaxConnsPerHost: 1,
		},
	}

	return &TestClient{
		consumerClient: consumerClient,
		producerClient: producerClient,
	}
}

func (c *TestClient) ProduceJson(ordinal int, topic string, message string, partitionKey string) *http.Response {
	return c.produce(ordinal, topic, message, partitionKey, "application/json")
}

func (c *TestClient) ProduceNDJson(ordinal int, topic string, message string, partitionKey string) *http.Response {
	return c.produce(ordinal, topic, message, partitionKey, "application/x-ndjson")
}

func (c *TestClient) produce(ordinal int, topic string, message string, partitionKey string, contentType string) *http.Response {
	url := c.ProducerUrl(ordinal, topic, partitionKey)
	resp, err := c.producerClient.Post(url, contentType, strings.NewReader(message))
	Expect(err).NotTo(HaveOccurred())
	return resp
}

func (c *TestClient) ProducerUrl(ordinal int, topic string, partitionKey string) string {
	querystring := ""
	if partitionKey != "" {
		querystring = fmt.Sprintf("?partitionKey=%s", partitionKey)
	}
	return fmt.Sprintf("http://127.0.0.%d:%d/v1/topic/%s/messages%s", ordinal+1, producerPort, topic, querystring)
}

func (c *TestClient) RegisterAsConsumer(clusterSize int, message string) {
	for i := 0; i < clusterSize; i++ {
		url := fmt.Sprintf("http://127.0.0.%d:%d/%s", i+1, consumerPort, conf.ConsumerRegisterUrl)
		resp, err := c.consumerClient.Post(url, "application/json", strings.NewReader(message))
		Expect(err).NotTo(HaveOccurred())
		Expect(resp.StatusCode).To(Equal(http.StatusOK))
	}
}

func (c *TestClient) ConsumerPoll(ordinal int) *http.Response {
	url := fmt.Sprintf("http://127.0.0.%d:%d/%s", ordinal+1, consumerPort, conf.ConsumerPollUrl)
	var resp *http.Response
	for i := 0; i < 10; i++ {
		var err error
		resp, err = c.consumerClient.Post(url, "application/json", strings.NewReader(""))
		Expect(err).NotTo(HaveOccurred())
		Expect(resp.StatusCode).To(BeNumerically(">=", http.StatusOK))
		Expect(resp.StatusCode).To(BeNumerically("<", 300))
		if resp.StatusCode == http.StatusOK {
			return resp
		}
		if resp.Header.Get("Retry-After") != "" {
			time.Sleep(500 * time.Millisecond)
		}
	}
	return resp
}

func (c *TestClient) ConsumerCommit(ordinal int) *http.Response {
	url := fmt.Sprintf("http://127.0.0.%d:%d/%s", ordinal+1, consumerPort, conf.ConsumerManualCommitUrl)
	resp, err := c.consumerClient.Post(url, "application/json", strings.NewReader(""))
	Expect(err).NotTo(HaveOccurred())
	Expect(resp.StatusCode).To(BeNumerically(">=", http.StatusOK))
	Expect(resp.StatusCode).To(BeNumerically("<", 300))
	return resp
}

func (c *TestClient) Close() {
	c.producerClient.CloseIdleConnections()
	c.consumerClient.CloseIdleConnections()
}

func ReadBody(resp *http.Response) string {
	body, err := ioutil.ReadAll(resp.Body)
	Expect(err).NotTo(HaveOccurred())
    return string(body)
}
