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
	client *http.Client
}

type TestClientOptions struct {
	HttpVersion int
}

const (
	producerPort = 8081
	consumerPort = 8082
)

// Creates a htt
func NewTestClient(options *TestClientOptions) *TestClient {
	if options == nil {
		options = &TestClientOptions{}
	}

	client := &http.Client{
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

	if options.HttpVersion == 1 {
		client = &http.Client{
			Transport: &http.Transport{
				MaxConnsPerHost: 1,
			},
		}
	}

	return &TestClient{
		client: client,
	}
}

func (c *TestClient) ProduceJson(ordinal int, topic string, message string, partitionKey string) *http.Response {
	url := c.ProducerUrl(ordinal, topic, partitionKey)
	resp, err := c.client.Post(url, "application/json", strings.NewReader(message))
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
		resp, err := c.client.Post(url, "application/json", strings.NewReader(message))
		Expect(err).NotTo(HaveOccurred())
		Expect(resp.StatusCode).To(Equal(http.StatusOK))
	}
}

func (c *TestClient) ConsumerPoll(ordinal int) *http.Response {
	url := fmt.Sprintf("http://127.0.0.%d:%d/%s", ordinal+1, consumerPort, conf.ConsumerPollUrl)
	resp, err := c.client.Post(url, "application/json", strings.NewReader(""))
	Expect(err).NotTo(HaveOccurred())
	Expect(resp.StatusCode).To(BeNumerically(">=", http.StatusOK))
	Expect(resp.StatusCode).To(BeNumerically("<", 300))
	return resp
}

func (c *TestClient) Close() {
	c.client.CloseIdleConnections()
}

func ReadBody(resp *http.Response) string {
	body, err := ioutil.ReadAll(resp.Body)
	Expect(err).NotTo(HaveOccurred())
    return string(body)
}
