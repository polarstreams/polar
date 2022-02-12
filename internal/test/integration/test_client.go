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

func (c *TestClient) ProduceJson(ordinal int, topic string, message string) *http.Response {
	url := fmt.Sprintf("http://127.0.0.%d:%d/v1/topic/%s/messages", ordinal+1, 8081, topic)
	resp, err := c.client.Post(url, "application/json", strings.NewReader(message))
	Expect(err).NotTo(HaveOccurred())
	return resp
}

func ReadBody(resp *http.Response) string {
	body, err := ioutil.ReadAll(resp.Body)
	Expect(err).NotTo(HaveOccurred())
    return string(body)
}