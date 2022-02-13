//go:build integration
// +build integration

package integration_test

import (
	"net/http"
	"testing"
	"time"

	. "github.com/barcostreams/barco/internal/test/integration"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/rs/zerolog/log"
)

func TestData(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Integration test suite")
}

var _ = Describe("A 3 node cluster", func() {
	// Note that on macos you need to manually create the alias for the loopback addresses, for example
	// sudo ifconfig lo0 alias 127.0.0.2 up && sudo ifconfig lo0 alias 127.0.0.3 up

	Describe("Producing and consuming", func() {
		var b1 *TestBroker
		var b2 *TestBroker
		var b3 *TestBroker

		BeforeEach(func ()  {
			b1 = NewTestBroker(0)
			b2 = NewTestBroker(1)
			b3 = NewTestBroker(2)
		})

		AfterEach(func ()  {
			b1.Shutdown()
			b2.Shutdown()
			b3.Shutdown()
		})

		It("should work", func() {
			b1.WaitForStart()
			b2.WaitForStart()
			b3.WaitForStart()

			log.Debug().Msgf("All brokers started successfully")

			b1.WaitOutput("Setting committed version 1 with leader 0 for range")
			b2.WaitOutput("Setting committed version 1 with leader 1 for range")
			b3.WaitOutput("Setting committed version 1 with leader 2 for range")

			message := `{"hello": "world"}`

			// Test with HTTP/1
			client := NewTestClient(&TestClientOptions{HttpVersion: 1})
			resp := client.ProduceJson(0, "abc", message, "")
			expectResponseOk(resp)

			// Test with HTTP/2
			client = NewTestClient(nil)
			resp = client.ProduceJson(0, "abc", message, "")
			expectResponseOk(resp)

			// Use different partition keys
			expectResponseOk(client.ProduceJson(0, "abc", message, "123")) // B0
			expectResponseOk(client.ProduceJson(0, "abc", message, "567")) // Re-routed to B1
			expectResponseOk(client.ProduceJson(0, "abc", message, "234")) // Re-routed to B2

			client.RegisterAsConsumer(3, `{"id": "c1", "group": "g1", "topics": ["abc"]}`)
			log.Debug().Msgf("Registered as consumer")

			// Wait for the consumer to be considered
			time.Sleep(500 * time.Millisecond)

			resp = client.ConsumerPoll(0)
			// expectResponseOk(resp)

			client.Close()
		})
	})
})

func expectResponseOk(resp *http.Response) {
	defer resp.Body.Close()
	Expect(ReadBody(resp)).To(Equal("OK"))
	Expect(resp.StatusCode).To(Equal(200))
}