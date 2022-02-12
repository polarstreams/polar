//go:build integration
// +build integration

package integration_test

import (
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

			client := NewTestClient(nil)
			resp := client.ProduceJson(0, "abc", `{"hello": "world"}`)
			defer resp.Body.Close()

			time.Sleep(1 * time.Second)
			Expect(ReadBody(resp)).To(Equal("OK"))
			Expect(resp.StatusCode).To(Equal(200))

			log.Debug().Msgf("Producer response: %v", resp)
		})
	})
})
