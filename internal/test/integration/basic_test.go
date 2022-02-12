//go:build integration
// +build integration

package integration_test

import (
	"testing"

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
		It("should work", func() {
			b1 := NewTestBroker(0)
			b2 := NewTestBroker(1)
			b3 := NewTestBroker(2)

			b1.WaitForStart()
			b2.WaitForStart()
			b3.WaitForStart()

			log.Debug().Msgf("All brokers started successfully")

			b1.Shutdown()
			b2.Shutdown()
			b3.Shutdown()
		})
	})
})
