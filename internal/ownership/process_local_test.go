package ownership

import (
	"fmt"
	"testing"

	"github.com/barcostreams/barco/internal/interbroker"
	. "github.com/barcostreams/barco/internal/test/discovery/mocks"
	. "github.com/barcostreams/barco/internal/test/interbroker/mocks"
	. "github.com/barcostreams/barco/internal/test/localdb/mocks"
	. "github.com/barcostreams/barco/internal/types"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/mock"
)

func TestSuite(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Ownership Suite")
}

var _ = Describe("generator", func() {
	Describe("determineStartReason()", func() {
		It("should mark as scaling up when covered by n-1", func() {
			log.Info().Msgf("Starting second test")
			dbMock := new(Client)
			dbMock.On("DbWasNewlyCreated").Return(true)
			currentOrdinal := 1
			previousOrdinal := 3 // 0, 3, 1, ...

			topology := newTestTopology(6, currentOrdinal)
			discovererMock := new(Discoverer)
			discovererMock.On("Topology").Return(&topology)

			gossiperMock := new(Gossiper)
			gossiperMock.On("IsTokenRangeCovered", previousOrdinal, topology.MyToken()).Return(true, nil)

			o := &generator{
				discoverer: discovererMock,
				gossiper:   gossiperMock,
				localDb:    dbMock,
			}

			Expect(o.determineStartReason()).To(Equal(scalingUp))
		})

		It("should panic on gossiper error", func() {
			log.Info().Msgf("Starting third test")
			dbMock := new(Client)
			dbMock.On("DbWasNewlyCreated").Return(true)

			topology := newTestTopology(6, 1)
			discovererMock := new(Discoverer)
			discovererMock.On("Topology").Return(&topology)

			gossiperMock := new(Gossiper)
			gossiperMock.
				On("IsTokenRangeCovered", mock.Anything, topology.MyToken()).
				Return(false, fmt.Errorf("Test error"))

			o := &generator{
				discoverer: discovererMock,
				gossiper:   gossiperMock,
				localDb:    dbMock,
			}

			Expect(o).NotTo(BeNil())

			Expect(func() {
				o.determineStartReason()
			}).To(Panic())
		})
	})

	Describe("processLocal()", func() {
		It("should create a version 1 for empty metadata", func() {
			topology := newTestTopology(6, 3)
			discovererMock := new(Discoverer)
			discovererMock.On("Topology").Return(&topology)
			discovererMock.On("GenerationProposed", mock.Anything).Return(nil, nil)
			discovererMock.On("SetGenerationProposed", mock.Anything, mock.Anything, mock.Anything).Return(nil)
			discovererMock.On("SetAsCommitted", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

			gossiperMock := new(Gossiper)
			// Return nil generations
			gossiperMock.
				On("GetGenerations", mock.Anything, mock.Anything).
				Return(interbroker.GenReadResult{})
			gossiperMock.
				On("SetGenerationAsProposed", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
				Return(nil)
			gossiperMock.
				On("SetAsCommitted", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
				Return(nil)

			o := &generator{
				discoverer: discovererMock,
				gossiper:   gossiperMock,
			}

			err := o.processLocalMyToken(&localGenMessage{})
			Expect(err).NotTo(HaveOccurred())

			gossiperMock.AssertExpectations(GinkgoT())
		})
	})
})

func newTestTopology(length int, ordinal int) TopologyInfo {
	brokers := make([]BrokerInfo, length, length)
	for i := 0; i < length; i++ {
		brokers[i] = BrokerInfo{
			IsSelf:   i == ordinal,
			Ordinal:  i,
			HostName: fmt.Sprintf("test-%d", i),
		}
	}

	return NewTopology(brokers)
}
