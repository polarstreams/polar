package ownership

import (
	"fmt"

	. "github.com/jorgebay/soda/internal/test/discovery/mocks"
	. "github.com/jorgebay/soda/internal/test/interbroker/mocks"
	. "github.com/jorgebay/soda/internal/test/localdb/mocks"
	. "github.com/jorgebay/soda/internal/types"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/stretchr/testify/mock"
)

var _ = Describe("generator", func() {
	Describe("determineStartReason()", func() {
		It("should mark as restarted when data is there", func() {
			dbMock := new(Client)
			dbMock.On("DbWasNewlyCreated").Return(false)
			o := &generator{localDb: dbMock}

			Expect(o.determineStartReason()).To(Equal(restarted))
		})

		It("should mark as scaling up when covered by n-1", func() {
			dbMock := new(Client)
			dbMock.On("DbWasNewlyCreated").Return(true)
			currentOrdinal := 1
			previousOrdinal := 3 // 0, 3, 1, ...

			topology := newTestTopology(6, currentOrdinal)
			discovererMock := new(Discoverer)
			discovererMock.On("Topology").Return(topology)

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
			dbMock := new(Client)
			dbMock.On("DbWasNewlyCreated").Return(true)

			topology := newTestTopology(6, 1)
			discovererMock := new(Discoverer)
			discovererMock.On("Topology").Return(topology)

			gossiperMock := new(Gossiper)
			gossiperMock.
				On("IsTokenRangeCovered", mock.Anything, topology.MyToken()).
				Return(false, fmt.Errorf("Test error"))

			o := &generator{
				discoverer: discovererMock,
				gossiper:   gossiperMock,
				localDb:    dbMock,
			}

			Expect(func() {
				o.determineStartReason()
			}).To(Panic())
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
