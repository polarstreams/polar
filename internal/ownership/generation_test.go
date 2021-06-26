package ownership

import (
	"fmt"
	"testing"
	"time"

	. "github.com/jorgebay/soda/internal/test/discovery/mocks"
	. "github.com/jorgebay/soda/internal/test/interbroker/mocks"
	. "github.com/jorgebay/soda/internal/test/localdb/mocks"
	. "github.com/jorgebay/soda/internal/types"
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
	var nilGeneration *Generation = nil
	Describe("startNew()", func() {
		It("Should create a new local generation when local and peer info is empty", func() {
			startToken := GetTokenAtIndex(3, 0)
			endToken := GetTokenAtIndex(3, 1)

			discovererMock := new(Discoverer)
			discovererMock.On("LocalInfo").Return(localInfo(0))
			discovererMock.On("Brokers").Return(brokers(0, 0, 1, 2))
			discovererMock.On("TokenByOrdinal", 0).Return(startToken)
			discovererMock.On("TokenByOrdinal", 1).Return(endToken)

			dbMock := new(Client)
			dbMock.On("GetGenerationsByToken", mock.Anything).Return([]Generation{}, nil)
			dbMock.On("UpsertGeneration", mock.Anything, mock.Anything).Return(nil)

			gossiperMock := new(Gossiper)
			gossiperMock.On("GetGenerations", mock.Anything, startToken).Return(nil, nil)
			gossiperMock.On("UpsertGeneration", mock.Anything, mock.Anything, mock.Anything).Return(nil)

			txId := []byte{0, 1, 2, 3}

			o := &generator{
				discoverer: discovererMock,
				gossiper:   gossiperMock,
				localDb:    dbMock,
				nextUuid:   func() []byte { return txId },
				items:      make(chan genMessage),
			}

			go o.process()

			err := o.startNew(100 * time.Millisecond)
			Expect(err).ToNot(HaveOccurred())

			// It should have proposed a generation
			expected := &Generation{
				Start:     startToken,
				End:       endToken,
				Version:   1,
				Leader:    0,
				Tx:        txId,
				Status:    StatusProposed,
				Followers: []int{1, 2},
			}

			log.Debug().Msgf("Expected %+v", expected)
			dbMock.AssertCalled(GinkgoT(), "UpsertGeneration", nilGeneration, expected)

			discovererMock.AssertExpectations(GinkgoT())
			dbMock.AssertExpectations(GinkgoT())
			gossiperMock.AssertExpectations(GinkgoT())
		})
	})
})

func localInfo(ordinal int) *BrokerInfo {
	return &BrokerInfo{
		IsSelf:   true,
		Ordinal:  ordinal,
		HostName: fmt.Sprintf("host-%d", ordinal),
	}
}

func brokers(self int, ordinals ...int) []BrokerInfo {
	result := make([]BrokerInfo, 0)
	for _, ordinal := range ordinals {
		result = append(result, BrokerInfo{
			IsSelf:   self == ordinal,
			Ordinal:  ordinal,
			HostName: fmt.Sprintf("host-%d", ordinal),
		})
	}

	return result
}
