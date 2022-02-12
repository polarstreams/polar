package consuming

import (
	"fmt"

	dMocks "github.com/barcostreams/barco/internal/test/discovery/mocks"
	iMocks "github.com/barcostreams/barco/internal/test/interbroker/mocks"
	tMocks "github.com/barcostreams/barco/internal/test/types/mocks"
	. "github.com/barcostreams/barco/internal/types"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/stretchr/testify/mock"
)

var _ = Describe("groupReadQueue()", func() {
	Describe("getMaxProducedOffset()", func() {
		It("should get the max produced offset from local", func() {
			gen := Generation{Followers: []int{2, 0}}
			topicId := TopicDataId{}
			expected := uint64(123)
			topology := newTestTopology(3, 1)

			offsetState := new(tMocks.OffsetState)
			offsetState.On("ProducerOffsetLocal", &topicId).Return(expected, nil)
			gossiper := new(iMocks.Gossiper)
			gossiper.On("ReadProducerOffset", mock.Anything, mock.Anything).Return(uint64(0), fmt.Errorf("Fake error"))
			topologyGetter := new(dMocks.Discoverer)
			topologyGetter.On("GenerationInfo", mock.Anything, mock.Anything).Return(&gen, nil)
			topologyGetter.On("Topology").Return(&topology)
			q := groupReadQueue{
				offsetState:    offsetState,
				topologyGetter: topologyGetter,
				gossiper:       gossiper,
			}

			obtained, err := q.getMaxProducedOffset(&topicId)
			Expect(err).NotTo(HaveOccurred())
			Expect(obtained).To(Equal(expected))
		})
	})
})
