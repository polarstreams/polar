package consuming

import (
	"fmt"

	cMocks "github.com/barcostreams/barco/internal/test/conf/mocks"
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
			expected := int64(123)
			topology := newTestTopology(3, 1)

			offsetState := new(tMocks.OffsetState)
			offsetState.On("ProducerOffsetLocal", &topicId).Return(expected, nil)
			gossiper := new(iMocks.Gossiper)
			gossiper.On("ReadProducerOffset", mock.Anything, mock.Anything).Return(int64(0), fmt.Errorf("Fake error"))
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

	Describe("moveOffsetToNextGeneration()", func() {
		It("should not move it when there aren't next gens", func() {
			topicId := TopicDataId{}

			topologyGetter := new(dMocks.Discoverer)
			topologyGetter.On("NextGeneration", topicId.GenId()).Return([]Generation{})

			q := groupReadQueue{topologyGetter: topologyGetter}
			result := q.moveOffsetToNextGeneration(topicId, GenId{})
			Expect(result).To(BeFalse())
		})

		It("should move offset using the same range index when it's 1-1", func() {
			parentGen := Generation{Start: 0, Version: 1}
			topicId := TopicDataId{}

			topologyGetter := new(dMocks.Discoverer)
			topologyGetter.On("NextGeneration", topicId.GenId()).Return([]Generation{{
				Start:   0,
				Version: 2,
				Parents: []GenId{parentGen.Id()},
			}})

			expectedOffset := Offset{
				Offset:  0,
				Version: 2,
				Source:  GenId{},
			}

			offsetState := new(tMocks.OffsetState)
			offsetState.
				On("Set",
					mock.Anything, topicId.Name, topicId.Token, topicId.RangeIndex, expectedOffset, OffsetCommitAll).
				Once()

			q := groupReadQueue{
				offsetState:    offsetState,
				topologyGetter: topologyGetter,
			}
			result := q.moveOffsetToNextGeneration(topicId, GenId{})
			Expect(result).To(BeTrue())
			offsetState.AssertExpectations(GinkgoT())
		})

		It("should move offset in the current token range when it's 1-2 (scaling up)", func() {
			parentGen := Generation{Start: 0, Version: 1}
			topicId := TopicDataId{
				RangeIndex: 1,
			}

			config := new(cMocks.Config)
			config.On("ConsumerRanges").Return(4)

			topologyGetter := new(dMocks.Discoverer)
			topologyGetter.On("NextGeneration", topicId.GenId()).Return([]Generation{
				{
					Start:   0,
					Version: 2,
					Parents: []GenId{parentGen.Id()},
				},
				{
					Start:   1000,
					Version: 1,
					Parents: []GenId{parentGen.Id()},
				},
			})

			expectedOffset := Offset{
				Offset:  0,
				Version: 2,
				Source:  GenId{},
			}

			offsetState := new(tMocks.OffsetState)

			// B0 Range 1 -> B0 Range 2 & 3
			offsetState.
				On("Set", mock.Anything, topicId.Name, topicId.Token, RangeIndex(2), expectedOffset, OffsetCommitAll).
				Once()
			offsetState.
				On("Set", mock.Anything, topicId.Name, topicId.Token, RangeIndex(3), expectedOffset, OffsetCommitAll).
				Once()

			q := groupReadQueue{
				config:         config,
				offsetState:    offsetState,
				topologyGetter: topologyGetter,
			}
			result := q.moveOffsetToNextGeneration(topicId, GenId{})
			Expect(result).To(BeTrue())
			offsetState.AssertExpectations(GinkgoT())
		})

		It("should move offset in the next token range when it's 1-2 (scaling up)", func() {
			parentGen := Generation{Start: 0, Version: 1}
			nextGen := Generation{
				Start:   1000,
				Version: 1,
				Parents: []GenId{parentGen.Id()},
			}
			topicId := TopicDataId{
				RangeIndex: 2,
			}
			config := new(cMocks.Config)
			config.On("ConsumerRanges").Return(4)

			topologyGetter := new(dMocks.Discoverer)
			topologyGetter.On("NextGeneration", topicId.GenId()).Return([]Generation{
				{
					Start:   0,
					Version: 2,
					Parents: []GenId{parentGen.Id()},
				},
				nextGen,
			})

			expectedOffset := Offset{
				Offset:  0,
				Version: nextGen.Version,
				Source:  GenId{},
			}

			offsetState := new(tMocks.OffsetState)

			// B0 Range 2 -> B1 Range 0 & 1
			offsetState.
				On("Set", mock.Anything, topicId.Name, nextGen.Start, RangeIndex(0), expectedOffset, OffsetCommitAll).
				Once()
			offsetState.
				On("Set", mock.Anything, topicId.Name, nextGen.Start, RangeIndex(1), expectedOffset, OffsetCommitAll).
				Once()

			q := groupReadQueue{
				config:         config,
				offsetState:    offsetState,
				topologyGetter: topologyGetter,
			}
			result := q.moveOffsetToNextGeneration(topicId, GenId{})
			Expect(result).To(BeTrue())
			offsetState.AssertExpectations(GinkgoT())
		})

		It("should move offset using the different range indices when it's 2-1 (scaling down)", func() {
			parentGens := []GenId{{Start: 0, Version: 2}, {Start: 1000, Version: 1}}
			topicId := TopicDataId{
				Token:      0,
				RangeIndex: 2,
			}
			nextGen := Generation{
				Start:   topicId.Token,
				Version: 3,
				Parents: parentGens,
			}
			config := new(cMocks.Config)
			config.On("ConsumerRanges").Return(4)

			topologyGetter := new(dMocks.Discoverer)
			topologyGetter.On("NextGeneration", topicId.GenId()).Return([]Generation{nextGen})

			offsetState := new(tMocks.OffsetState)

			// First it should mark as completed
			offsetState.
				On("Set", mock.Anything, topicId.Name, nextGen.Start, RangeIndex(2), Offset{
					Offset:  OffsetCompleted,
					Version: topicId.Version,
				}, OffsetCommitAll).
				Once()

			// Then it should get the other
			offsetState.
				On("Get", mock.Anything, topicId.Name, nextGen.Start, RangeIndex(3)).
				Return(&Offset{Offset: OffsetCompleted, Version: topicId.Version})

			// B0 Range 2 & 3 -> B0 Range 1
			offsetState.
				On("Set", mock.Anything, topicId.Name, nextGen.Start, RangeIndex(1), Offset{
					Offset:  0,
					Version: nextGen.Version,
				}, OffsetCommitAll).
				Once()

			q := groupReadQueue{
				config:         config,
				offsetState:    offsetState,
				topologyGetter: topologyGetter,
			}
			result := q.moveOffsetToNextGeneration(topicId, GenId{})
			Expect(result).To(BeTrue())
			offsetState.AssertExpectations(GinkgoT())
		})

		It("should move offset using the different token and range indices when it's 2-1 (scaling down)", func() {
			parentGens := []GenId{{Start: 0, Version: 2}, {Start: 1000, Version: 1}}
			topicId := TopicDataId{
				Token:      1000,
				RangeIndex: 1,
			}
			nextGen := Generation{
				Start:   0,
				Version: 3,
				Parents: parentGens,
			}
			config := new(cMocks.Config)
			config.On("ConsumerRanges").Return(4)

			topologyGetter := new(dMocks.Discoverer)
			topologyGetter.On("NextGeneration", topicId.GenId()).Return([]Generation{nextGen})

			offsetState := new(tMocks.OffsetState)

			// First it should mark as completed
			offsetState.
				On("Set", mock.Anything, topicId.Name, topicId.Token, RangeIndex(1), Offset{
					Offset:  OffsetCompleted,
					Version: topicId.Version,
				}, OffsetCommitAll).
				Once()

			// Then it should get the other
			offsetState.
				On("Get", mock.Anything, topicId.Name, topicId.Token, RangeIndex(0)).
				Return(&Offset{Offset: OffsetCompleted, Version: topicId.Version})

			// B3 Range 0 & 1 -> B0 Range 2
			offsetState.
				On("Set", mock.Anything, topicId.Name, nextGen.Start, RangeIndex(2), Offset{
					Offset:  0,
					Version: nextGen.Version,
				}, OffsetCommitAll).
				Once()

			q := groupReadQueue{
				config:         config,
				offsetState:    offsetState,
				topologyGetter: topologyGetter,
			}
			result := q.moveOffsetToNextGeneration(topicId, GenId{})
			Expect(result).To(BeTrue())
			offsetState.AssertExpectations(GinkgoT())
		})
	})
})
