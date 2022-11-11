package consuming

import (
	"fmt"
	"time"

	cMocks "github.com/barcostreams/barco/internal/test/conf/mocks"
	dataMocks "github.com/barcostreams/barco/internal/test/data/mocks"
	dMocks "github.com/barcostreams/barco/internal/test/discovery/mocks"
	iMocks "github.com/barcostreams/barco/internal/test/interbroker/mocks"
	dbMocks "github.com/barcostreams/barco/internal/test/localdb/mocks"
	. "github.com/barcostreams/barco/internal/types"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/stretchr/testify/mock"
)

var _ = Describe("defaultOffsetState", func() {
	const consumerRanges = 4
	const group = "g1"
	const topic = "t1"
	t0 := StartToken
	t1C3 := GetTokenAtIndex(3, 1)
	t2C3 := GetTokenAtIndex(3, 2)

	key := OffsetStoreKey{Group: group, Topic: topic}
	valueC12_T0_2 := Offset{
		Token:       t0,
		Index:       2,
		Version:     3,
		ClusterSize: 12,
		Offset:      456,
	}
	valueC12_T0_3 := Offset{
		Token:       t0,
		Index:       3,
		Version:     3,
		ClusterSize: 12,
		Offset:      789,
	}
	valueC3_T0_1 := Offset{
		Token:       t0,
		Index:       1,
		Version:     1,
		ClusterSize: 3,
		Offset:      123,
	}
	valueC3_T2_3 := Offset{
		Token:       t2C3,
		Index:       3,
		Version:     1,
		ClusterSize: 3,
		Offset:      5050,
	}

	Context("With some offset ranges for Get() and GetAllWithDefaults()", func() {
		config := new(cMocks.Config)
		config.On("ConsumerRanges").Return(consumerRanges)
		startC12_T0_2, endC12_T0_2 := RangeByTokenAndClusterSize(t0, 2, consumerRanges, 12)
		startC12_T0_3, endC12_T0_3 := RangeByTokenAndClusterSize(t0, 3, consumerRanges, 12)
		startC3T0_1, endC3T0_1 := RangeByTokenAndClusterSize(t0, 1, consumerRanges, 3)
		startC3T2_3, endC3T2_3 := RangeByTokenAndClusterSize(t2C3, 3, consumerRanges, 3)
		offsetMap := map[OffsetStoreKey][]offsetRange{
			key: {
				{start: startC12_T0_2, end: endC12_T0_2, value: valueC12_T0_2},
				{start: startC12_T0_3, end: endC12_T0_3, value: valueC12_T0_3},
				{start: startC3T0_1, end: endC3T0_1, value: valueC3_T0_1},
				{start: startC3T2_3, end: endC3T2_3, value: valueC3_T2_3},
			},
		}

		s := &defaultOffsetState{
			offsetMap: offsetMap,
			config:    config,
		}

		Describe("Get()", func() {
			It("should return nil when topic and group is not found", func() {
				value, rangesMatch := s.Get(group, "t2", t0, 1, 3)
				Expect(value).To(BeNil())
				Expect(rangesMatch).To(BeFalse())
			})

			It("should return the offset when there's ranges match", func() {
				// 3-broker cluster
				value, rangesMatch := s.Get(group, topic, StartToken, 1, 3)
				Expect(value).NotTo(BeNil())
				Expect(rangesMatch).To(BeTrue())
				Expect(*value).To(Equal(valueC3_T0_1))
			})

			It("should return nil when ranges don't intersect", func() {
				value, rangesMatch := s.Get(group, "t2", StartToken, 3, 3)
				Expect(value).To(BeNil())
				Expect(rangesMatch).To(BeFalse())

				value, rangesMatch = s.Get(group, "t2", GetTokenAtIndex(6, 1), 0, 6)
				Expect(value).To(BeNil())
				Expect(rangesMatch).To(BeFalse())
			})

			It("should return nil when there are ranges with smaller end", func() {
				offsetMap := map[OffsetStoreKey][]offsetRange{
					key: {
						{start: startC12_T0_2, end: endC12_T0_2, value: valueC12_T0_2},
					},
				}

				s := &defaultOffsetState{
					offsetMap: offsetMap,
					config:    config,
				}

				value, rangesMatch := s.Get(group, topic, t2C3, 0, 3)
				Expect(value).To(BeNil())
				Expect(rangesMatch).To(BeFalse())
			})

			It("should return the offset when there's range is contained", func() {
				// 6-broker cluster
				value, rangesMatch := s.Get(group, topic, StartToken, 3, 6)
				Expect(value).NotTo(BeNil())
				Expect(rangesMatch).To(BeFalse(), "The range must not match")
				Expect(*value).To(Equal(valueC3_T0_1), "The returned value is still valueC3_T0_1")

				value, rangesMatch = s.Get(group, topic, StartToken, 2, 6)
				Expect(value).NotTo(BeNil())
				Expect(rangesMatch).To(BeFalse(), "The range must not match")
				Expect(*value).To(Equal(valueC3_T0_1), "The returned value is still valueC3_T0_1")
			})

			It("should return any of the previous offsets when the cluster is smaller than offset", func() {
				// 6-broker cluster
				value, rangesMatch := s.Get(group, topic, StartToken, 1, 6)
				Expect(value).NotTo(BeNil())
				Expect(rangesMatch).To(BeFalse(), "The range must not match")
				Expect(*value).To(Equal(valueC12_T0_3), "The returned value is one of the smaller ranges")
			})

			It("should work with the last range", func() {
				value, rangesMatch := s.Get(group, topic, t2C3, 3, 3)
				Expect(value).NotTo(BeNil())
				Expect(rangesMatch).To(BeTrue(), "The range must match")
				Expect(*value).To(Equal(valueC3_T2_3), "The returned value should be the last offset")

				value, rangesMatch = s.Get(group, topic, GetTokenAtIndex(6, 5), 3, 6)
				Expect(value).NotTo(BeNil())
				Expect(rangesMatch).To(BeFalse(), "The range must not match")
				Expect(*value).To(Equal(valueC3_T2_3), "The returned value should be the last offset")
			})
		})

		Describe("GetAllWithDefaults()", func() {
			It("should return all existing offsets for the range", func() {
				values := s.GetAllWithDefaults(group, topic, t0, 2, 12, StartFromEarliest)
				Expect(values).To(Equal([]Offset{offsetMap[key][0].value}), "Should return existing C12_T0_2")

				values = s.GetAllWithDefaults(group, topic, t0, 3, 12, StartFromEarliest)
				Expect(values).To(Equal([]Offset{offsetMap[key][1].value}), "Should return existing C12_T0_3")

				values = s.GetAllWithDefaults(group, topic, t0, 1, 6, StartFromEarliest)
				Expect(values).To(Equal([]Offset{offsetMap[key][0].value, offsetMap[key][1].value}),
					"Should return existing C12_T0_2 and C12_T0_3")
			})

			Context("With StartFromEarliest", func() {
				It("should complete existing ranges with sibling defaults", func() {
					t6C12 := GetTokenAtIndex(12, 1)
					t3C6 := GetTokenAtIndex(6, 1)

					siblingGen := &Generation{
						Start:       t0,
						End:         t6C12,
						Version:     3,
						ClusterSize: 12,
						Parents:     []GenId{{Start: t0, Version: 2}},
					}
					parentGen := &Generation{
						Start:       t0,
						End:         t6C12,
						Version:     2,
						ClusterSize: 12, // Parent gen with cluster size = 12
						Parents:     []GenId{{Start: t0, Version: 1}},
					}
					initialGen := &Generation{
						Start:       t0,
						End:         t3C6,
						Version:     1,
						ClusterSize: 6, // Initial gen with cluster size = 6
						Parents:     []GenId{},
					}

					discoverer := new(dMocks.Discoverer)
					discoverer.On("GenerationInfo", GenId{Start: t0, Version: 3}).Return(siblingGen)
					discoverer.On("GenerationInfo", GenId{Start: t0, Version: 2}).Return(parentGen)
					discoverer.On("GenerationInfo", GenId{Start: t0, Version: 1}).Return(initialGen)
					discoverer.On("ParentRanges", siblingGen, []RangeIndex{0}).Return(nil)
					s.discoverer = discoverer
					datalog := new(dataMocks.Datalog)
					datalog.On("SegmentFileList", &TopicDataId{
						Name:       topic,
						Token:      parentGen.Start,
						RangeIndex: 0,
						Version:    parentGen.Version,
					}, mock.Anything).Return([]int64{1000, 2000}, nil)
					datalog.On("SegmentFileList", &TopicDataId{
						Name:       topic,
						Token:      parentGen.Start,
						RangeIndex: 1,
						Version:    parentGen.Version,
					}, mock.Anything).Return([]int64{1010}, nil)
					s.datalog = datalog

					values := s.GetAllWithDefaults(group, topic, t0, 0, 3, StartFromEarliest)
					Expect(values).To(Equal([]Offset{
						{
							Version:     parentGen.Version,
							Token:       parentGen.Start,
							ClusterSize: parentGen.ClusterSize,
							Offset:      1000,
							Index:       0,
						}, {
							Version:     parentGen.Version,
							Token:       parentGen.Start,
							ClusterSize: parentGen.ClusterSize,
							Offset:      1010,
							Index:       1,
						},
						offsetMap[key][0].value,
						offsetMap[key][1].value,
					}), "Should return latest default based on C12_T0_2 and C12_T0_3 generations")
				})

				It("should return the first generation for ranges without offset or segments", func() {
					const index = 2
					parentGen := &Generation{
						Start:       t0,
						End:         t1C3,
						Version:     4,
						ClusterSize: 3,
					}
					latestGen := &Generation{
						Start:       t0,
						End:         t1C3,
						Version:     5,
						ClusterSize: 3,
						Parents:     []GenId{parentGen.Id()},
					}
					discoverer := new(dMocks.Discoverer)
					discoverer.On("GetTokenHistory", t0, 3).Return(latestGen, nil)
					discoverer.On("ParentRanges", latestGen, []RangeIndex{index}).Return(
						[]GenerationRanges{{Generation: parentGen, Indices: []RangeIndex{index}}})
					discoverer.On("ParentRanges", parentGen, []RangeIndex{index}).Return(nil)

					datalog := new(dataMocks.Datalog)
					datalog.On("SegmentFileList", mock.Anything, mock.Anything).Return(nil, nil)
					s.datalog = datalog
					s.discoverer = discoverer

					values := s.GetAllWithDefaults(group, topic, t0, 2, 3, StartFromEarliest)
					Expect(values).To(Equal([]Offset{{
						Version:     parentGen.Version,
						ClusterSize: 3,
						Offset:      0,
						Token:       t0,
						Index:       2,
					}}), "Should return existing a default")
				})
			})

			Context("With StartFromLatest", func() {
				It("should complete existing ranges with sibling defaults", func() {
					t6C12 := GetTokenAtIndex(12, 1)

					siblingGen := &Generation{
						Start:       t0,
						End:         t6C12,
						Version:     3,
						ClusterSize: 12,
						Parents:     []GenId{{Start: t0, Version: 2}},
					}
					nextGen := &Generation{
						Start:       siblingGen.Start,
						End:         siblingGen.End,
						Version:     4,
						Parents:     []GenId{siblingGen.Id()},
						ClusterSize: siblingGen.ClusterSize,
					}

					offsetRange0 := int64(5000)
					offsetRange1 := int64(5500)
					topology := newTestTopology(3, 0)
					discoverer := new(dMocks.Discoverer)
					discoverer.On("Topology").Return(&topology)
					discoverer.On("GenerationInfo", siblingGen.Id()).Return(siblingGen)
					discoverer.On("GenerationInfo", nextGen.Id()).Return(nextGen)
					discoverer.On("NextGeneration", siblingGen.Id()).Return([]Generation{*nextGen})
					discoverer.On("NextGeneration", mock.Anything).Return(nil)
					s.discoverer = discoverer
					datalog := new(dataMocks.Datalog)
					datalog.On("ReadProducerOffset", &TopicDataId{
						Name:       topic,
						Token:      nextGen.Start,
						RangeIndex: 0,
						Version:    nextGen.Version,
					}, mock.Anything).Return(offsetRange0, nil)
					datalog.On("ReadProducerOffset", &TopicDataId{
						Name:       topic,
						Token:      nextGen.Start,
						RangeIndex: 1,
						Version:    nextGen.Version,
					}, mock.Anything).Return(offsetRange1, nil)
					s.datalog = datalog

					values := s.GetAllWithDefaults(group, topic, t0, 0, 3, StartFromLatest)
					Expect(values).To(Equal([]Offset{
						{
							Version:     nextGen.Version,
							Token:       nextGen.Start,
							ClusterSize: nextGen.ClusterSize,
							Offset:      offsetRange0,
							Index:       0,
						}, {
							Version:     nextGen.Version,
							Token:       nextGen.Start,
							ClusterSize: nextGen.ClusterSize,
							Offset:      offsetRange1,
							Index:       1,
						},
						offsetMap[key][0].value,
						offsetMap[key][1].value,
					}), "Should return latest default based on C12_T0_2 and C12_T0_3 generations")
				})

				It("should return the last generation and the max produced", func() {
					// Use C3T0_2
					const index = 2
					const clusterSize = 3
					const expected = int64(6565)
					gen := &Generation{
						Start:       t0,
						End:         t1C3,
						Version:     5,
						ClusterSize: clusterSize,
					}
					topology := newTestTopology(3, 0)
					discoverer := new(dMocks.Discoverer)
					discoverer.On("Topology").Return(&topology)
					discoverer.On("GetTokenHistory", t0, clusterSize).Return(gen, nil)
					discoverer.On("GenerationInfo", gen.Id()).Return(gen)
					discoverer.On("NextGeneration", gen.Id()).Return(nil)

					datalog := new(dataMocks.Datalog)
					datalog.On("ReadProducerOffset", &TopicDataId{
						Name:       topic,
						Token:      gen.Start,
						RangeIndex: index,
						Version:    gen.Version,
					}, mock.Anything).Return(expected, nil)
					s.datalog = datalog
					s.discoverer = discoverer

					values := s.GetAllWithDefaults(group, topic, t0, index, clusterSize, StartFromLatest)
					Expect(values).To(Equal([]Offset{{
						Token:       t0,
						Version:     gen.Version,
						Index:       index,
						ClusterSize: 3,
						Offset:      expected,
					}}), "Should return max produced")
				})
			})
		})
	})

	Describe("Set()", func() {
		startC12_T0_2, endC12_T0_2 := RangeByTokenAndClusterSize(t0, 2, consumerRanges, 12)
		startC12_T0_3, endC12_T0_3 := RangeByTokenAndClusterSize(t0, 3, consumerRanges, 12)
		startC3T0_1, endC3T0_1 := RangeByTokenAndClusterSize(t0, 1, consumerRanges, 3)
		startC3T2_3, endC3T2_3 := RangeByTokenAndClusterSize(t2C3, 3, consumerRanges, 3)
		startC3T1_0, endC3T1_0 := RangeByTokenAndClusterSize(t1C3, 0, consumerRanges, 3)

		offsetMap := map[OffsetStoreKey][]offsetRange{
			key: {
				{start: startC12_T0_2, end: endC12_T0_2, value: valueC12_T0_2},
				{start: startC12_T0_3, end: endC12_T0_3, value: valueC12_T0_3},
				{start: startC3T0_1, end: endC3T0_1, value: valueC3_T0_1},
			},
		}

		It("should insert a range at the end and the middle", func() {
			s := newTestOffsetState(offsetMap, consumerRanges)
			offsetEnd := Offset{
				Version:     2,
				ClusterSize: 3,
				Offset:      56789,
				Token:       t2C3,
				Index:       3,
			}
			result := s.Set(group, topic, offsetEnd, OffsetCommitNone)

			Expect(result).To(BeTrue())
			Expect(s.offsetMap[key]).To(HaveLen(4))
			Expect(s.offsetMap[key][3]).To(Equal(offsetRange{start: startC3T2_3, end: endC3T2_3, value: offsetEnd}))

			offsetMiddle := Offset{
				Version:     2,
				ClusterSize: 3,
				Offset:      7090,
				Token:       t1C3,
				Index:       0,
			}
			result = s.Set(group, topic, offsetMiddle, OffsetCommitNone)

			Expect(result).To(BeTrue())
			Expect(s.offsetMap[key]).To(HaveLen(5))
			Expect(s.offsetMap[key][3]).To(Equal(offsetRange{start: startC3T1_0, end: endC3T1_0, value: offsetMiddle}))
			Expect(s.offsetMap[key][4]).To(Equal(offsetRange{start: startC3T2_3, end: endC3T2_3, value: offsetEnd}))
		})

		It("should insert a range at the beginning", func() {
			// Empty initial map
			s := newTestOffsetState(nil, consumerRanges)

			offset := Offset{
				Version:     2,
				ClusterSize: 3,
				Offset:      7090,
				Token:       t2C3,
				Index:       3,
			}
			result := s.Set(group, topic, offset, OffsetCommitNone)
			Expect(result).To(BeTrue())
			Expect(s.offsetMap[key]).To(HaveLen(1))
			Expect(s.offsetMap[key][0]).To(Equal(offsetRange{start: startC3T2_3, end: endC3T2_3, value: offset}))

			// Insert at the beginning with the range
			offset = Offset{
				Version:     2,
				ClusterSize: 3,
				Offset:      6301,
				Token:       t1C3,
				Index:       0,
			}
			result = s.Set(group, topic, offset, OffsetCommitNone)
			Expect(result).To(BeTrue())
			Expect(s.offsetMap[key]).To(HaveLen(2))
			Expect(s.offsetMap[key][0]).To(Equal(offsetRange{start: startC3T1_0, end: endC3T1_0, value: offset}))
		})

		It("should insert when offset splitting at the start", func() {
			// Split the range (startC3T0_1, endC3T0_1]
			startC6T0_2, endC6T0_2 := RangeByTokenAndClusterSize(t0, 2, consumerRanges, 6)
			startC6T0_3, endC6T0_3 := RangeByTokenAndClusterSize(t0, 3, consumerRanges, 6)
			s := newTestOffsetState(offsetMap, consumerRanges)
			offset := Offset{
				Version:     3,
				ClusterSize: 6,
				Offset:      889900,
				Token:       t0,
				Index:       2, // Second index
				Source:      NewOffsetSource(GenId{}),
			}
			existingOffset := valueC3_T0_1
			existingOffset.Offset = OffsetCompleted

			result := s.Set(group, topic, offset, OffsetCommitNone)
			Expect(result).To(BeTrue())
			Expect(s.offsetMap[key]).To(HaveLen(4))
			Expect(s.offsetMap[key][2]).To(Equal(offsetRange{start: startC6T0_2, end: endC6T0_2, value: offset}))
			Expect(s.offsetMap[key][3]).To(Equal(
				offsetRange{start: startC6T0_3, end: endC6T0_3, value: existingOffset}))
		})

		It("should insert when offset splitting at the end", func() {
			// Split the range (startC3T0_1, endC3T0_1]
			startC6T0_2, endC6T0_2 := RangeByTokenAndClusterSize(t0, 2, consumerRanges, 6)
			startC6T0_3, endC6T0_3 := RangeByTokenAndClusterSize(t0, 3, consumerRanges, 6)
			s := newTestOffsetState(offsetMap, consumerRanges)
			offset := Offset{
				Version:     3,
				ClusterSize: 6,
				Offset:      889900,
				Token:       t0,
				Index:       3, //Third index
				Source:      NewOffsetSource(GenId{}),
			}
			existingOffset := valueC3_T0_1
			existingOffset.Offset = OffsetCompleted

			result := s.Set(group, topic, offset, OffsetCommitNone)
			Expect(result).To(BeTrue())
			Expect(s.offsetMap[key]).To(HaveLen(4))
			Expect(s.offsetMap[key][2]).To(Equal(
				offsetRange{start: startC6T0_2, end: endC6T0_2, value: existingOffset}))
			Expect(s.offsetMap[key][3]).To(Equal(offsetRange{start: startC6T0_3, end: endC6T0_3, value: offset}))
		})

		It("should join entirely contained ranges", func() {
			// Replace C12_T0_2 and C12_T0_3 with C6_T0_1
			startC6T0_1, endC6T0_1 := RangeByTokenAndClusterSize(t0, 1, consumerRanges, 6)
			s := newTestOffsetState(offsetMap, consumerRanges)
			offset := Offset{
				Version:     4,
				ClusterSize: 6,
				Offset:      7090,
				Token:       t0,
				Index:       1,
				Source:      NewOffsetSource(GenId{}),
			}
			result := s.Set(group, topic, offset, OffsetCommitNone)
			Expect(result).To(BeTrue())
			Expect(s.offsetMap[key]).To(HaveLen(2))
			Expect(s.offsetMap[key][0]).To(Equal(offsetRange{start: startC6T0_1, end: endC6T0_1, value: offset}))
		})

		It("should join partially contained ranges", func() {
			// Replace C12_T0_2 and C12_T0_3 with C3_T0_0
			startC3T0_0, endC3T0_0 := RangeByTokenAndClusterSize(t0, 0, consumerRanges, 3)
			// Values C12_T0_0 and C12_T0_1 are not contained (range at the beginning empty)

			s := newTestOffsetState(offsetMap, consumerRanges)
			Expect(s.offsetMap[key]).To(HaveLen(3))
			offset := Offset{
				Version:     4,
				ClusterSize: 3,
				Offset:      7091,
				Token:       t0,
				Index:       0,
				Source:      NewOffsetSource(GenId{}),
			}
			result := s.Set(group, topic, offset, OffsetCommitNone)
			Expect(result).To(BeTrue())
			Expect(s.offsetMap[key]).To(HaveLen(2))
			Expect(s.offsetMap[key][0]).To(Equal(offsetRange{start: startC3T0_0, end: endC3T0_0, value: offset}))
			Expect(s.offsetMap[key][1]).To(Equal(offsetRange{start: startC3T0_1, end: endC3T0_1, value: valueC3_T0_1}))
		})

		It("should move to the next generation when cluster size is the same", func() {
			gen := Generation{
				Start:       t0,
				End:         t1C3,
				Version:     1,
				Parents:     []GenId{},
				ClusterSize: 3,
			}
			nextGens := []Generation{{
				Start:       t0,
				End:         t1C3,
				Version:     2,
				ClusterSize: 3,
			}}

			discoverer := new(dMocks.Discoverer)
			discoverer.On("GenerationInfo", mock.Anything, mock.Anything).Return(&gen, nil)
			discoverer.On("NextGeneration", mock.Anything).Return(nextGens)
			s := newTestOffsetState(offsetMap, consumerRanges)
			s.discoverer = discoverer
			value := valueC3_T0_1
			value.Offset = OffsetCompleted

			result := s.Set(group, topic, value, OffsetCommitNone)
			Expect(result).To(BeTrue())
			Expect(s.offsetMap[key]).To(HaveLen(3))
			Expect(s.offsetMap[key][2]).To(Equal(offsetRange{start: startC3T0_1, end: endC3T0_1, value: Offset{
				Version:     2, // Next version
				ClusterSize: 3,
				Offset:      0,
				Token:       t0,
				Index:       1,
			}}))
		})

		Context("Splitting offsets", func() {
			gen := Generation{
				Start:       t0,
				End:         t1C3,
				Version:     1,
				Parents:     []GenId{},
				ClusterSize: 3,
			}
			t3C6 := GetTokenAtIndex(6, 1)
			nextGens := []Generation{
				{Start: t0, End: t3C6, Version: 2, ClusterSize: 6},
				{Start: t3C6, End: t1C3, Version: 1, ClusterSize: 6},
			}

			discoverer := new(dMocks.Discoverer)
			discoverer.On("GenerationInfo", mock.Anything, mock.Anything).Return(&gen, nil)
			discoverer.On("NextGeneration", mock.Anything).Return(nextGens)

			It("should move to the next generation when scaling up on low indices", func() {
				s := newTestOffsetState(offsetMap, consumerRanges)
				s.discoverer = discoverer
				value := valueC3_T0_1
				value.Offset = OffsetCompleted

				startC6T0_2, endC6T0_2 := RangeByTokenAndClusterSize(t0, 2, consumerRanges, 6)
				startC6T0_3, endC6T0_3 := RangeByTokenAndClusterSize(t0, 3, consumerRanges, 6)
				result := s.Set(group, topic, value, OffsetCommitNone)
				Expect(result).To(BeTrue())
				Expect(s.offsetMap[key]).To(HaveLen(4))
				Expect(s.offsetMap[key][2]).To(Equal(offsetRange{start: startC6T0_2, end: endC6T0_2, value: Offset{
					Version:     2,
					ClusterSize: 6,
					Offset:      0,
					Token:       t0,
					Index:       2,
				}}))
				Expect(s.offsetMap[key][3]).To(Equal(offsetRange{start: startC6T0_3, end: endC6T0_3, value: Offset{
					Version:     2,
					ClusterSize: 6,
					Offset:      0,
					Token:       t0,
					Index:       3,
				}}))
			})

			It("should move to the next generation when scaling up on high indices", func() {
				s := newTestOffsetState(offsetMap, consumerRanges)
				// Add the range that is going to be split
				valueC3_T0_2 := Offset{
					Version:     1,
					ClusterSize: 3,
					Offset:      0,
					Token:       t0,
					Index:       2,
				}
				s.Set(group, topic, valueC3_T0_2, OffsetCommitNone)
				Expect(s.offsetMap[key]).To(HaveLen(4))

				s.discoverer = discoverer
				value := valueC3_T0_2
				value.Offset = OffsetCompleted
				result := s.Set(group, topic, value, OffsetCommitNone)
				Expect(result).To(BeTrue())
				Expect(s.offsetMap[key]).To(HaveLen(5))

				startC6T3_0, endC6T3_0 := RangeByTokenAndClusterSize(t3C6, 0, consumerRanges, 6)
				startC6T3_1, endC6T3_1 := RangeByTokenAndClusterSize(t3C6, 1, consumerRanges, 6)
				Expect(s.offsetMap[key][3]).To(Equal(offsetRange{start: startC6T3_0, end: endC6T3_0, value: Offset{
					Version:     1,
					ClusterSize: 6,
					Offset:      0,
					Token:       t3C6,
					Index:       0,
				}}))
				Expect(s.offsetMap[key][4]).To(Equal(offsetRange{start: startC6T3_1, end: endC6T3_1, value: Offset{
					Version:     1,
					ClusterSize: 6,
					Offset:      0,
					Token:       t3C6,
					Index:       1,
				}}))
			})
		})

		Context("Joining offsets", func() {
			t6C12 := GetTokenAtIndex(12, 1)
			t3C6 := GetTokenAtIndex(6, 1)
			gen := Generation{
				Start:       t0,
				End:         t6C12,
				Version:     3,
				Parents:     []GenId{},
				ClusterSize: 12,
			}

			nextGens := []Generation{
				{Start: t0, End: t3C6, Version: 10, ClusterSize: 6, Parents: []GenId{gen.Id(), {}}},
			}
			discoverer := new(dMocks.Discoverer)
			discoverer.On("GenerationInfo", mock.Anything, mock.Anything).Return(&gen, nil)
			discoverer.On("NextGeneration", mock.Anything).Return(nextGens)

			It("should join the offset of itself and sibling on same token", func() {
				// Join C12_T0_2 and C12_T0_3 into C6_T0_1
				s := newTestOffsetState(offsetMap, consumerRanges)
				s.discoverer = discoverer
				Expect(s.offsetMap[key]).To(HaveLen(3))

				// Complete C12_T0_2
				s.Set(group, topic, Offset{
					Version:     gen.Version,
					ClusterSize: gen.ClusterSize,
					Offset:      OffsetCompleted,
					Token:       gen.Start,
					Index:       2,
				}, OffsetCommitNone)
				Expect(s.offsetMap[key]).To(HaveLen(3), "Join didn't happened yet")

				// Complete C12_T0_3
				s.Set(group, topic, Offset{
					Version:     gen.Version,
					ClusterSize: gen.ClusterSize,
					Offset:      OffsetCompleted,
					Token:       gen.Start,
					Index:       3,
				}, OffsetCommitNone)
				Expect(s.offsetMap[key]).To(HaveLen(2), "Join should have occurred")
				Expect(s.offsetMap[key][0]).To(Equal(offsetRange{start: startC12_T0_2, end: endC12_T0_3, value: Offset{
					Token:       nextGens[0].Start,
					Version:     nextGens[0].Version,
					ClusterSize: nextGens[0].ClusterSize,
					Offset:      0,
					Index:       1, // C6_T0_1
				}}))
			})
		})
	})

	Describe("Init()", func() {
		It("should load the offsets from localdb", func() {
			localDb := new(dbMocks.Client)
			localDb.On("Offsets").Return([]OffsetStoreKeyValue{
				{Key: key, Value: valueC3_T0_1},
				{Key: key, Value: valueC3_T2_3},
			}, nil)

			s := newTestOffsetState(nil, 4)
			s.localDb = localDb

			Expect(s.Init()).NotTo(HaveOccurred())
			Expect(s.offsetMap[key]).To(HaveLen(2))
			startC3T0_1, endC3T0_1 := RangeByTokenAndClusterSize(t0, 1, consumerRanges, 3)
			startC3T2_3, endC3T2_3 := RangeByTokenAndClusterSize(t2C3, 3, consumerRanges, 3)
			Expect(s.offsetMap[key][0]).To(Equal(offsetRange{start: startC3T0_1, end: endC3T0_1, value: valueC3_T0_1}))
			Expect(s.offsetMap[key][1]).To(Equal(offsetRange{start: startC3T2_3, end: endC3T2_3, value: valueC3_T2_3}))
		})

		It("should ignore previous offsets by source for identical ranges", func() {
			localDb := new(dbMocks.Client)

			// Identical ranges, compared by source
			valueT0old := valueC3_T0_1
			valueT0old.Source = OffsetSource{Id: GenId{Start: t0, Version: 1}}
			valueT0new := valueC3_T0_1
			valueT0new.Offset = 445566990123
			valueT0new.Source = OffsetSource{Id: GenId{Start: t0, Version: 2}} // Newer source

			localDb.On("Offsets").Return([]OffsetStoreKeyValue{
				{Key: key, Value: valueT0old},
				{Key: key, Value: valueT0new},
			}, nil)

			s := newTestOffsetState(nil, 4)
			s.localDb = localDb

			Expect(s.Init()).NotTo(HaveOccurred())
			startC3T0_1, endC3T0_1 := RangeByTokenAndClusterSize(t0, 1, consumerRanges, 3)
			Expect(s.offsetMap[key]).To(HaveLen(1))
			Expect(s.offsetMap[key][0]).To(Equal(offsetRange{start: startC3T0_1, end: endC3T0_1, value: valueT0new}))
		})

		It("should ignore previous offsets by value for identical ranges", func() {
			localDb := new(dbMocks.Client)

			// Identical ranges, compared by values
			valueT0new := valueC3_T0_1
			valueT0new.Offset = 445566990123
			valueT0new.Version = 100
			valueT0old := valueC3_T0_1

			localDb.On("Offsets").Return([]OffsetStoreKeyValue{
				{Key: key, Value: valueT0new},
				{Key: key, Value: valueT0old},
			}, nil)

			s := newTestOffsetState(nil, 4)
			s.localDb = localDb

			Expect(s.Init()).NotTo(HaveOccurred())
			startC3T0_1, endC3T0_1 := RangeByTokenAndClusterSize(t0, 1, consumerRanges, 3)
			Expect(s.offsetMap[key]).To(HaveLen(1))
			Expect(s.offsetMap[key][0]).To(Equal(offsetRange{start: startC3T0_1, end: endC3T0_1, value: valueT0new}))
		})

		It("should ignore previous offsets by timestamp for different ranges", func() {
			localDb := new(dbMocks.Client)

			valueT0new := valueC3_T0_1
			valueT0new.Source.Timestamp = time.Now().UnixMicro() // Newer timestamp
			valueT6old := Offset{
				Version:     4,
				ClusterSize: 6,
				Offset:      7090,
				Token:       GetTokenAtIndex(12, 1), // T6
				Index:       1,
			}

			localDb.On("Offsets").Return([]OffsetStoreKeyValue{
				{Key: key, Value: valueT0new},
				{Key: key, Value: valueT6old},
			}, nil)

			s := newTestOffsetState(nil, 4)
			s.localDb = localDb

			Expect(s.Init()).NotTo(HaveOccurred())
			startC3T0_1, endC3T0_1 := RangeByTokenAndClusterSize(t0, 1, consumerRanges, 3)
			Expect(s.offsetMap[key]).To(HaveLen(1))
			Expect(s.offsetMap[key][0]).To(Equal(offsetRange{start: startC3T0_1, end: endC3T0_1, value: valueT0new}))
		})
	})

	Describe("MaxProducedOffset()", func() {
		It("should get the max produced offset from local", func() {
			gen := Generation{Followers: []int{2, 0}}
			expected := int64(123)
			topology := newTestTopology(3, 1)

			datalog := new(dataMocks.Datalog)
			datalog.On("ReadProducerOffset", mock.Anything).Return(expected, nil)
			gossiper := new(iMocks.Gossiper)
			gossiper.On("ReadProducerOffset", mock.Anything, mock.Anything).Return(int64(0), fmt.Errorf("Fake error"))
			discoverer := new(dMocks.Discoverer)
			discoverer.On("GenerationInfo", mock.Anything, mock.Anything).Return(&gen, nil)
			discoverer.On("Topology").Return(&topology)

			s := defaultOffsetState{
				datalog:    datalog,
				gossiper:   gossiper,
				discoverer: discoverer,
			}
			obtained, err := s.MaxProducedOffset(&TopicDataId{})
			Expect(err).NotTo(HaveOccurred())
			Expect(obtained).To(Equal(expected))
		})

		It("should get the max produced offset from peers", func() {
			expected := int64(12345)
			topology := newTestTopology(3, 1)

			datalog := new(dataMocks.Datalog)
			datalog.On("ReadProducerOffset", mock.Anything).Return(expected-10, nil)
			gossiper := new(iMocks.Gossiper)
			gossiper.On("ReadProducerOffset", mock.Anything, mock.Anything).Return(expected, nil)
			discoverer := new(dMocks.Discoverer)
			discoverer.On("GenerationInfo", mock.Anything, mock.Anything).
				Return(&Generation{Followers: []int{2, 0}}, nil)
			discoverer.On("Topology").Return(&topology)

			s := defaultOffsetState{
				datalog:    datalog,
				gossiper:   gossiper,
				discoverer: discoverer,
			}
			obtained, err := s.MaxProducedOffset(&TopicDataId{})
			Expect(err).NotTo(HaveOccurred())
			Expect(obtained).To(Equal(expected))
		})
	})
})

func newTestOffsetState(originalMap map[OffsetStoreKey][]offsetRange, consumerRanges int) *defaultOffsetState {
	config := new(cMocks.Config)
	config.On("ConsumerRanges").Return(consumerRanges)

	// Deep clone the map
	offsetMap := make(map[OffsetStoreKey][]offsetRange)
	for k, v := range originalMap {
		list := make([]offsetRange, len(v))
		for n, item := range v {
			list[n] = item
		}
		offsetMap[k] = list
	}

	return &defaultOffsetState{
		offsetMap: offsetMap,
		config:    config,
	}
}
