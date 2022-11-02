package consuming

import (
	cMocks "github.com/barcostreams/barco/internal/test/conf/mocks"
	. "github.com/barcostreams/barco/internal/types"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/rs/zerolog/log"
)

var _ = Describe("defaultOffsetState", func() {
	const consumerRanges = 4
	const group = "g1"
	const topic = "t1"
	t0 := StartToken
	t1C3 := GetTokenAtIndex(3, 1)
	t2C3 := GetTokenAtIndex(3, 2)

	key1 := OffsetStoreKey{Group: group, Topic: topic}
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

	Describe("Get()", func() {
		config := new(cMocks.Config)
		config.On("ConsumerRanges").Return(consumerRanges)
		startC12_T0_2, endC12_T0_2 := RangeByTokenAndClusterSize(t0, 2, consumerRanges, 12)
		startC12_T0_3, endC12_T0_3 := RangeByTokenAndClusterSize(t0, 3, consumerRanges, 12)
		startC3T0_1, endC3T0_1 := RangeByTokenAndClusterSize(t0, 1, consumerRanges, 3)
		startC3T2_3, endC3T2_3 := RangeByTokenAndClusterSize(t2C3, 3, consumerRanges, 3)
		offsetMap := map[OffsetStoreKey][]offsetRange{
			key1: {
				{start: startC12_T0_2, end: endC12_T0_2, value: valueC12_T0_2},
				{start: startC12_T0_3, end: endC12_T0_3, value: valueC12_T0_3},
				{start: startC3T0_1, end: endC3T0_1, value: valueC3_T0_1},
				{start: startC3T2_3, end: endC3T2_3, value: valueC3_T2_3},
			},
		}

		s := &defaultOffsetState2{
			offsetMap: offsetMap,
			config:    config,
		}

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
				key1: {
					{start: startC12_T0_2, end: endC12_T0_2, value: valueC12_T0_2},
				},
			}

			s := &defaultOffsetState2{
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
			Expect(*value).To(Equal(valueC12_T0_2), "The returned value is one of the smaller ranges")
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

	Describe("Set()", func() {
		startC12_T0_2, endC12_T0_2 := RangeByTokenAndClusterSize(t0, 2, consumerRanges, 12)
		startC12_T0_3, endC12_T0_3 := RangeByTokenAndClusterSize(t0, 3, consumerRanges, 12)
		startC3T0_1, endC3T0_1 := RangeByTokenAndClusterSize(t0, 1, consumerRanges, 3)
		startC3T2_3, endC3T2_3 := RangeByTokenAndClusterSize(t2C3, 3, consumerRanges, 3)
		startC3T1_0, endC3T1_0 := RangeByTokenAndClusterSize(t1C3, 0, consumerRanges, 3)

		offsetMap := map[OffsetStoreKey][]offsetRange{
			key1: {
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
			Expect(s.offsetMap[key1]).To(HaveLen(4))
			Expect(s.offsetMap[key1][3]).To(Equal(offsetRange{start: startC3T2_3, end: endC3T2_3, value: offsetEnd}))

			offsetMiddle := Offset{
				Version:     2,
				ClusterSize: 3,
				Offset:      7090,
				Token:       t1C3,
				Index:       0,
			}
			result = s.Set(group, topic, offsetMiddle, OffsetCommitNone)

			Expect(result).To(BeTrue())
			Expect(s.offsetMap[key1]).To(HaveLen(5))
			Expect(s.offsetMap[key1][3]).To(Equal(offsetRange{start: startC3T1_0, end: endC3T1_0, value: offsetMiddle}))
			Expect(s.offsetMap[key1][4]).To(Equal(offsetRange{start: startC3T2_3, end: endC3T2_3, value: offsetEnd}))
		})

		It("should insert a range at the beginning", func() {
			// Empty initial map
			s := newTestOffsetState(map[OffsetStoreKey][]offsetRange{}, consumerRanges)

			offset := Offset{
				Version:     2,
				ClusterSize: 3,
				Offset:      7090,
				Token:       t2C3,
				Index:       3,
			}
			result := s.Set(group, topic, offset, OffsetCommitNone)
			Expect(result).To(BeTrue())
			Expect(s.offsetMap[key1]).To(HaveLen(1))
			Expect(s.offsetMap[key1][0]).To(Equal(offsetRange{start: startC3T2_3, end: endC3T2_3, value: offset}))

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
			Expect(s.offsetMap[key1]).To(HaveLen(2))
			Expect(s.offsetMap[key1][0]).To(Equal(offsetRange{start: startC3T1_0, end: endC3T1_0, value: offset}))
		})

		It("should insert when offset splitting at the start", func ()  {
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
			Expect(s.offsetMap[key1]).To(HaveLen(4))
			Expect(s.offsetMap[key1][2]).To(Equal(offsetRange{start: startC6T0_2, end: endC6T0_2, value: offset}))
			Expect(s.offsetMap[key1][3]).To(Equal(
				offsetRange{start: startC6T0_3, end: endC6T0_3, value: existingOffset}))
		})

		It("should insert when offset splitting at the end", func ()  {
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
			Expect(s.offsetMap[key1]).To(HaveLen(4))
			Expect(s.offsetMap[key1][2]).To(Equal(
				offsetRange{start: startC6T0_2, end: endC6T0_2, value: existingOffset}))
			Expect(s.offsetMap[key1][3]).To(Equal(offsetRange{start: startC6T0_3, end: endC6T0_3, value: offset}))
		})

		It("should join entirely contained ranges", func ()  {
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
			Expect(s.offsetMap[key1]).To(HaveLen(2))
			Expect(s.offsetMap[key1][0]).To(Equal(offsetRange{start: startC6T0_1, end: endC6T0_1, value: offset}))
		})

		It("should join partially contained ranges", func ()  {
			log.Debug().Msgf("--Starting test")
			// Replace C12_T0_2 and C12_T0_3 with C3_T0_0
			startC3T0_0, endC3T0_0 := RangeByTokenAndClusterSize(t0, 0, consumerRanges, 3)
			// Values C12_T0_0 and C12_T0_1 are not contained (range at the beginning empty)

			s := newTestOffsetState(offsetMap, consumerRanges)
			Expect(s.offsetMap[key1]).To(HaveLen(3))
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
			Expect(s.offsetMap[key1]).To(HaveLen(2))
			Expect(s.offsetMap[key1][0]).To(Equal(offsetRange{start: startC3T0_0, end: endC3T0_0, value: offset}))
			Expect(s.offsetMap[key1][1]).To(Equal(offsetRange{start: startC3T0_1, end: endC3T0_1, value: valueC3_T0_1}))
		})
	})
})

func newTestOffsetState(originalMap map[OffsetStoreKey][]offsetRange, consumerRanges int) *defaultOffsetState2 {
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

	return &defaultOffsetState2{
		offsetMap: offsetMap,
		config:    config,
	}
}
