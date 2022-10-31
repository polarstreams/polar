package consuming

import (
	cMocks "github.com/barcostreams/barco/internal/test/conf/mocks"
	. "github.com/barcostreams/barco/internal/types"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("defaultOffsetState", func() {
	const consumerRanges = 4
	config := new(cMocks.Config)
	config.On("ConsumerRanges").Return(consumerRanges)
	tokenC3_2 := GetTokenAtIndex(3, 2)

	Describe("Get()", func() {
		key1 := OffsetStoreKey{Group: "g1", Topic: "t1"}
		valueC12_T0_2 := Offset{
			Token:       StartToken,
			Index:       2,
			Version:     3,
			ClusterSize: 12,
			Offset:      456,
		}
		valueC12_T0_3 := Offset{
			Token:       StartToken,
			Index:       3,
			Version:     3,
			ClusterSize: 12,
			Offset:      789,
		}
		valueC3_T0_1 := Offset{
			Token:       StartToken,
			Index:       1,
			Version:     1,
			ClusterSize: 3,
			Offset:      123,
		}
		valueC3_T2_3 := Offset{
			Token:       tokenC3_2,
			Index:       3,
			Version:     1,
			ClusterSize: 3,
			Offset:      5050,
		}

		startC12_T0_2, endC12_T0_2 := RangeByTokenAndClusterSize(StartToken, 2, consumerRanges, 12)
		startC12_T0_3, endC12_T0_3 := RangeByTokenAndClusterSize(StartToken, 3, consumerRanges, 12)
		startC3T0_1, endC3T0_1 := RangeByTokenAndClusterSize(StartToken, 1, consumerRanges, 3)
		startC3T2_3, endC3T2_3 := RangeByTokenAndClusterSize(tokenC3_2, 3, consumerRanges, 3)
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
			value, rangesMatch := s.Get("g1", "t2", StartToken, 1, 3)
			Expect(value).To(BeNil())
			Expect(rangesMatch).To(BeFalse())
		})

		It("should return the offset when there's ranges match", func() {
			// 3-broker cluster
			value, rangesMatch := s.Get("g1", "t1", StartToken, 1, 3)
			Expect(value).NotTo(BeNil())
			Expect(rangesMatch).To(BeTrue())
			Expect(*value).To(Equal(valueC3_T0_1))
		})

		It("should return nil when ranges don't intersect", func() {
			value, rangesMatch := s.Get("g1", "t2", StartToken, 3, 3)
			Expect(value).To(BeNil())
			Expect(rangesMatch).To(BeFalse())

			value, rangesMatch = s.Get("g1", "t2", GetTokenAtIndex(6, 1), 0, 6)
			Expect(value).To(BeNil())
			Expect(rangesMatch).To(BeFalse())
		})

		It("should return the offset when there's range is contained", func() {
			// 6-broker cluster
			value, rangesMatch := s.Get("g1", "t1", StartToken, 3, 6)
			Expect(value).NotTo(BeNil())
			Expect(rangesMatch).To(BeFalse(), "The range must not match")
			Expect(*value).To(Equal(valueC3_T0_1), "The returned value is still valueC3_T0_1")

			value, rangesMatch = s.Get("g1", "t1", StartToken, 2, 6)
			Expect(value).NotTo(BeNil())
			Expect(rangesMatch).To(BeFalse(), "The range must not match")
			Expect(*value).To(Equal(valueC3_T0_1), "The returned value is still valueC3_T0_1")
		})

		It("should return any of the previous offsets when the cluster is smaller than offset", func() {
			// 6-broker cluster
			value, rangesMatch := s.Get("g1", "t1", StartToken, 1, 6)
			Expect(value).NotTo(BeNil())
			Expect(rangesMatch).To(BeFalse(), "The range must not match")
			Expect(*value).To(Equal(valueC12_T0_2), "The returned value is one of the smaller ranges")
		})

		It("should work with the last range", func() {
			value, rangesMatch := s.Get("g1", "t1", tokenC3_2, 3, 3)
			Expect(value).NotTo(BeNil())
			Expect(rangesMatch).To(BeTrue(), "The range must match")
			Expect(*value).To(Equal(valueC3_T2_3), "The returned value should be the last offset")

			value, rangesMatch = s.Get("g1", "t1", GetTokenAtIndex(6, 5), 3, 6)
			Expect(value).NotTo(BeNil())
			Expect(rangesMatch).To(BeFalse(), "The range must not match")
			Expect(*value).To(Equal(valueC3_T2_3), "The returned value should be the last offset")
		})
	})
})
