package consuming

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net/http/httptest"

	"github.com/barcostreams/barco/internal/conf"
	"github.com/barcostreams/barco/internal/data"
	cMocks "github.com/barcostreams/barco/internal/test/conf/mocks"
	dMocks "github.com/barcostreams/barco/internal/test/discovery/mocks"
	iMocks "github.com/barcostreams/barco/internal/test/interbroker/mocks"
	tMocks "github.com/barcostreams/barco/internal/test/types/mocks"
	. "github.com/barcostreams/barco/internal/types"
	"github.com/klauspost/compress/zstd"
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

	Describe("marshalResponse()", func() {
		topic := TopicDataId{
			Name:       "my-topic1",
			Token:      -3074457345618259968,
			RangeIndex: 2,
			Version:    3,
		}
		config := new(cMocks.Config)
		config.On("MaxGroupSize").Return(1 * conf.MiB)
		decoder, err := zstd.NewReader(bytes.NewReader(make([]byte, 0)),
			zstd.WithDecoderConcurrency(1), zstd.WithDecoderMaxMemory(uint64(config.MaxGroupSize())))
		Expect(err).NotTo(HaveOccurred())
		q := groupReadQueue{
			config:        config,
			decoder:       decoder,
			decoderBuffer: make([]byte, 16_384),
		}

		It("should marshal empty responses into JSON", func() {
			q := groupReadQueue{
				config:        config,
				decoder:       decoder,
				decoderBuffer: make([]byte, 16_384),
			}

			w := httptest.NewRecorder()
			responseItem := consumerResponseItem{
				chunk: data.NewEmptyChunk(123),
				topic: topic,
			}
			err := q.marshalResponse(w, jsonFormat, []consumerResponseItem{responseItem})
			Expect(err).NotTo(HaveOccurred())

			resp := w.Result()
			Expect(resp.Header.Get("Content-Type")).To(Equal("application/json"))
			body, _ := io.ReadAll(resp.Body)
			Expect(string(body)).To(Equal(`[{"topic":"my-topic1","token":"-3074457345618259968","rangeIndex":2,"version":3,"startOffset":"123","values":[]}]`))
		})

		It("should marshal multiple records", func() {
			writeBuffer := &bytes.Buffer{}
			compressor, _ := zstd.NewWriter(
				writeBuffer, zstd.WithEncoderCRC(true), zstd.WithEncoderLevel(zstd.SpeedDefault))

			msg1 := `{"hello": 1}`
			msg2 := `{"hello": 2, "example": true}`
			for _, msg := range []string{msg1, msg2} {
				err := binary.Write(compressor, conf.Endianness, recordHeader{
					Length: uint32(len(msg)),
				})
				Expect(err).NotTo(HaveOccurred())
				_, err = compressor.Write([]byte(msg))
				Expect(err).NotTo(HaveOccurred())
			}
			compressor.Close()

			w := httptest.NewRecorder()
			responseItem := consumerResponseItem{
				chunk: &data.ReadSegmentChunk{
					Buffer: writeBuffer.Bytes(),
					Start:  567,
					Length: 2,
				},
				topic: topic,
			}

			err := q.marshalResponse(w, jsonFormat, []consumerResponseItem{responseItem})
			Expect(err).NotTo(HaveOccurred())

			resp := w.Result()
			body, _ := io.ReadAll(resp.Body)
			expected := fmt.Sprintf(
				`[{"topic":"my-topic1","token":"-3074457345618259968","rangeIndex":2,"version":3,"startOffset":"567","values":[%s,%s]}]`,
				msg1, msg2,
			)
			Expect(string(body)).To(Equal(expected))

		})
	})
})
