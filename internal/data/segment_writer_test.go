package data

import (
	"bytes"
	"io/ioutil"
	"time"

	"github.com/barcostreams/barco/internal/test/conf/mocks"
	mocks2 "github.com/barcostreams/barco/internal/test/types/mocks"
	. "github.com/barcostreams/barco/internal/types"
	"github.com/barcostreams/barco/internal/utils"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/stretchr/testify/mock"
)

var _ = Describe("SegmentWriter", func() {
	Describe("alignBuffer()", func() {
		const alignment = 512
		It("should complete the remaining length", func() {
			s := &SegmentWriter{
				buffer: new(bytes.Buffer),
			}

			s.alignBuffer()
			Expect(s.buffer.Len()).To(Equal(0))

			s.buffer.Write([]byte{0, 0, 0})
			s.alignBuffer()
			Expect(s.buffer.Len()).To(Equal(alignment))
			Expect(s.buffer.Bytes()[:3]).To(Equal([]byte{0, 0, 0}))

			// Followed by 0x80
			for i := 3; i < alignment; i++ {
				Expect(s.buffer.Bytes()[i]).To(Equal(alignmentFlag))
			}

			// It's aligned, no effect
			s.alignBuffer()
			Expect(s.buffer.Len()).To(Equal(alignment))

			s.buffer.Write([]byte{1})
			s.alignBuffer()
			Expect(s.buffer.Len()).To(Equal(alignment * 2))
			Expect(s.buffer.Bytes()[alignment]).To(Equal(byte(1)))

			// Filled with 0x80
			for i := 1; i < alignment; i++ {
				Expect(s.buffer.Bytes()[alignment+i]).To(Equal(alignmentFlag))
			}
		})
	})

	Describe("writeLoopAsLeader()", func() {
		It("should create new files and flush", func() {
			config := new(mocks.Config)
			config.On("IndexFilePeriodBytes").Return(5 * 1024 * 1024)
			config.On("MaxGroupSize").Return(100)
			config.On("SegmentBufferSize").Return(300)
			config.On("MaxSegmentSize").Return(500)
			config.On("SegmentFlushInterval").Return(1 * time.Second)

			replicator := new(mocks2.Replicator)
			replicator.On("SendToFollowers", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

			dir, err := ioutil.TempDir("", "test_write_loop_leader")
			Expect(err).NotTo(HaveOccurred())
			s := &SegmentWriter{
				Items:      make(chan SegmentChunk, 0),
				buffer:     utils.NewBufferCap(100),
				config:     config,
				indexFile:  newIndexFileWriter(dir, config),
				basePath:   dir,
				Topic:      TopicDataId{Name: "abc"},
				replicator: replicator,
			}

			s.createFile(0)
			go s.writeLoopAsLeader()

			s.Items <- &testWriteItem{
				data:     make([]byte, 100),
				response: make(chan error, 1),
			}
			s.Items <- &testWriteItem{
				data:     make([]byte, 100),
				response: make(chan error, 1),
			}
			lastItem := &testWriteItem{
				data:     make([]byte, 100),
				response: make(chan error, 1),
			}
			s.Items <- lastItem

			Expect(<-lastItem.response).NotTo(HaveOccurred())

			close(s.Items)
		})
	})
})

type testWriteItem struct {
	data     []byte
	response chan error
}

// DataBlock() gets the compressed payload of the chunk
func (d *testWriteItem) DataBlock() []byte {
	return d.data
}

func (d *testWriteItem) Replication() ReplicationInfo {
	return ReplicationInfo{
		Leader:    &BrokerInfo{},
		Followers: []BrokerInfo{},
		Token:     0,
	}
}

func (d *testWriteItem) StartOffset() int64 {
	return 123
}

func (d *testWriteItem) RecordLength() uint32 {
	return 200
}

func (d *testWriteItem) SetResult(err error) {
	d.response <- err
}
