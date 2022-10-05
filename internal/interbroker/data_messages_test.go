package interbroker

import (
	"bytes"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("fileStreamRequest", func() {
	Describe("Marshal() / unmarshal", func() {
		It("should marshal/unmarshal", func() {
			topic := "hello world"
			r := fileStreamRequest{
				meta: dataRequestMeta{
					SegmentId:    1,
					Token:        2,
					RangeIndex:   3,
					GenVersion:   4,
					StartOffset:  5,
					RecordLength: 6,
					TopicLength:  uint8(len(topic)),
				},
				topic:   topic,
				maxSize: 1234567,
			}

			buf := new(bytes.Buffer)
			header := header{Version: 1, StreamId: 3, Op: fileStreamOp}
			// Marshal the header and body
			r.Marshal(buf, &header)

			// Unmarshal the header
			obtainedHeader, err := readHeader(buf.Bytes())
			Expect(err).NotTo(HaveOccurred())
			obtainedHeader.Crc = 0 // Set for comparison
			Expect(*obtainedHeader).To(Equal(header))

			// Unmarshal the body
			obtained, err := unmarshalFileStreamRequest(buf.Bytes()[headerSize:])
			Expect(err).NotTo(HaveOccurred())
			Expect(*obtained).To(Equal(r))
		})
	})
})
