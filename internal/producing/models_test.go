package producing

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io"
	"strings"
	"time"

	"github.com/polarstreams/polar/internal/conf"
	. "github.com/polarstreams/polar/internal/types"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("recordItem", func() {
	Describe("marshal()", func() {
		It("should write multiple records with line-separated format", func() {
			data := "something0\nsomething1\n\nsomething2\nsomething3\nsomething4\nsomething5\nsomething6\nsomething7\n"

			item := recordItem{
				length:      uint32(len(data)),
				timestamp:   time.Now().UnixMicro(),
				contentType: ContentTypeNDJSON,
				body:        strings.NewReader(data),
			}

			readBuffer := make([]byte, 64)
			writer := new(bytes.Buffer)
			totalRecords, err := item.marshal(writer, readBuffer)
			Expect(err).NotTo(HaveOccurred())
			Expect(totalRecords).To(Equal(8))

			written := writer.Bytes()
			messages := strings.Split(data, "\n")
			resultReader := bytes.NewReader(written)

			// Per message it should write a record according to the format
			for _, message := range messages {
				if message == "" {
					// Empty messages should be ignored
					continue
				}
				assertRecord(resultReader, item.timestamp, message)
			}
		})

		It("should write a single record", func() {
			data := "something0\nsomething1\n"

			item := recordItem{
				length:      uint32(len(data)),
				timestamp:   time.Now().UnixMicro(),
				contentType: ContentTypeJSON,
				body:        strings.NewReader(data),
			}

			readBuffer := make([]byte, 64)
			writer := new(bytes.Buffer)
			totalRecords, err := item.marshal(writer, readBuffer)
			Expect(err).NotTo(HaveOccurred())
			Expect(totalRecords).To(Equal(1))

			written := writer.Bytes()
			resultReader := bytes.NewReader(written)
			assertRecord(resultReader, item.timestamp, data)
		})

		It("should write a single record on multi-line", func() {
			data := "something0\n"

			item := recordItem{
				length:      uint32(len(data)),
				timestamp:   time.Now().UnixMicro(),
				contentType: ContentTypeNDJSON,
				body:        strings.NewReader(data),
			}

			readBuffer := make([]byte, 64)
			writer := new(bytes.Buffer)
			totalRecords, err := item.marshal(writer, readBuffer)
			Expect(err).NotTo(HaveOccurred())
			Expect(totalRecords).To(Equal(1))

			written := writer.Bytes()
			resultReader := bytes.NewReader(written)
			assertRecord(resultReader, item.timestamp, data[:len(data)-1])
		})

		It("should result in error when token is too long", func() {
			data := "something0\nsomething1"

			item := recordItem{
				length:      uint32(len(data)),
				timestamp:   0,
				contentType: ContentTypeNDJSON,
				body:        strings.NewReader(data),
			}

			readBuffer := make([]byte, 4)
			_, err := item.marshalRecordsByLine(new(bytes.Buffer), readBuffer)
			Expect(err).To(Equal(bufio.ErrTooLong))
		})
	})
})

func assertRecord(reader io.Reader, expectedTimestamp int64, expectedBody string) {
	var timestamp int64
	var length uint32
	binary.Read(reader, conf.Endianness, &timestamp)
	binary.Read(reader, conf.Endianness, &length)
	Expect(timestamp).To(Equal(expectedTimestamp))
	Expect(length).To(Equal(uint32(len(expectedBody))))
	body := make([]byte, length)
	reader.Read(body)
	Expect(string(body)).To(Equal(expectedBody))

}
