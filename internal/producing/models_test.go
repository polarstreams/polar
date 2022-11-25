package producing

import (
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
		It("should write a single record in a single buffer", func() {
			const body = "something0\nsomething1\n"
			buffers := [][]byte{[]byte(body)}

			item := recordItem{
				length:      uint32(len(buffers[0])),
				timestamp:   time.Now().UnixMicro(),
				contentType: ContentTypeJSON,
				buffers:     buffers,
			}

			writer := new(bytes.Buffer)
			totalRecords, err := item.marshal(writer)
			Expect(err).NotTo(HaveOccurred())
			Expect(totalRecords).To(Equal(1))

			written := writer.Bytes()
			resultReader := bytes.NewReader(written)
			assertRecord(resultReader, item.timestamp, body)
		})

		It("should write a single record in multiple buffers", func() {
			const body = "hello world"
			buffers := [][]byte{[]byte("hello "), []byte("worldZZZZZZZZZZ")} // Additional bytes should be ignored

			item := recordItem{
				length:      uint32(len(body)),
				timestamp:   time.Now().UnixMicro(),
				contentType: ContentTypeJSON,
				buffers:     buffers,
			}

			writer := new(bytes.Buffer)
			totalRecords, err := item.marshal(writer)
			Expect(err).NotTo(HaveOccurred())
			Expect(totalRecords).To(Equal(1))

			written := writer.Bytes()
			resultReader := bytes.NewReader(written)
			assertRecord(resultReader, item.timestamp, body)
		})

		It("should write multiple records with line-separated format in multiple buffers", func() {
			data := "something0\nsomething1\n\nsomething2\nsomething3\nsomething4\nsomething5\nsomething6\nsomething7\n"
			dataBuf := []byte(data)
			item := recordItem{
				length:      uint32(len(data)),
				timestamp:   time.Now().UnixMicro(),
				contentType: ContentTypeNDJSON,
				buffers: [][]byte{
					dataBuf[:22],
					dataBuf[22:26],
					dataBuf[26:27],
					dataBuf[27:],
				},
			}

			testMultipleRecords(item, data, 8)
		})

		It("should write multiple records with line-separated format in multiple buffers (over-dimensioned)", func() {
			data := "something0\nsomething1\n\nsomething2\nsomething3\nsomething4\nsomething5\nsomething6\nsomething7\n"
			dataBuf := []byte(data)
			item := recordItem{
				length:      uint32(len(data)),
				timestamp:   time.Now().UnixMicro(),
				contentType: ContentTypeNDJSON,
				buffers: [][]byte{
					dataBuf[:22],
					dataBuf[22:26],
					dataBuf[26:27],
					dataBuf[27:],
					[]byte("THIS SHOULD BE DISCARDED"),
				},
			}

			testMultipleRecords(item, data, 8)
		})

		It("should write multiple records with line-separated format with a single buffer", func() {
			data := "something0\nsomething1\n\nsomething2\nsomething3\nsomething4\nsomething5"
			item := recordItem{
				length:      uint32(len(data)),
				timestamp:   time.Now().UnixMicro(),
				contentType: ContentTypeNDJSON,
				buffers:     [][]byte{[]byte(data)},
			}

			testMultipleRecords(item, data, 6)
		})

		It("should write multiple records with line-separated format with a single buffer over-dimensioned", func() {
			data := "something0\nsomething1\n\nsomething2\nsomething3\nsomething4\nsomething5"
			additionalBytes := "THIS SHOULD BE DISCARDED"
			item := recordItem{
				length:      uint32(len(data)),
				timestamp:   time.Now().UnixMicro(),
				contentType: ContentTypeNDJSON,
				buffers:     [][]byte{[]byte(data + additionalBytes)}, // Include the additional bytes
			}

			testMultipleRecords(item, data, 6)
		})

		It("should write a single record on multi-line with a single buffer", func() {
			data := "something0\n"

			item := recordItem{
				length:      uint32(len(data)),
				timestamp:   time.Now().UnixMicro(),
				contentType: ContentTypeNDJSON,
				buffers:     [][]byte{[]byte(data)},
			}

			writer := new(bytes.Buffer)
			totalRecords, err := item.marshal(writer)
			Expect(err).NotTo(HaveOccurred())
			Expect(totalRecords).To(Equal(1))

			written := writer.Bytes()
			resultReader := bytes.NewReader(written)
			assertRecord(resultReader, item.timestamp, data[:len(data)-1])
		})

		It("should write a single record on multi-line with multiple buffers", func() {
			data := "something else"

			item := recordItem{
				length:      uint32(len(data)),
				timestamp:   time.Now().UnixMicro(),
				contentType: ContentTypeNDJSON,
				buffers:     [][]byte{[]byte("something "), []byte("else")},
			}

			writer := new(bytes.Buffer)
			totalRecords, err := item.marshal(writer)
			Expect(err).NotTo(HaveOccurred())
			Expect(totalRecords).To(Equal(1))

			written := writer.Bytes()
			resultReader := bytes.NewReader(written)
			assertRecord(resultReader, item.timestamp, data)
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
	_, err := io.ReadFull(reader, body)
	Expect(err).NotTo(HaveOccurred())
	Expect(string(body)).To(Equal(expectedBody))
}

func testMultipleRecords(item recordItem, data string, expectedRecords int) {
	writer := new(bytes.Buffer)
	totalRecords, err := item.marshal(writer)
	Expect(err).NotTo(HaveOccurred())
	Expect(totalRecords).To(Equal(expectedRecords))

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
}
