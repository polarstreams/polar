package producing

import (
	"bytes"
	"encoding/binary"
	"io"
	"strings"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/polarstreams/polar/internal/conf"
	. "github.com/polarstreams/polar/internal/types"
)

var _ = Describe("recordItem", func() {
	Describe("marshal()", func() {
		Context("JSON", func()  {
			It("should write a single record in a single buffer", func() {
				const body = "something0\nsomething1\n"
				buffers := [][]byte{[]byte(body)}

				item := recordItem{
					length:      uint32(len(buffers[0])),
					timestamp:   time.Now().UnixMicro(),
					contentType: MIMETypeJSON,
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
					contentType: MIMETypeJSON,
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

		})

		Context("NDJSON", func()  {
			It("should write multiple records with line-separated format in multiple buffers", func() {
				data := "something0\nsomething1\n\nsomething2\nsomething3\nsomething4\nsomething5\nsomething6\nsomething7\n"
				dataBuf := []byte(data)
				item := recordItem{
					length:      uint32(len(data)),
					timestamp:   time.Now().UnixMicro(),
					contentType: MIMETypeNDJSON,
					buffers: [][]byte{
						dataBuf[:22],
						dataBuf[22:26],
						dataBuf[26:27],
						dataBuf[27:],
					},
				}

				testMultipleLineRecords(item, data, 8)
			})

			It("should write multiple records with line-separated format in multiple buffers (over-dimensioned)", func() {
				data := "something0\nsomething1\n\nsomething2\nsomething3\nsomething4\nsomething5\nsomething6\nsomething7\n"
				dataBuf := []byte(data)
				item := recordItem{
					length:      uint32(len(data)),
					timestamp:   time.Now().UnixMicro(),
					contentType: MIMETypeNDJSON,
					buffers: [][]byte{
						dataBuf[:22],
						dataBuf[22:26],
						dataBuf[26:27],
						dataBuf[27:],
						[]byte("THIS SHOULD BE DISCARDED"),
					},
				}

				testMultipleLineRecords(item, data, 8)
			})

			It("should write multiple records with line-separated format with a single buffer", func() {
				data := "something0\nsomething1\n\nsomething2\nsomething3\nsomething4\nsomething5"
				item := recordItem{
					length:      uint32(len(data)),
					timestamp:   time.Now().UnixMicro(),
					contentType: MIMETypeNDJSON,
					buffers:     [][]byte{[]byte(data)},
				}

				testMultipleLineRecords(item, data, 6)
			})

			It("should write multiple records with line-separated format with a single buffer over-dimensioned", func() {
				data := "something0\nsomething1\n\nsomething2\nsomething3\nsomething4\nsomething5"
				additionalBytes := "THIS SHOULD BE DISCARDED"
				item := recordItem{
					length:      uint32(len(data)),
					timestamp:   time.Now().UnixMicro(),
					contentType: MIMETypeNDJSON,
					buffers:     [][]byte{[]byte(data + additionalBytes)}, // Include the additional bytes
				}

				testMultipleLineRecords(item, data, 6)
			})

			It("should write a single record on multi-line with a single buffer", func() {
				data := "something0\n"

				item := recordItem{
					length:      uint32(len(data)),
					timestamp:   time.Now().UnixMicro(),
					contentType: MIMETypeNDJSON,
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
					contentType: MIMETypeNDJSON,
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

		Context("ProducerBinary", func()  {
			It("should write multiple records with frame format in multiple buffers", func() {
				data := concat(messageFrame("something0"), messageFrame("something1"), messageFrame("something else"))
				for i := 11; i < len(data)-1; i++ {
					item := recordItem{
						length:      uint32(len(data)),
						timestamp:   time.Now().UnixMicro(),
						contentType: MIMETypeProducerBinary,
						buffers: [][]byte{
							data[:10],
							data[10:i],
							data[i:],
						},
					}

					testFramedRecords(item, data, 3)
				}
			})

			It("should write a single record with frame format in multiple buffers", func() {
				data := concat(messageFrame("something0"))
				for i := 1; i < 9; i++ {
					item := recordItem{
						length:      uint32(len(data)),
						timestamp:   time.Now().UnixMicro(),
						contentType: MIMETypeProducerBinary,
						buffers: [][]byte{
							data[:i],
							data[i:],
						},
					}

					testFramedRecords(item, data, 1)
				}
			})
		})
	})
})

func concat(buffers... []byte) []byte {
	result := make([]byte, 0)
	for _, b := range buffers {
		result = append(result, b...)
	}
	return result
}

func messageFrame(value string) []byte {
	return concat(uint32Length(value), []byte(value))
}

func uint32Length(value string) []byte {
	buf := make([]byte, 4)
	conf.Endianness.PutUint32(buf, uint32(len(value)))
	return buf
}

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

func testMultipleLineRecords(item recordItem, data string, expectedRecords int) {
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

func testFramedRecords(item recordItem, data []byte, expectedRecords int) {
	writer := new(bytes.Buffer)
	totalRecords, err := item.marshal(writer)
	Expect(err).NotTo(HaveOccurred())
	Expect(totalRecords).To(Equal(expectedRecords))

	written := writer.Bytes()
	messages := make([]string, 0)
	readIndex := 0
	for readIndex < len(data) {
		stringLength := int(conf.Endianness.Uint32(data[readIndex:]))
		readIndex += 4
		messages = append(messages, string(data[readIndex:readIndex+stringLength]))
		readIndex += stringLength
	}
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
