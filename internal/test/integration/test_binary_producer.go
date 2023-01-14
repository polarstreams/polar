//go:build integration
// +build integration

package integration

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"net"
	"sync"

	. "github.com/onsi/gomega"
	"github.com/polarstreams/polar/internal/conf"
	. "github.com/polarstreams/polar/internal/types"
	"github.com/polarstreams/polar/internal/utils"
	"github.com/rs/zerolog/log"
)

const maxStreamId = 128

type BinaryProducer struct {
	connections []*binaryProducerConnection // Connections by ordinal
}

// Represents a client to a single broker
type binaryProducerConnection struct {
	inner net.Conn
	streamIds chan uint16
	handlers map[uint16]responseHandler
	muHandlers sync.Mutex
}

// Reads in the background
func (c *binaryProducerConnection) receiveResponses() {
	headerBuf := make([]byte, binaryHeaderSize)
	header := &binaryHeader{}
	var lastError error
	for {
		if _, err := io.ReadFull(c.inner, headerBuf); err != nil {
			lastError = err
			break
		}
		if err := binary.Read(bytes.NewReader(headerBuf), conf.Endianness, header); err != nil {
			lastError = err
			break
		}
		var body []byte = nil
		if header.BodyLength > 0 {
			body := make([]byte, header.BodyLength)
			if _, err := io.ReadFull(c.inner, body); err != nil {
				lastError = err
				break
			}
		}
		c.muHandlers.Lock()
		handler := c.handlers[header.StreamId]
		delete(c.handlers, header.StreamId)
		c.muHandlers.Unlock()

		// Call the handler
		res, err := unmarshalResponse(header, body)
		if err != nil {
			lastError = err
			break
		}
		if handler == nil {
			panic(fmt.Sprintf("handler for stream id %d is nil", header.StreamId))
		}
		handler(res)
	}
	log.Debug().Err(lastError).Msgf("Binary producer connection closed")
}

func NewBinaryProducerClient(clusterSize int) *BinaryProducer {
	p := &BinaryProducer{}
	p.init(clusterSize)
	return p
}

func (p *BinaryProducer) Close() {
	for _, conn := range p.connections {
		_ = conn.inner.Close()
	}
}

func (p *BinaryProducer) init(clusterSize int) {
	p.connections = make([]*binaryProducerConnection, clusterSize)
	startupMessage := new(bytes.Buffer)
	writeProducerHeader(startupMessage, &binaryHeader{
		Version:    1,
		Flags:      0,
		StreamId:   0,
		Op:         1, // startup op
		BodyLength: 0,
	})

	result := make(chan error, clusterSize)
	for i := 0; i < clusterSize; i++ {
		func(ordinal int) {
			c, err := net.Dial("tcp", fmt.Sprintf("127.0.0.%d:%d", ordinal+1, conf.DefaultProducerBinaryPort))
			if err != nil {
				result <- err
				return
			}
			conn := &binaryProducerConnection{
				inner:     c,
				streamIds: make(chan uint16, maxStreamId),
				handlers: make(map[uint16]responseHandler),
				muHandlers: sync.Mutex{},
			}
			go conn.receiveResponses()
			for i := 0; i < maxStreamId; i++ {
				conn.streamIds <- uint16(i)
			}
			response := make(chan binaryResponse)
			conn.handlers[0] = func(res binaryResponse) {
				response <- res
			}
			_, err = c.Write(startupMessage.Bytes())
			if err != nil {
				result <- err
				return
			}
			p.connections[ordinal] = conn
			err = toError(<- response)
			result <- err
		}(i)
	}

	for i := 0; i < clusterSize; i++ {
		err := <-result
		Expect(err).NotTo(HaveOccurred())
	}
}

func (p *BinaryProducer) Send(ordinal int, topic string, message string, partitionKey string) {
	request := new(bytes.Buffer)
	conn := p.connections[ordinal]
	streamId := <-conn.streamIds
	writeProducerRequest(request, streamId, &binaryProducerBody{
		PartitionKey: partitionKey,
		Topic:        topic,
		Messages:     []string{message},
	})
	response := make(chan binaryResponse, 1)
	conn.muHandlers.Lock()
	conn.handlers[streamId] = func(res binaryResponse) {
		response <- res
	}
	conn.muHandlers.Unlock()
	_, err := io.Copy(conn.inner, request)
	if err != nil {
		conn.muHandlers.Lock()
		delete(conn.handlers, streamId)
		conn.muHandlers.Unlock()
	}
	Expect(err).NotTo(HaveOccurred())
	res := <- response
	Expect(res).NotTo(BeNil())
	Expect(toError(res)).NotTo(HaveOccurred())
}

var binaryHeaderSize = utils.BinarySize(binaryHeader{})

// Test model for producer messages.
type binaryHeader struct {
	Version    uint8
	Flags      uint8
	StreamId   uint16
	Op         uint8
	BodyLength uint32
	Crc        uint32
}

type binaryProducerBody struct {
	Timestamp    *uint64
	PartitionKey string
	Topic        string
	Messages     []string
}

func writeProducerHeader(w BufferBackedWriter, header *binaryHeader) {
	err := binary.Write(w, conf.Endianness, header)
	Expect(err).NotTo(HaveOccurred())
	putCrc32(w)
}

func putCrc32(w BufferBackedWriter) {
	const crcByteSize = 4
	buf := w.Bytes()
	headerBuf := buf[0:binaryHeaderSize]
	crc := crc32.ChecksumIEEE(headerBuf[:len(headerBuf)-crcByteSize])
	conf.Endianness.PutUint32(headerBuf[len(headerBuf)-crcByteSize:], crc)
}

func putBodyLength(w BufferBackedWriter, value int) {
	const position = 8 // Position starting from the last
	buf := w.Bytes()
	headerBuf := buf[0:binaryHeaderSize]
	conf.Endianness.PutUint32(headerBuf[len(headerBuf)-position:], uint32(value))
}

func writeProducerRequest(w BufferBackedWriter, streamId uint16, body *binaryProducerBody) {
	const produceOp = 4
	// Header without body length and crc
	header := binaryHeader{
		Version:  1,
		Flags:    0,
		StreamId: streamId,
		Op:       produceOp,
	}

	_ = binary.Write(w, conf.Endianness, header)
	bodyLength := 0
	bodyLength += writeBinaryString(w, body.PartitionKey)
	bodyLength += writeBinaryString(w, body.Topic)
	for _, m := range body.Messages {
		_ = binary.Write(w, conf.Endianness, uint32(len(m)))
		_, _ = w.Write([]byte(m))
		bodyLength += 4 + len(m)
	}

	putBodyLength(w, bodyLength)
	putCrc32(w)
}

func writeBinaryString(w io.Writer, value string) int {
	_, _ = w.Write([]byte{byte(len(value))})
	n, _ := w.Write([]byte(value))
	return 1 + n
}


type binaryResponse interface {
	Op() uint8
}

type responseHandler func(binaryResponse)

func unmarshalResponse(header *binaryHeader, body []byte) (binaryResponse, error) {
	if len(body) == 0 {
		return &binaryEmptyResponse{op: header.Op}, nil
	}
	if header.Op == 3 {
		message := string(body[1:])
		return &binaryErrorResponse{
			code:    body[0],
			message: message,
		}, nil
	}
	return nil, fmt.Errorf("Unsupported op %d", header.Op)
}

type binaryEmptyResponse struct {
	op uint8
}

func (r *binaryEmptyResponse) Op() uint8 {
	return r.op
}

type binaryErrorResponse struct {
	code     byte
	message  string
}

func (r *binaryErrorResponse) Op() uint8 {
	return 3 // errorOp
}

func toError(r binaryResponse) error {
	if errorRes, ok  := r.(*binaryErrorResponse); ok {
		return fmt.Errorf("Response error %d: %s", errorRes.code, errorRes.message)
	}
	return nil
}