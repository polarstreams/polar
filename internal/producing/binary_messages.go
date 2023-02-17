package producing

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"

	"github.com/polarstreams/polar/internal/conf"
	. "github.com/polarstreams/polar/internal/types"
	"github.com/polarstreams/polar/internal/utils"
)

type opcode uint8
type streamId uint16
type flags uint8
type errorCode uint8

const messageVersion = 1

// Operation codes.
// Use fixed numbers (not iota) to make it harder to break the protocol by moving stuff around.
const (
	startupOp         opcode = 1
	readyOp           opcode = 2
	errorOp           opcode = 3
	produceOp         opcode = 4
	produceResponseOp opcode = 5
	heartbeatOp       opcode = 6
)

// Flags.
// Use fixed numbers (not iota) to make it harder to break the protocol by moving stuff around.
const (
	withTimestamp flags = 0b00000001
)

const (
	serverError         errorCode = 0
	routingError        errorCode = 1
	leaderNotFoundError errorCode = 2
)

// Header for producer messages. Order of fields defines the serialization format.
type binaryHeader struct {
	Version    uint8
	Flags      flags
	StreamId   streamId
	Op         opcode
	BodyLength uint32
	Crc        uint32
}

var binaryHeaderSize = utils.BinarySize(binaryHeader{})

type binaryResponse interface {
	Marshal(w BufferBackedWriter) error
	BodyLength() int
}

type emptyResponse struct {
	streamId streamId
	op       opcode
}

func (r *emptyResponse) Marshal(w BufferBackedWriter) error {
	return writeHeader(w, &binaryHeader{
		Version:    messageVersion,
		StreamId:   r.streamId,
		Op:         r.op,
		Flags:      0,
		BodyLength: 0,
		Crc:        0,
	})
}

func (r *emptyResponse) BodyLength() int {
	return 0
}

type errorResponse struct {
	streamId streamId
	code     errorCode
	message  string
}

func (r *errorResponse) Marshal(w BufferBackedWriter) error {
	if err := writeHeader(w, &binaryHeader{
		Version:    messageVersion,
		StreamId:   r.streamId,
		Op:         errorOp,
		BodyLength: uint32(r.BodyLength()),
	}); err != nil {
		return err
	}

	if _, err := w.Write([]byte{byte(r.code)}); err != nil {
		return err
	}

	_, err := w.Write([]byte(r.message))
	return err
}

func (r *errorResponse) BodyLength() int {
	return len(r.message) + 1
}

func writeHeader(w BufferBackedWriter, header *binaryHeader) error {
	if err := binary.Write(w, conf.Endianness, header); err != nil {
		return err
	}

	const crcByteSize = 4
	buf := w.Bytes()
	headerBuf := buf[len(buf)-binaryHeaderSize:]
	crc := crc32.ChecksumIEEE(headerBuf[:len(headerBuf)-crcByteSize])
	conf.Endianness.PutUint32(headerBuf[len(headerBuf)-crcByteSize:], crc)
	return nil
}

// Generic error response
func newErrorResponse(message string, requestHeader *binaryHeader) binaryResponse {
	return &errorResponse{
		message:  message,
		streamId: requestHeader.StreamId,
		code:     serverError,
	}
}

func newRoutingErrorResponse(err error, requestHeader *binaryHeader) binaryResponse {
	return &errorResponse{
		message:  err.Error(),
		streamId: requestHeader.StreamId,
		code:     routingError,
	}
}

func newLeaderNotFoundErrorResponse(token Token, requestHeader *binaryHeader) binaryResponse {
	return &errorResponse{
		message:  fmt.Sprintf("Leader for token %d could not be found", token),
		streamId: requestHeader.StreamId,
		code:     leaderNotFoundError,
	}
}
