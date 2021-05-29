package interbroker

import (
	"bufio"
	"encoding/binary"
	"io"

	"github.com/jorgebay/soda/internal/conf"
	"github.com/jorgebay/soda/internal/types"
)

type opcode uint8
type streamId uint16

const (
	startupOp opcode = iota
	readyOp
	errorOp
	dataOp
	dataResponseOp
)

// header is the interbroker message header
type header struct {
	// Strict ordering
	Version    uint8
	StreamId   streamId
	Op         opcode
	BodyLength uint32
	// TODO: Add CRC to header
}

const headerSize = 1 + // version
	2 + // stream id
	1 + // op
	4 // length

type dataRequestMeta struct {
	// Strict ordering
	segmentId int64
	token     types.Token
	genId     uint16
	// topicLength is the size in bytes of the topic name
	topicLength uint8
}

const dataRequestMetaSize = 8 + // segment id
	8 + // token
	2 + // genId
	1 // topicLength

type dataRequest struct {
	meta     dataRequestMeta
	topic    string
	data     []byte
	response chan dataResponse
}

func (r *dataRequest) BodyLength() uint32 {
	return uint32(dataRequestMetaSize) + uint32(r.meta.topicLength) + uint32(len(r.data))
}

func (r *dataRequest) Marshal(w *bufio.Writer, header *header) {
	writeHeader(w, header)
	binary.Write(w, conf.Endianness, r.meta)
	w.WriteString(r.topic)
	w.Write(r.data)
}

type dataResponse interface {
	Marshal(w io.Writer) error
}

type errorResponse struct {
	message  string
	streamId streamId
}

func newErrorResponse(message string, requestHeader *header) *errorResponse {
	return &errorResponse{message: message, streamId: requestHeader.StreamId}
}

// Deserializes an error response into a buffer
func (r *errorResponse) Marshal(w io.Writer) error {
	message := []byte(r.message)
	if err := writeHeader(w, &header{
		Version:    1,
		StreamId:   r.streamId,
		Op:         errorOp,
		BodyLength: uint32(len(message)),
	}); err != nil {
		return err
	}

	_, err := w.Write(message)
	return err
}

func writeHeader(w io.Writer, header *header) error {
	return binary.Write(w, conf.Endianness, header)
}

type emptyResponse struct {
	streamId streamId
	op       opcode
}

func (r *emptyResponse) Marshal(w io.Writer) error {
	return writeHeader(w, &header{
		Version:    1,
		StreamId:   r.streamId,
		Op:         r.op,
		BodyLength: 0,
	})
}
