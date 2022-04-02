package interbroker

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"io"

	"github.com/barcostreams/barco/internal/conf"
	"github.com/barcostreams/barco/internal/types"
	"github.com/barcostreams/barco/internal/utils"
)

type opcode uint8
type streamId uint16

// Operation codes.
// Use fixed numbers (not iota) to make it harder to break the protocol by moving stuff around.
const (
	startupOp                  opcode = 1
	readyOp                    opcode = 2
	errorOp                    opcode = 3
	chunkReplicationOp         opcode = 4
	chunkReplicationResponseOp opcode = 5
	fileStreamOp               opcode = 6
	fileStreamResponseOp       opcode = 7
)

const messageVersion = 1

type dataRequest interface {
	BodyLength() uint32
	Ctxt() context.Context
	Marshal(w types.StringWriter, header *header)
	SetResponse(res dataResponse)
}

type dataResponse interface {
	Marshal(w io.Writer) error
}

// header is the interbroker message header
type header struct {
	// Strict ordering, exported fields
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
	// Strict ordering, exported fields
	SegmentId    int64
	Token        types.Token
	RangeIndex   types.RangeIndex
	GenVersion   types.GenVersion
	StartOffset  int64
	RecordLength uint32
	TopicLength  uint8 // The size in bytes of the topic name
}

var dataRequestMetaSize = utils.BinarySize(dataRequestMeta{})

type chunkReplicationRequest struct {
	meta         dataRequestMeta
	topic        string
	data         []byte
	response     chan dataResponse // response from replica
	appendResult chan error        // result from append as a replica
	ctxt         context.Context
}

func (r *chunkReplicationRequest) BodyLength() uint32 {
	return uint32(dataRequestMetaSize) + uint32(r.meta.TopicLength) + uint32(len(r.data))
}

func (r *chunkReplicationRequest) DataBlock() []byte {
	return r.data
}

func (r *chunkReplicationRequest) SegmentId() int64 {
	return r.meta.SegmentId
}

func (r *chunkReplicationRequest) StartOffset() int64 {
	return r.meta.StartOffset
}

func (r *chunkReplicationRequest) RecordLength() uint32 {
	return r.meta.RecordLength
}

func (r *chunkReplicationRequest) SetResult(err error) {
	r.appendResult <- err
}

func (r *chunkReplicationRequest) Ctxt() context.Context {
	return r.ctxt
}

func (r *chunkReplicationRequest) SetResponse(res dataResponse) {
	r.response <- res
}

func (r *chunkReplicationRequest) Marshal(w types.StringWriter, header *header) {
	header.Op = chunkReplicationOp

	writeHeader(w, header)
	binary.Write(w, conf.Endianness, r.meta)
	w.WriteString(r.topic)
	w.Write(r.data)
}

func (r *chunkReplicationRequest) topicId() types.TopicDataId {
	return types.TopicDataId{
		Name:       r.topic,
		Token:      r.meta.Token,
		Version:    r.meta.GenVersion,
		RangeIndex: r.meta.RangeIndex,
	}
}

// Represents a request message to stream a file starting from offset.
type fileStreamRequest struct {
	meta     dataRequestMeta
	topic    string
	fileName string
	response chan dataResponse
	ctxt     context.Context
}

func (r *fileStreamRequest) Marshal(w types.StringWriter, header *header) {
	header.Op = fileStreamOp
	writeHeader(w, header)
	binary.Write(w, conf.Endianness, r.meta)
	w.WriteString(r.topic)
}

func (r *fileStreamRequest) Ctxt() context.Context {
	return r.ctxt
}

func (r *fileStreamRequest) BodyLength() uint32 {
	return uint32(dataRequestMetaSize) + uint32(r.meta.TopicLength)
}

func (r *fileStreamRequest) SetResponse(res dataResponse) {
	r.response <- res
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
		Version:    messageVersion,
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

func readHeader(buffer []byte) (*header, error) {
	header := &header{}
	err := binary.Read(bytes.NewReader(buffer), conf.Endianness, header)
	return header, err
}

type emptyResponse struct {
	streamId streamId
	op       opcode
}

func (r *emptyResponse) Marshal(w io.Writer) error {
	return writeHeader(w, &header{
		Version:    messageVersion,
		StreamId:   r.streamId,
		Op:         r.op,
		BodyLength: 0,
	})
}

func unmarshalResponse(header *header, body []byte) dataResponse {
	if header.Op == errorOp {
		return newErrorResponse(string(body), header)
	}
	return &emptyResponse{
		streamId: header.StreamId,
		op:       header.Op,
	}
}

type fileStreamResponse struct {
	// Fields for server
	streamId streamId
	op       opcode

	// Field for both the client and the server
	bodyLength uint32

	// For the client
	reader *bufio.Reader
}

func (r *fileStreamResponse) Marshal(w io.Writer) error {
	return writeHeader(w, &header{
		Version:    messageVersion,
		StreamId:   r.streamId,
		Op:         r.op,
		BodyLength: r.bodyLength,
	})
}

func unmarshalFileStreamResponse(header *header, r *bufio.Reader) dataResponse {
	res := &fileStreamResponse{
		streamId:   header.StreamId,
		op:         header.Op,
		bodyLength: header.BodyLength,
		reader:     r, // Store the reader to get the data
	}
	return res
}

// decodes into a data request (without response channel)
func unmarshalDataRequest(body []byte) (*chunkReplicationRequest, error) {
	meta := dataRequestMeta{}
	reader := bytes.NewReader(body)
	if err := binary.Read(reader, conf.Endianness, &meta); err != nil {
		return nil, err
	}

	index := dataRequestMetaSize
	topic := string(body[index : index+int(meta.TopicLength)])
	index += int(meta.TopicLength)
	request := &chunkReplicationRequest{
		meta:  meta,
		topic: topic,
		data:  body[index:],
	}

	return request, nil
}
