package interbroker

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
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
	Ctxt() context.Context
	Marshal(w types.BufferBackedWriter, header *header)
	SetResponse(res dataResponse) error
	BodyLength() uint32
}

type dataResponse interface {
	Marshal(w types.BufferBackedWriter) error
	BodyLength() uint32
	BodyBuffer() []byte // When set, it hints that the body will not be marshalled and the buffer should be used instead
	ReleaseBuffer()     // For buffered responses, it marks the body buffer for reuse
}

// header is the interbroker message header
type header struct {
	// Strict ordering, exported fields
	Version    uint8
	StreamId   streamId
	Op         opcode
	BodyLength uint32
	Crc        uint32
}

var headerSize = utils.BinarySize(header{})

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

func (r *chunkReplicationRequest) SetResponse(res dataResponse) error {
	r.response <- res
	return nil
}

func (r *chunkReplicationRequest) Marshal(w types.BufferBackedWriter, header *header) {
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
	// Field for both the client and the server
	meta    dataRequestMeta
	maxSize uint32 // The maximum amount of bytes to be streamed in a single response
	topic   string

	// Fields exclusively used by the client
	response    chan dataResponse // channel for the response for the client
	responseBuf []byte            // The buffer to be filled with the response body
	ctxt        context.Context
}

func (r *fileStreamRequest) Marshal(w types.BufferBackedWriter, header *header) {
	header.Op = fileStreamOp
	writeHeader(w, header)
	binary.Write(w, conf.Endianness, r.meta)
	binary.Write(w, conf.Endianness, r.maxSize)
	w.WriteString(r.topic)
}

func (r *fileStreamRequest) Ctxt() context.Context {
	return r.ctxt
}

func (r *fileStreamRequest) BodyLength() uint32 {
	return uint32(dataRequestMetaSize) +
		uint32(r.meta.TopicLength) +
		4 // uint32 for max size
}

func (r *fileStreamRequest) topicId() types.TopicDataId {
	return types.TopicDataId{
		Name:       r.topic,
		Token:      r.meta.Token,
		Version:    r.meta.GenVersion,
		RangeIndex: r.meta.RangeIndex,
	}
}

func (r *fileStreamRequest) SetResponse(res dataResponse) error {
	fileResponse, ok := res.(*fileStreamResponse)
	if ok {
		// We need to consume the reader here in the foreground
		// Use the
		buf := r.responseBuf
		if int(fileResponse.bodyLength) < len(r.responseBuf) {
			buf = buf[:fileResponse.bodyLength]
		}
		n, err := io.ReadFull(fileResponse.reader, buf)
		if err != nil {
			return err
		}
		fileResponse.readBytes = n
	}

	r.response <- res
	return nil
}

type errorResponse struct {
	message  string
	streamId streamId
}

func newErrorResponse(message string, requestHeader *header) *errorResponse {
	return &errorResponse{message: message, streamId: requestHeader.StreamId}
}

// Deserializes an error response into a buffer
func (r *errorResponse) Marshal(w types.BufferBackedWriter) error {
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

func (r *errorResponse) BodyLength() uint32 {
	return uint32(len(r.message))
}

func (r *errorResponse) BodyBuffer() []byte {
	return nil
}

func (r *errorResponse) ReleaseBuffer() {}

func writeHeader(w types.BufferBackedWriter, header *header) error {
	if err := binary.Write(w, conf.Endianness, header); err != nil {
		return err
	}

	const crcByteSize = 4
	buf := w.Bytes()
	headerBuf := buf[len(buf)-headerSize:]
	crc := crc32.ChecksumIEEE(headerBuf[:len(headerBuf)-crcByteSize])
	conf.Endianness.PutUint32(headerBuf[len(headerBuf)-crcByteSize:], crc)
	return nil
}

func readHeader(buffer []byte) (*header, error) {
	header := &header{}
	if err := binary.Read(bytes.NewReader(buffer), conf.Endianness, header); err != nil {
		return nil, err
	}
	const crcByteSize = 4
	expectedCrc := crc32.ChecksumIEEE(buffer[:headerSize-crcByteSize])
	if header.Crc != expectedCrc {
		return nil, fmt.Errorf("Checksum mismatch")
	}
	return header, nil
}

type emptyResponse struct {
	streamId streamId
	op       opcode
}

func (r *emptyResponse) Marshal(w types.BufferBackedWriter) error {
	return writeHeader(w, &header{
		Version:    messageVersion,
		StreamId:   r.streamId,
		Op:         r.op,
		BodyLength: 0,
	})
}

func (r *emptyResponse) BodyLength() uint32 {
	return 0
}

func (r *emptyResponse) BodyBuffer() []byte {
	return nil
}

func (r *emptyResponse) ReleaseBuffer() {}

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
	// Field for both the client and the server
	streamId   streamId
	op         opcode
	bodyLength uint32

	// For the client
	reader    *bufio.Reader
	readBytes int

	// For the server
	buf            []byte
	releaseHandler func()
}

func (r *fileStreamResponse) Marshal(w types.BufferBackedWriter) error {
	return writeHeader(w, &header{
		Version:    messageVersion,
		StreamId:   r.streamId,
		Op:         r.op,
		BodyLength: r.bodyLength,
	})
}

func (r *fileStreamResponse) BodyLength() uint32 {
	return r.bodyLength
}

func (r *fileStreamResponse) BodyBuffer() []byte {
	return r.buf
}

func (r *fileStreamResponse) ReleaseBuffer() {
	r.releaseHandler()
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

func unmarshalFileStreamRequest(body []byte) (*fileStreamRequest, error) {
	request := fileStreamRequest{}
	reader := bytes.NewReader(body)
	if err := binary.Read(reader, conf.Endianness, &request.meta); err != nil {
		return nil, err
	}

	if err := binary.Read(reader, conf.Endianness, &request.maxSize); err != nil {
		return nil, err
	}

	topic := make([]byte, request.meta.TopicLength)
	if _, err := reader.Read(topic); err != nil {
		return nil, err
	}
	request.topic = string(topic)
	return &request, nil
}
