package consuming

import (
	"bytes"
	"encoding/binary"
	"io"
	"net"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/barcostreams/barco/internal/conf"
	"github.com/barcostreams/barco/internal/data"
	"github.com/barcostreams/barco/internal/types"
	. "github.com/barcostreams/barco/internal/types"
	"github.com/barcostreams/barco/internal/utils"
	"github.com/google/uuid"
	"github.com/karlseguin/jsonwriter"
	"github.com/klauspost/compress/zstd"
	"github.com/rs/zerolog/log"
)

// Represents a single consumer instance
type ConsumerInfo struct {
	Id         string            `json:"id"`    // A unique id within the consumer group
	Group      string            `json:"group"` // A group unique id
	Topics     []string          `json:"topics"`
	OnNewGroup OffsetResetPolicy `json:"onNewGroup"`

	// Only used internally
	assignedTokens []Token
}

type ReplicationReaderFactory interface {
	GetOrCreate(topic *TopicDataId, topology *TopologyInfo, topicGen *Generation, offsetState OffsetState) data.ReplicationReader
}

// Consumer response responseFormat
type responseFormat int

const (
	// Default poll response as described in `docs/developer/NETWORK_FORMATS.md`
	compressedBinaryFormat responseFormat = iota

	// A JSON formatted response
	jsonFormat
)

type segmentReadItem struct {
	chunkResult chan SegmentChunk
	errorResult chan error
	origin      string
	commitOnly  bool
}

func newSegmentReadItem(origin string, commitOnly bool) *segmentReadItem {
	return &segmentReadItem{
		chunkResult: make(chan SegmentChunk, 1),
		errorResult: make(chan error, 1),
		origin:      origin,
		commitOnly:  commitOnly,
	}
}

func (r *segmentReadItem) SetResult(err error, chunk SegmentChunk) {
	r.chunkResult <- chunk
	r.errorResult <- err
}

func (r *segmentReadItem) Origin() string {
	return r.origin
}

func (r *segmentReadItem) CommitOnly() bool {
	return r.commitOnly
}

func (r *segmentReadItem) result() (err error, chunk SegmentChunk) {
	return <-r.errorResult, <-r.chunkResult
}

type groupInfoBuilder struct {
	name       string
	topics     StringSet
	keys       StringSet
	onNewGroup OffsetResetPolicy
}

func newGroupInfoBuilder(group string, onNewGroup OffsetResetPolicy) *groupInfoBuilder {
	return &groupInfoBuilder{
		name:       group,
		topics:     make(map[string]bool),
		keys:       make(map[string]bool),
		onNewGroup: onNewGroup,
	}
}

// Represents a single response item from a poll request
type consumerResponseItem struct {
	chunk SegmentChunk
	topic TopicDataId
}

func (i *consumerResponseItem) Marshal(w io.Writer) error {
	// Can be extracted into "MarshalTopic"
	if err := binary.Write(w, conf.Endianness, i.topic.Token); err != nil {
		return err
	}
	if err := binary.Write(w, conf.Endianness, i.topic.RangeIndex); err != nil {
		return err
	}
	if err := binary.Write(w, conf.Endianness, i.topic.Version); err != nil {
		return err
	}
	if err := binary.Write(w, conf.Endianness, uint8(len(i.topic.Name))); err != nil {
		return err
	}
	if _, err := w.Write([]byte(i.topic.Name)); err != nil {
		return err
	}
	if err := binary.Write(w, conf.Endianness, i.chunk.StartOffset()); err != nil {
		return err
	}
	payload := i.chunk.DataBlock()
	if err := binary.Write(w, conf.Endianness, int32(len(payload))); err != nil {
		return err
	}
	if _, err := w.Write(payload); err != nil {
		return err
	}
	return nil
}

func (i *consumerResponseItem) MarshalJson(
	writer *jsonwriter.Writer,
	decoder *zstd.Decoder,
	decoderBuffer []byte,
) error {
	_ = decoder.Reset(bytes.NewReader(i.chunk.DataBlock()))
	writer.ArrayObject(func() {
		writer.KeyString("topic", i.topic.Name)
		// Use strings for int64 values
		writer.KeyString("token", i.topic.Token.String())
		writer.KeyInt("rangeIndex", int(i.topic.RangeIndex))
		writer.KeyInt("version", int(i.topic.Version))

		// Use strings for int64 values
		writer.KeyString("startOffset", strconv.FormatInt(i.chunk.StartOffset(), 10))
		writer.Array("values", func() {
			_ = writeJsonRecords(writer, decoder, decoderBuffer)
		})
	})

	return nil
}

// Writes records as JSON array items
func writeJsonRecords(
	writer *jsonwriter.Writer,
	reader *zstd.Decoder,
	readBuffer []byte,
) error {
	var header recordHeader
	for {
		if err := binary.Read(reader, conf.Endianness, &header); err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		writer.Separator()

		// TODO: Handle error
		_ = writeRecordBody(int(header.Length), writer, reader, readBuffer)
	}
}

func writeRecordBody(bodyLength int, writer *jsonwriter.Writer, reader *zstd.Decoder, readBuffer []byte) error {
	read := 0
	buf := readBuffer[0:utils.Min(bodyLength, len(readBuffer))]
	for read < bodyLength {
		n, err := reader.Read(buf)
		if n > 0 {
			if err = utils.WriteBytes(writer.W, buf[0:n]); err != nil {
				return err
			}
		}
		read += n
		if err != nil {
			if err != io.EOF {
				return err
			}
			break
		}
	}
	return nil
}

// Presents a map key for readers by token range
type readerKey struct {
	token       Token
	rangeIndex  RangeIndex
	clusterSize int
}

func newReaderKey(token Token, index RangeIndex, clusterSize int) readerKey {
	return readerKey{
		token:       token,
		rangeIndex:  index,
		clusterSize: clusterSize,
	}
}

type recordHeader struct {
	Timestamp int64
	Length    uint32
}

// Noop workaround for manual committing
// https://github.com/barcostreams/barco/issues/70
type ignoreResponse struct{}

func (i ignoreResponse) Header() http.Header {
	return http.Header{}
}

func (i ignoreResponse) Write(b []byte) (int, error) {
	return len(b), nil
}

func (i ignoreResponse) WriteHeader(statusCode int) {}

type trackedConsumer interface {
	types.Closer

	// Sets the last tracked read for deadlines
	SetAsRead()

	LastRead() time.Time

	// Returns the identifier of the consumer instance connection.
	// When using stateless-less flow, it matches the identifier of the consumer
	Id() string

	// Determines whether it has been closed
	IsClosed() bool
}

// Handles state to support both connection based consumers and stateless-less ones.
type trackedConsumerByConnection struct {
	conn     net.Conn
	lastRead int64
	id       string // Id of the connection
	isClosed atomic.Bool
}

func (c *trackedConsumerByConnection) Close() {
	_ = c.conn.Close()
	c.isClosed.Store(true)
}

func (c *trackedConsumerByConnection) SetAsRead() {
	atomic.StoreInt64(&c.lastRead, time.Now().UnixMilli())
}

func (c *trackedConsumerByConnection) LastRead() time.Time {
	return time.UnixMilli(atomic.LoadInt64(&c.lastRead))
}

func (c *trackedConsumerByConnection) Id() string {
	return c.id
}

func (c *trackedConsumerByConnection) IsClosed() bool {
	return c.isClosed.Load()
}

type trackedConsumerById struct {
	lastRead int64
	id       string // Id of the connection and the consumer
	isClosed atomic.Bool
}

func (c *trackedConsumerById) Close() {
	c.isClosed.Store(true)
}

func (c *trackedConsumerById) SetAsRead() {
	atomic.StoreInt64(&c.lastRead, time.Now().UnixMilli())
}

func (c *trackedConsumerById) LastRead() time.Time {
	return time.UnixMilli(atomic.LoadInt64(&c.lastRead))
}

func (c *trackedConsumerById) Id() string {
	return c.id
}

func (c *trackedConsumerById) IsClosed() bool {
	return c.isClosed.Load()
}

// Maintains state to support both connection based consumers and connection-less ones
type trackedConsumerHandler struct {
	value       atomic.Value
	initialConn net.Conn
}

func newTrackedConsumerHandler(initialConn net.Conn) *trackedConsumerHandler {
	return &trackedConsumerHandler{
		value:       atomic.Value{},
		initialConn: initialConn,
	}
}

func (c *trackedConsumerHandler) getValue() trackedConsumer {
	v := c.value.Load()
	if v == nil {
		return nil
	}
	return v.(trackedConsumer)
}

func (c *trackedConsumerHandler) Id() string {
	value := c.getValue()
	if value == nil {
		log.Error().Msgf("Tracked consumer id accessed before registering")
		return ""
	}
	return value.Id()
}

func (c *trackedConsumerHandler) IsTracked() bool {
	return c.getValue() != nil
}

func (c *trackedConsumerHandler) TrackAsStateless(id string) {
	// Force compile time check
	var value trackedConsumer = &trackedConsumerById{
		lastRead: time.Now().UnixMilli(),
		id:       id,
	}
	c.value.Store(value)
	c.initialConn = nil // Allow to be GC'ed
}

func (c *trackedConsumerHandler) IsStateless() bool {
	if v := c.getValue(); v != nil {
		_, ok := v.(*trackedConsumerById)
		return ok
	}
	return false
}

func (c *trackedConsumerHandler) LoadFromExisting(existing *trackedConsumerHandler) {
	existingValue := existing.getValue()
	if existingValue == nil {
		log.Error().Msgf("Unexpected existing tracked consumer without value")
		return
	}
	byId, ok := existingValue.(*trackedConsumerById)
	if !ok {
		log.Error().Msgf("Unexpected existing tracked consumer that is NOT by id: %+v", existingValue)
		return
	}
	c.value.Store(byId)
	c.initialConn = nil // Allow to be GC'ed
}

func (c *trackedConsumerHandler) TrackAsConnectionBound() {
	var value trackedConsumer = &trackedConsumerByConnection{
		conn:     c.initialConn,
		lastRead: time.Now().UnixMilli(),
		id:       uuid.New().String(),
	}
	c.value.Store(value)
}

func (c *trackedConsumerHandler) SetAsRead() {
	if value := c.getValue(); value != nil {
		value.SetAsRead()
	}
}

func (c *trackedConsumerHandler) LastRead() time.Time {
	if value := c.getValue(); value != nil {
		return value.LastRead()
	}
	return time.Time{}
}

func (c *trackedConsumerHandler) IsClosed() bool {
	if v := c.getValue(); v != nil {
		return v.IsClosed()
	}
	return false
}

// Handles closing the internal connection, when there's one
func (c *trackedConsumerHandler) Close() {
	if value := c.getValue(); value != nil {
		value.Close()
	}
}
