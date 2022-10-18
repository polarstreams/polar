package consuming

import (
	"bytes"
	"encoding/binary"
	"io"
	"net"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/barcostreams/barco/internal/conf"
	"github.com/barcostreams/barco/internal/data"
	. "github.com/barcostreams/barco/internal/types"
	"github.com/barcostreams/barco/internal/utils"
	"github.com/google/uuid"
	"github.com/karlseguin/jsonwriter"
	"github.com/klauspost/compress/zstd"
	"github.com/rs/zerolog/log"
)

// Represents a single consumer instance
type ConsumerInfo struct {
	Id     string   `json:"id"`    // A unique id within the consumer group
	Group  string   `json:"group"` // A group unique id
	Topics []string `json:"topics"`

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
	decoder.Reset(bytes.NewReader(i.chunk.DataBlock()))
	writer.ArrayObject(func() {
		writer.KeyString("topic", i.topic.Name)
		// Use strings for int64 values
		writer.KeyString("token", i.topic.Token.String())
		writer.KeyInt("rangeIndex", int(i.topic.RangeIndex))
		writer.KeyInt("version", int(i.topic.Version))

		// Use strings for int64 values
		writer.KeyString("startOffset", strconv.FormatInt(i.chunk.StartOffset(), 10))
		writer.Array("values", func() {
			writeJsonRecords(writer, decoder, decoderBuffer)
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
		writeRecordBody(int(header.Length), writer, reader, readBuffer)
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
	token      Token
	rangeIndex RangeIndex
}

type recordHeader struct {
	Timestamp int64
	Length    uint32
}

// Handles state to support both connection based consumers and connection-less ones.
type trackedConsumer struct {
	conn             atomic.Value // For connection bound consumers
	initialConn      net.Conn
	lastRead         int64        // Used to follow last read from consumer
	id               atomic.Value // The id representing the connection
}

func newTrackedConsumer(initialConn net.Conn) *trackedConsumer {
	return &trackedConsumer{
		conn:             atomic.Value{},
		id:               atomic.Value{},
		initialConn:      initialConn,
		lastRead:         time.Now().UnixMilli(),
	}
}

// Returns the identifier of the consumer instance connection.
// When using connection-less flow, it matches the identifier of the consumer
func (c *trackedConsumer) Id() string {
	id := c.id.Load()
	if id == nil {
		log.Error().Msgf("Tracked consumer id accessed before registering")
		return ""
	}
	return id.(string)
}

// Determines whether this consumer has been registered
func (c *trackedConsumer) Registered() bool {
	return c.id.Load() != nil
}

func (c *trackedConsumer) RegisterAsConnectionLess(id string) {
	c.id.Store(id)
	c.initialConn = nil // Allow to be GC'ed
}

func (c *trackedConsumer) RegisterAsConnectionBound() {
	c.id.Store(uuid.New().String())
	c.conn.Store(c.initialConn)
}

// Sets the last tracked read for deadlines
func (c *trackedConsumer) SetAsRead() {
	atomic.StoreInt64(&c.lastRead, time.Now().UnixMilli())
}

// Gets the last tracked read for read timeouts
func (c *trackedConsumer) LastRead() time.Time {
	return time.UnixMilli(atomic.LoadInt64(&c.lastRead))
}

// Determines whether the interaction with this consumer is based on the connection state
func (c *trackedConsumer) IsConnectionBound() bool {
	return c.conn.Load() != nil
}

// Closes the net connection, when connection exists
func (c *trackedConsumer) CloseConnection() bool {
	if conn := c.conn.Load(); conn != nil {
		_ = conn.(net.Conn).Close()
		return true
	}
	return false
}
