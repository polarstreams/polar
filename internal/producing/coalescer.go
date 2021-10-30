package producing

import (
	"bytes"
	"encoding/binary"
	"io"
	"time"

	"github.com/jorgebay/soda/internal/conf"
	"github.com/jorgebay/soda/internal/data"
	"github.com/jorgebay/soda/internal/metrics"
	"github.com/jorgebay/soda/internal/types"
	"github.com/jorgebay/soda/internal/utils"
	"github.com/klauspost/compress/zstd"
	"github.com/rs/zerolog/log"
)

// The coalescer allows 2 goroutines to be compressing/appending/sending
// at any time (in an orderly manner)
const writeConcurrencyLevel = 2

var lengthBuffer []byte = []byte{0, 0, 0, 0}

type coalescer struct {
	items      chan *record
	topic      types.TopicDataId
	config     conf.ProducerConfig
	replicator types.Replicator
	offset     uint64
	buffers    buffers
	segment    *data.SegmentWriter
}

type record struct {
	replication types.ReplicationInfo
	length      uint32 // Body length
	timestamp   int64  // Timestamp in micros
	body        io.ReadCloser
	offset      uint64 // Record offset
	response    chan error
}

type buffers struct {
	group      [writeConcurrencyLevel]*bytes.Buffer
	compressor [writeConcurrencyLevel]*zstd.Encoder
}

func newBuffers(config conf.ProducerConfig) buffers {
	// We pre-allocate the compression buffers
	initCapacity := config.MaxGroupSize() / 32
	result := buffers{
		group:      [writeConcurrencyLevel]*bytes.Buffer{},
		compressor: [writeConcurrencyLevel]*zstd.Encoder{},
	}
	for i := 0; i < writeConcurrencyLevel; i++ {
		b := utils.NewBufferCap(initCapacity)
		compressor, _ := zstd.NewWriter(b, zstd.WithEncoderCRC(true), zstd.WithEncoderLevel(zstd.SpeedDefault))
		result.group[i] = b
		result.compressor[i] = compressor
	}
	return result
}

func newCoalescer(
	topic types.TopicDataId,
	config conf.ProducerConfig,
	replicator types.Replicator,
) (*coalescer, error) {
	s, err := data.NewSegmentWriter(topic, replicator, config, 0)

	if err != nil {
		return nil, err
	}

	c := &coalescer{
		items:      make(chan *record, 0),
		topic:      topic,
		config:     config,
		replicator: replicator,
		offset:     0, // TODO: Set the initial offset
		buffers:    newBuffers(config),
		segment:    s,
	}
	// Start receiving in the background
	go c.receive()
	return c, nil
}

func (c *coalescer) add(group []record, item *record, length *int64) ([]record, *record) {
	itemLength := int64(item.length)
	if *length+itemLength > int64(c.config.MaxGroupSize()) {
		// Return a non-nil record as a signal that it was not appended
		return group, item
	}
	*length += itemLength
	item.offset = c.offset
	item.timestamp = time.Now().UnixMicro()
	c.offset++
	metrics.CoalescerMessagesProcessed.Inc()
	group = append(group, *item)
	return group, nil
}

func (c *coalescer) receive() {
	var item *record = nil
	var index uint8
	for {
		group := make([]record, 0)
		length := int64(0)

		// Block receiving the first item or when there isn't a buffered item
		if item == nil {
			item = <-c.items
		}

		// Either there was a buffered item or we just received it
		group, _ = c.add(group, item, &length)
		item = nil

		canAddNext := true
		timeout := time.After(100 * time.Microsecond)
		for canAddNext {
			// Receive without blocking until there are no more items
			// or the max length for a group was reached
			select {
			case item = <-c.items:
				group, item = c.add(group, item, &length)
				if item != nil {
					// The group can't contain the new item
					canAddNext = false
				}
			case <-timeout:
				canAddNext = false
			}
		}

		data, err := c.compress(&index, group)

		if err != nil {
			log.Err(err).Msg("Unexpected compression error")
			sendResponse(group, err)
			// The group will not be persisted, reset the offset
			c.offset = group[0].offset
			continue
		}

		metrics.CoalescerMessagesPerGroup.Observe(float64(len(group)))

		// Send in the background while the next block is generated in the foreground
		c.segment.Items <- &localDataItem{
			data:  data,
			group: group,
		}
	}
}

func sendResponse(group []record, err error) {
	for _, r := range group {
		r.response <- err
	}
}

func (c *coalescer) compress(index *uint8, group []record) ([]byte, error) {
	i := *index % 2
	*index = *index + 1
	buf := c.buffers.group[i]
	buf.Reset()
	compressor := c.buffers.compressor[i]
	// Compressor writter needs to be reinitialized each time
	compressor.Reset(buf)

	for _, item := range group {
		// Record's bodyLength+timestamp
		if err := binary.Write(compressor, conf.Endianness, item.length); err != nil {
			return nil, err
		}
		if err := binary.Write(compressor, conf.Endianness, item.timestamp); err != nil {
			return nil, err
		}

		// Record's body
		if _, err := io.Copy(compressor, item.body); err != nil {
			return nil, err
		}
	}

	if err := compressor.Close(); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func (c *coalescer) append(replication types.ReplicationInfo, length uint32, body io.ReadCloser) error {
	record := &record{
		replication: replication,
		length:      length,
		body:        body,
		response:    make(chan error, 1),
	}
	c.items <- record
	return <-record.response
}

type localDataItem struct {
	data  []byte   // compressed payload of the chunk
	group []record // records associated with this chunk
}

// DataBlock() gets the compressed payload of the chunk
func (d *localDataItem) DataBlock() []byte {
	return d.data
}

func (d *localDataItem) Replication() types.ReplicationInfo {
	// TODO: Maybe simplify, 1 replication info per generation
	return d.group[0].replication
}

func (d *localDataItem) StartOffset() uint64 {
	return d.group[0].offset
}

func (d *localDataItem) RecordLength() uint32 {
	return uint32(len(d.group))
}

func (d *localDataItem) SetResult(err error) {
	for _, r := range d.group {
		r.response <- err
	}
}
