package producing

import (
	"bytes"
	"encoding/binary"
	"io"

	"github.com/jorgebay/soda/internal/conf"
	"github.com/jorgebay/soda/internal/interbroker"
	"github.com/jorgebay/soda/internal/types"
	"github.com/klauspost/compress/zstd"
	"github.com/rs/zerolog/log"
)

// The coalescer allows 2 goroutines to be compressing/appending/sending
// at any time (in an orderly manner)
const writeConcurrencyLevel = 2

var lengthBuffer []byte = []byte{0, 0, 0, 0}

type coalescer struct {
	items    chan *record
	topic    string
	config   conf.ProducerConfig
	gossiper interbroker.Replicator
	offset   uint64
	buffers  buffers
	segment  *segmentWriter
}

type record struct {
	replication types.ReplicationInfo
	length      int64
	body        io.ReadCloser
	offset      uint64
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
		b := bytes.NewBuffer(make([]byte, 0, initCapacity))
		compressor, _ := zstd.NewWriter(b, zstd.WithEncoderCRC(true), zstd.WithEncoderLevel(zstd.SpeedDefault))
		result.group[i] = b
		result.compressor[i] = compressor
	}
	return result
}

func newCoalescer(
	topic string,
	token types.Token,
	genId string,
	config conf.ProducerConfig,
	gossiper interbroker.Replicator,
) (*coalescer, error) {
	s, err := newSegmentWriter(topic, token, genId, config)

	if err != nil {
		return nil, err
	}

	c := &coalescer{
		items:    make(chan *record, 0),
		topic:    topic,
		config:   config,
		gossiper: gossiper,
		offset:   0, // TODO: Set the initial offset
		buffers:  newBuffers(config),
		segment:  s,
	}
	// Start receiving in the background
	go c.receive()
	return c, nil
}

func (c *coalescer) add(group []record, item *record, length *int64) ([]record, *record) {
	if *length+item.length > int64(c.config.MaxGroupSize()) {
		// Return a non-nil record as a signal that it was not appended
		return group, item
	}
	*length += item.length
	item.offset = c.offset
	c.offset++
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

		canAddNext := true
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
			default:
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

		// Send in the background while the next block is generated in the foreground
		c.segment.items <- &dataItem{
			data:  data,
			group: group,
		}
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

	// Save space for the length of the block
	buf.Write(lengthBuffer)

	for _, item := range group {
		if _, err := io.Copy(compressor, item.body); err != nil {
			return nil, err
		}
	}

	if err := compressor.Close(); err != nil {
		return nil, err
	}

	b := buf.Bytes()
	// Write the length of the whole block
	binary.BigEndian.PutUint32(b, uint32(len(b)-4))

	return b, nil
}

func (c *coalescer) append(replication types.ReplicationInfo, length int64, body io.ReadCloser) error {
	record := &record{
		replication: replication,
		length:      length,
		body:        body,
		response:    make(chan error, 1),
	}
	c.items <- record
	return <-record.response
}
