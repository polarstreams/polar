package producing

import (
	"bytes"
	"encoding/binary"
	"io"
	"time"

	"github.com/jorgebay/soda/internal/conf"
	"github.com/jorgebay/soda/internal/data"
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
	items     chan *record
	dataItems chan *dataItem
	topic     string
	config    conf.ProducerConfig
	appender  data.Appender
	gossiper  interbroker.Replicator
	offset    uint64
	buffers   buffers
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

type dataItem struct {
	data  []byte
	group []record
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

func newCoalescer(topic string, config conf.ProducerConfig, appender data.Appender, gossiper interbroker.Replicator) *coalescer {
	c := &coalescer{
		items: make(chan *record, 0),
		// Limit's to 1 outstanding write (the current one)
		// The next group can be generated while the previous is being flushed and sent
		dataItems: make(chan *dataItem, 0),
		topic:     topic,
		config:    config,
		appender:  appender,
		gossiper:  gossiper,
		offset:    0, // TODO: Set the initial offset
		buffers:   newBuffers(config),
	}
	// Start receiving in the background
	go c.receive()
	go c.flushTimer()
	return c
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
		c.dataItems <- &dataItem{
			data:  data,
			group: group,
		}
	}
}

func (c *coalescer) appendAndSendInLoop() {
	for item := range c.dataItems {
		// TODO: Maybe panic on flush err
		c.maybeFlushSegment()
		if item == nil {
			// It was only a signal to flush the segment, no data, move on
			continue
		}
		segmentId, err := c.writeToSegmentBuffer(item.data)
		if err != nil {
			sendResponse(item.group, err)
			continue
		}
		// Response channel should be buffered in case the response is discarded
		response := make(chan error, 1)

		// Start sending in the background while flushing is occurring
		go c.send(segmentId, item.data, item.group, response)

		// when it doesn't fit or time has passed since last flush
		if err = c.maybeFlushSegment(); err != nil {
			sendResponse(item.group, err)
			continue
		}
		sendResponse(item.group, <-response)
	}
}

func (c *coalescer) flushTimer() {
	const resolution = 200 * time.Millisecond
	for {
		time.Sleep(resolution)
		// Send a nil data item as an indication of a flush message
		c.dataItems <- nil
	}
}

func (c *coalescer) maybeFlushSegment() error {
	return nil
}

func (c *coalescer) writeToSegmentBuffer(data []byte) (segmentId int64, err error) {
	return 0, nil
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

func (c *coalescer) send(segmentId int64, block []byte, group []record, response chan error) {

	//TODO: Implement

	// if err := p.gossiper.SendToFollowers(replicationInfo, topic, body); err != nil {
	// 	return err
	// }
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

func sendResponse(group []record, err error) {
	for _, r := range group {
		r.response <- err
	}
}
