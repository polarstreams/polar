package producing

import (
	"bytes"
	"time"

	"github.com/polarstreams/polar/internal/conf"
	"github.com/polarstreams/polar/internal/data"
	"github.com/polarstreams/polar/internal/discovery"
	"github.com/polarstreams/polar/internal/metrics"
	"github.com/polarstreams/polar/internal/types"
	"github.com/polarstreams/polar/internal/utils"
	"github.com/klauspost/compress/zstd"
	"github.com/rs/zerolog/log"
)

// The coalescer allows 2 goroutines to be compressing/appending/sending
// at any time (in an orderly manner)
const writeConcurrencyLevel = 2

// Groups records into compressed chunks and dispatches them in order
// to the segment writers.
//
// There should be one instance of the coalescer per topic+token (regardless of the
// generation).
// The coalescer is responsible for managing the lifetime of the segment writers.
type coalescer struct {
	items           chan *recordItem
	topicName       string
	token           types.Token
	rangeIndex      types.RangeIndex
	generationState discovery.TopologyGetter
	replicator      types.Replicator
	config          conf.ProducerConfig
	offset          int64
	buffers         coalescerBuffers
	writer          *data.SegmentWriter
}

func newBuffers(config conf.ProducerConfig) coalescerBuffers {
	// We pre-allocate the compression buffers
	initCapacity := config.MaxGroupSize() / 32
	result := coalescerBuffers{
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
	topicName string,
	token types.Token,
	rangeIndex types.RangeIndex,
	generationState discovery.TopologyGetter,
	replicator types.Replicator,
	config conf.ProducerConfig,
) *coalescer {
	c := &coalescer{
		items:           make(chan *recordItem),
		topicName:       topicName,
		token:           token,
		rangeIndex:      rangeIndex,
		generationState: generationState,
		replicator:      replicator,
		config:          config,
		offset:          0,
		buffers:         newBuffers(config),
		writer:          nil,
	}
	// Start processing in the background
	go c.process()
	return c
}

func (c *coalescer) process() {
	var item *recordItem = nil
	var bufferIndex uint8
	for {
		group := newCoalescerGroup(c.offset, c.config.MaxGroupSize())
		var err error

		// Block receiving the first item or when there isn't a buffered item
		if item == nil {
			item = <-c.items
		}

		gen := c.generationState.Generation(c.token)

		if gen == nil {
			log.Error().Msgf("Coalescer was not able to retrieve generation for token %d", c.token)
			item.response <- types.NewNoWriteAttemptedError("Coalescer was not able to retrieve generation")
			item = nil
			continue
		}

		if gen.Leader != c.generationState.Topology().MyOrdinal() {
			item.response <- types.NewNoWriteAttemptedError("Coalescer can not write as it's no longer the leader")
			item = nil
			continue
		}

		if c.writer == nil {
			// Use generation to create a new writer
			topic := types.TopicDataId{
				Name:       c.topicName,
				Token:      c.token,
				RangeIndex: c.rangeIndex,
				Version:    types.GenVersion(gen.Version),
			}
			if c.writer, err = data.NewSegmentWriter(topic, c.replicator, c.config, nil); err != nil {
				log.Err(err).Msg("There was an error while creating the segment writer")
				item.response <- types.NewNoWriteAttemptedError("SegmentWriter could not be created")
				item = nil
				continue
			}
		}

		if c.writer.Topic.Version != types.GenVersion(gen.Version) {
			// There was a generational change since the last write
			// Close existing writer and open a new one
			close(c.writer.Items)
			c.writer = nil

			// For the new generation, we start at zero
			c.offset = 0

			// Reuse the writer setup logic, the current item will be handled in the next iteration
			continue
		}

		// Either there was a buffered item or we just received it
		item = group.tryAdd(item)

		canAddNext := true
		timeout := time.After(100 * time.Microsecond)
		for canAddNext {
			// Receive without blocking until there are no more items
			// or the max length for a group was reached
			select {
			case item = <-c.items:
				item = group.tryAdd(item)
				if item != nil {
					// The group can't contain the new item
					canAddNext = false
				}
			case <-timeout:
				canAddNext = false
			}
		}

		data, recordLength, err := c.compress(&bufferIndex, group)
		if err != nil {
			log.Err(err).Msg("Error while compressing group in coalescer")
			group.sendResponse(err)
			continue
		}

		metrics.CoalescerMessagesPerGroup.Observe(float64(recordLength))

		// The group will be persisted, move the offset
		c.offset = group.offset + int64(recordLength)

		// Send in the background while the next block is generated in the foreground
		c.writer.Items <- newLocalDataItem(data, group, recordLength)
	}
}

// Compresses the group of record items and returns the compressed buffer along with the total number of records
func (c *coalescer) compress(index *uint8, group *coalescerGroup) ([]byte, int, error) {
	i := *index % 2
	*index = *index + 1
	buf := c.buffers.group[i]
	buf.Reset()
	compressor := c.buffers.compressor[i]
	// Compressor writer needs to be reinitialized each time
	compressor.Reset(buf)
	totalRecordLength := 0

	for _, item := range group.items {
		recordLength, err := item.marshal(compressor)
		if err != nil {
			return nil, 0, err
		}
		totalRecordLength += recordLength
	}

	if err := compressor.Close(); err != nil {
		return nil, 0, err
	}

	return buf.Bytes(), totalRecordLength, nil
}

func (c *coalescer) append(
	replication types.ReplicationInfo,
	length uint32,
	timestampMicros int64,
	contentType string,
	buffers [][]byte,
) error {
	record := &recordItem{
		replication: replication,
		length:      length,
		timestamp:   timestampMicros,
		contentType: contentType,
		buffers:     buffers,
		response:    make(chan error, 1),
	}
	c.items <- record
	return <-record.response
}

type localDataItem struct {
	payload      []byte          // compressed payload of the chunk
	group        *coalescerGroup // records associated with this chunk
	recordLength int             // the number of records contained in this group
}

func newLocalDataItem(payload []byte, group *coalescerGroup, recordLength int) *localDataItem {
	return &localDataItem{
		payload:      payload,
		group:        group,
		recordLength: recordLength,
	}
}

// DataBlock() gets the compressed payload of the chunk
func (d *localDataItem) DataBlock() []byte {
	return d.payload
}

func (d *localDataItem) Replication() types.ReplicationInfo {
	// TODO: Maybe simplify, 1 replication info per generation
	return d.group.items[0].replication
}

func (d *localDataItem) StartOffset() int64 {
	return d.group.offset
}

func (d *localDataItem) RecordLength() uint32 {
	if d.recordLength == 0 {
		log.Panic().Msgf("Invalid localDataItem with zero records")
	}
	return uint32(d.recordLength)
}

func (d *localDataItem) SetResult(err error) {
	d.group.sendResponse(err)
}
