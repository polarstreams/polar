package consuming

import (
	"bytes"
	"encoding/binary"
	"net/http"
	"time"

	"github.com/barcostreams/barco/internal/conf"
	"github.com/barcostreams/barco/internal/data"
	. "github.com/barcostreams/barco/internal/data"
	"github.com/barcostreams/barco/internal/discovery"
	"github.com/barcostreams/barco/internal/interbroker"
	. "github.com/barcostreams/barco/internal/types"
	"github.com/barcostreams/barco/internal/utils"
	"github.com/karlseguin/jsonwriter"
	"github.com/klauspost/compress/zstd"
	"github.com/rs/zerolog/log"
)

const refreshPeriod = 2 * time.Second
const offsetNoData = -1 // We should use types and flags in the future

// Receives read requests per group on a single thread.
//
// It should close unused readers
type groupReadQueue struct {
	// TODO: In the future, we should look into writing responses in parallel while
	// continue to have the ability to close unused readers.
	items          chan readQueueItem
	group          string
	state          *ConsumerState
	offsetState    OffsetState
	topologyGetter discovery.TopologyGetter
	datalog        data.Datalog
	gossiper       interbroker.Gossiper
	rrFactory      ReplicationReaderFactory
	config         conf.ConsumerConfig
	readerIndex    uint16
	readers        map[string]map[readerKey]*SegmentReader // map of readers per topic with map of token+index+clusterSize as keys
	decoder        *zstd.Decoder                           // Decoder used for json consumer responses
	decoderBuffer  []byte                                  // Small buffer for reading the decoded payload
}

func newGroupReadQueue(
	group string,
	state *ConsumerState,
	offsetState OffsetState,
	topologyGetter discovery.TopologyGetter,
	datalog data.Datalog,
	gossiper interbroker.Gossiper,
	rrFactory ReplicationReaderFactory,
	config conf.ConsumerConfig,
) *groupReadQueue {
	decoder, err := zstd.NewReader(bytes.NewReader(make([]byte, 0)),
		zstd.WithDecoderConcurrency(1), zstd.WithDecoderMaxMemory(uint64(config.MaxGroupSize())))
	utils.PanicIfErr(err, "Invalid zstd reader settings")

	queue := &groupReadQueue{
		items:          make(chan readQueueItem),
		readers:        make(map[string]map[readerKey]*SegmentReader),
		group:          group,
		state:          state,
		offsetState:    offsetState,
		topologyGetter: topologyGetter,
		datalog:        datalog,
		gossiper:       gossiper,
		rrFactory:      rrFactory,
		config:         config,
		decoder:        decoder,
		decoderBuffer:  make([]byte, 16_384),
	}
	go queue.process()
	go queue.refreshPeriodically()
	return queue
}

type readQueueItem struct {
	connId     string
	writer     http.ResponseWriter
	done       chan bool // Gets a single value when it's done writing the response
	commitOnly bool
	refresh    bool // Determines whether the item was meant for the read queue to re-evaluate internal maps
	format     responseFormat
}

func (q *groupReadQueue) process() {
	for item := range q.items {
		if item.refresh {
			q.refreshReaders()
			item.done <- true
			continue
		}

		group, tokens, topics := logsToServe(q.state, q.topologyGetter, item.connId)
		if group != q.group {
			// There was a change in topology, tell the client to poll again
			utils.NoContentResponse(item.writer, 0)
			item.done <- true
			continue
		}
		if len(tokens) == 0 || len(topics) == 0 {
			utils.NoContentResponse(item.writer, consumerNoOwnedDataDelay)
			item.done <- true
			continue
		}

		responseItems := make([]consumerResponseItem, 0, 1)
		errors := make([]error, 0)

		if len(responseItems) == 0 {
			readers := q.getReaders(tokens, topics, q.state.OffsetPolicy(item.connId))
			totalSize := 0

			for i := 0; i < len(readers) && totalSize < q.config.ConsumerReadThreshold(); i++ {
				// Use an incremental index to try to be fair between calls by round robin through readers
				reader := readers[int(q.readerIndex)%len(readers)]
				q.readerIndex++
				segmentReadItem := newSegmentReadItem(item.connId, item.commitOnly)
				reader.Items <- segmentReadItem
				err, chunk := segmentReadItem.result()

				if err != nil {
					log.Warn().Err(err).Msgf("There was an error reading for %s", &reader.Topic)
					errors = append(errors, err)
					continue
				}

				size := len(chunk.DataBlock())
				if size > 0 {
					// A non-empty data block
					responseItems = append(responseItems, consumerResponseItem{chunk: chunk, topic: reader.Topic})
					totalSize += size
				} else if !item.commitOnly {
					// No data from this reader since we last read
					q.maybeCloseReader(reader, chunk)
				}
			}
		}

		if len(responseItems) == 0 {
			if len(errors) > 0 {
				if !item.commitOnly {
					http.Error(item.writer, "Unexpected error while reading", http.StatusInternalServerError)
				} else {
					http.Error(item.writer, "Manual commit ignored: another origin reading", http.StatusConflict)
				}
			} else {
				utils.NoContentResponse(item.writer, consumerNoDataDelay)
			}

			item.done <- true
			continue
		}

		err := q.marshalResponse(item.writer, item.format, responseItems)
		if err != nil {
			// There was an error writing to the consumer
			// As long as the connection id is not longer reused, readers will reset the offset

			// TODO: Consider options, file ticket
			// Maybe we should force unregister on the consumer or reject altogether or maybe support replaying
			log.Warn().Err(err).Msgf("There was an error writing to the consumer")

			// Set the status at least (unlikely it will be set but worth a try)
			item.writer.WriteHeader(http.StatusInternalServerError)
		}
		item.done <- true
	}
}

func (q *groupReadQueue) refreshReaders() {
	// TODO: From time to time, we should check for readers for which the broker is not longer the leader and close,
	// using the offset and leadership info
	toRemove := make([]*SegmentReader, 0)
	for _, readersByTopic := range q.readers {
		for _, reader := range readersByTopic {
			if reader.HasStoppedReceiving() {
				toRemove = append(toRemove, reader)
				continue
			}

			topicId := &reader.Topic
			setMaxOffset := false

			// Check whether there was a generation change
			if reader.MaxProducedOffset == nil {
				gen := q.topologyGetter.Generation(topicId.Token)
				if gen == nil {
					// It's possible that there is no generation for token due to scale down
					prevGen := q.topologyGetter.GenerationInfo(topicId.GenId())
					if prevGen == nil {
						// Should resolve itself in the future
						log.Warn().Msgf("Generation %s could not be found", topicId.GenId())
						continue
					}
					if len(q.topologyGetter.NextGeneration(topicId.GenId())) > 0 {
						setMaxOffset = true
					}
				}
				if gen.Version > topicId.Version {
					// There's a new version, we should set the maximum offset for the logic to identify when we
					// finished consuming
					setMaxOffset = true
				}
			}

			if setMaxOffset {
				maxOffset, err := q.offsetState.MaxProducedOffset(topicId)
				if err != nil {
					log.Warn().Err(err).Msgf("Max offset could not be retrieved for %s", topicId)
					continue
				}
				if maxOffset == offsetNoData {
					log.Error().Msgf("Unexpected no data found for open reader in previous generation %s", topicId)
				}
				reader.MaxProducedOffset = &maxOffset
			}
		}
	}

	for _, reader := range toRemove {
		readersByTopic, found := q.readers[reader.Topic.Name]
		if !found {
			continue
		}
		t := &reader.Topic
		delete(readersByTopic, newReaderKey(t.Token, t.RangeIndex, reader.TopicRangeClusterSize))
	}
}

func (q *groupReadQueue) refreshPeriodically() {
	for {
		time.Sleep(refreshPeriod)
		done := make(chan bool, 1)
		q.items <- readQueueItem{refresh: true, done: done}
		<-done
	}
}

func (q *groupReadQueue) marshalResponse(
	w http.ResponseWriter,
	format responseFormat,
	responseItems []consumerResponseItem,
) error {
	if format == jsonFormat {
		return q.marshalJsonResponse(w, responseItems)
	}

	w.Header().Add("Content-Type", defaultMimeType)
	if err := binary.Write(w, conf.Endianness, uint16(len(responseItems))); err != nil {
		// There was an issue writing to the wire
		log.Err(err).Msgf("There was an error while trying to write the consumer response")
		return err
	}
	for _, item := range responseItems {
		err := item.Marshal(w)
		if err != nil {
			log.Err(err).Msgf("There was an error while trying to write the consumer response items")
			return err
		}
	}

	return nil
}

func (q *groupReadQueue) marshalJsonResponse(w http.ResponseWriter, responseItems []consumerResponseItem) error {
	w.Header().Add("Content-Type", jsonMimeType)
	writer := jsonwriter.New(w)
	var err error = nil
	writer.RootArray(func() {
		for _, item := range responseItems {
			err = item.MarshalJson(writer, q.decoder, q.decoderBuffer)
			if err != nil {
				log.Err(err).Msgf("There was an error while trying to write the consumer response items in json")
				break
			}
		}
	})
	return err
}

func (q *groupReadQueue) readNext(connId string, format responseFormat, w http.ResponseWriter) {
	done := make(chan bool, 1)
	q.items <- readQueueItem{
		connId: connId,
		writer: w,
		format: format,
		done:   done,
	}

	<-done
}

func (q *groupReadQueue) manualCommit(connId string, w http.ResponseWriter) {
	done := make(chan bool, 1)
	q.items <- readQueueItem{
		connId:     connId,
		writer:     w,
		done:       done,
		commitOnly: true,
	}

	<-done
}

func (q *groupReadQueue) maybeCloseReader(reader *SegmentReader, chunk SegmentChunk) {
	nextReadOffset := chunk.StartOffset()
	if reader.MaxProducedOffset != nil && nextReadOffset > *reader.MaxProducedOffset {
		// There will be no more data, we should dispose the reader
		q.closeReader(reader)

		if !reader.StoredOffsetAsCompleted() {
			offset := NewOffset(&reader.Topic, reader.TopicRangeClusterSize, reader.SourceVersion, OffsetCompleted)
			q.offsetState.Set(q.group, reader.Topic.Name, offset, OffsetCommitAll)
		}
	}
}

func (q *groupReadQueue) closeReader(reader *SegmentReader) {
	topicId := reader.Topic
	topicReaders, found := q.readers[topicId.Name]
	if !found {
		return
	}

	key := newReaderKey(topicId.Token, topicId.RangeIndex, reader.TopicRangeClusterSize)
	delete(topicReaders, key)
	close(reader.Items)
}

// Gets the readers, creating them if necessary
func (q *groupReadQueue) getReaders(
	tokenRanges []TokenRanges,
	topics []string,
	policy OffsetResetPolicy,
) []*SegmentReader {
	result := make([]*SegmentReader, 0, len(tokenRanges)*len(topics))

	for _, t := range tokenRanges {
		currentGen := q.topologyGetter.Generation(t.Token)
		if currentGen == nil {
			// Maybe the token/broker no longer exists
			log.Warn().Msgf("Information about the generation of token %d could not be found", t.Token)
			continue
		}
		if currentGen.Leader != q.topologyGetter.Topology().MyOrdinal() {
			continue
		}

		for _, topic := range topics {
			topicReaders, found := q.readers[topic]
			// It is possible that offsets point to the same generation range, avoid duplicates
			includedInResult := make(map[readerKey]bool)
			if !found {
				topicReaders = make(map[readerKey]*SegmentReader)
				q.readers[topic] = topicReaders
			}

			for _, index := range t.Indices {
				key := newReaderKey(t.Token, index, t.ClusterSize)
				reader, found := topicReaders[key]
				if found && !includedInResult[key] {
					result = append(result, reader)
					includedInResult[key] = true
					continue
				}

				offsetList := q.offsetState.GetAllWithDefaults(q.group, topic, t.Token, index, t.ClusterSize, policy)
				for _, offset := range offsetList {
					if offset.Offset == OffsetCompleted {
						continue
					}
					key := newReaderKey(offset.Token, offset.Index, offset.ClusterSize)
					reader, found := topicReaders[key]
					if found && !includedInResult[key] {
						result = append(result, reader)
						includedInResult[key] = true
						continue
					}

					// We don't have a reader for this key and group, create one
					reader = q.createReader(topic, offset, currentGen)
					if reader == nil {
						continue
					}

					topicReaders[key] = reader
					result = append(result, reader)
				}
			}
		}
	}

	return result
}

func (q *groupReadQueue) createReader(
	topic string,
	offset Offset,
	source *Generation,
) *SegmentReader {
	topicId := TopicDataId{
		Name:       topic,
		Token:      offset.Token,
		RangeIndex: offset.Index,
		Version:    offset.Version,
	}
	topicGen := q.topologyGetter.GenerationInfo(topicId.GenId())
	if topicGen == nil {
		log.Panic().Msgf("Generation %s could not be retrieved to create a reader", topicId.GenId())
	}

	var maxProducedOffset *int64 = nil
	if topicId.GenId() != source.Id() {
		// We are reading from an old generation
		// We need to set the max offset produced
		value, err := q.offsetState.MaxProducedOffset(&topicId)
		if err != nil {
			// Hopefully in the future it will resolve itself
			return nil
		}
		if value == offsetNoData {
			// No data was produced for this token+index
			log.Info().Msgf("No data was produced for %s, moving offset", &topicId)

			// Mark as completed
			offset := NewOffset(&topicId, offset.ClusterSize, source.Id(), OffsetCompleted)
			q.offsetState.Set(q.group, topicId.Name, offset, OffsetCommitAll)
			return nil
		}
		maxProducedOffset = &value
	}

	topology := q.topologyGetter.Topology()
	wasLeader := topicGen.Leader == topology.MyOrdinal()
	var replicationReader ReplicationReader
	if wasLeader {
		log.Info().Msgf("Creating reader for group %s and topic %s", q.group, &topicId)
	} else {
		log.Info().Msgf("Creating reader for group %s and topic %s as follower", q.group, &topicId)
		replicationReader = q.rrFactory.GetOrCreate(&topicId, topology, topicGen, q.offsetState)
	}

	reader, err := NewSegmentReader(
		q.group,
		wasLeader,
		replicationReader,
		topicId,
		topicGen.ClusterSize,
		source.Id(),
		offset.Offset,
		q.offsetState,
		maxProducedOffset,
		q.datalog,
		q.config)

	if err != nil {
		// reader could not be initialized, skip for now
		return nil
	}

	return reader
}
