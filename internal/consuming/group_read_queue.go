package consuming

import (
	"encoding/binary"
	"fmt"
	"net/http"
	"time"

	"github.com/barcostreams/barco/internal/conf"
	. "github.com/barcostreams/barco/internal/data"
	"github.com/barcostreams/barco/internal/discovery"
	"github.com/barcostreams/barco/internal/interbroker"
	. "github.com/barcostreams/barco/internal/types"
	"github.com/barcostreams/barco/internal/utils"
	. "github.com/google/uuid"
	"github.com/rs/zerolog/log"
)

const (
	fetchMaxWait = 1 * time.Second
	requeueDelay = 200 * time.Millisecond
)

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
	gossiper       interbroker.Gossiper
	config         conf.ConsumerConfig
	readerIndex    uint16
	readers        map[readerKey]map[string]*SegmentReader // Uses token+index as keys and map of readers per topic as values
}

func newGroupReadQueue(
	group string,
	state *ConsumerState,
	offsetState OffsetState,
	topologyGetter discovery.TopologyGetter,
	gossiper interbroker.Gossiper,
	config conf.ConsumerConfig,
) *groupReadQueue {
	queue := &groupReadQueue{
		items:          make(chan readQueueItem),
		readers:        make(map[readerKey]map[string]*SegmentReader),
		group:          group,
		state:          state,
		offsetState:    offsetState,
		topologyGetter: topologyGetter,
		gossiper:       gossiper,
		config:         config,
	}
	go queue.process()
	return queue
}

type readQueueItem struct {
	connId    UUID
	writer    http.ResponseWriter
	timestamp time.Time
	done      chan bool // Gets a single value when it's done writing the response
	refresh   bool      // Determines whether the item was meant for the read queue to re-evaluate internal maps
}

func (q *groupReadQueue) process() {
	failedResponseItems := make([]consumerResponseItem, 0)
	for item := range q.items {
		if item.refresh {
			// TODO: Implement close readers
			// Check which readers can be closed
			continue
		}

		group, tokens, topics := logsToServe(q.state, q.topologyGetter, item.connId)
		log.Debug().Msgf("Group read queue %s handling item %s %v %v", q.group, group, tokens, topics)
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

		if len(failedResponseItems) > 0 {
			responseItems = q.useFailedResponseItems(failedResponseItems, tokens, topics)
		}

		if len(responseItems) == 0 {
			readers := q.getReaders(tokens, topics)
			totalSize := 0

			for i := 0; i < len(readers) && totalSize < q.config.ConsumerReadThreshold(); i++ {
				// Use an incremental index to try to be fair between calls by round robin through readers
				reader := readers[int(q.readerIndex)%len(readers)]
				q.readerIndex++
				segmentReadItem := newSegmentReadItem()
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
				} else {
					// No data from this reader since we last read
					q.maybeCloseReader(reader, chunk)
				}
			}
		}

		if len(responseItems) == 0 {
			if len(errors) > 0 {
				http.Error(item.writer, "Internal server error", 500)
			} else if time.Since(item.timestamp) < fetchMaxWait-requeueDelay {
				// We can requeue it to await for new data and move on
				go func() {
					time.Sleep(requeueDelay)
					q.items <- item
				}()
				continue
			} else {
				// Fetch can't wait any more
				utils.NoContentResponse(item.writer, 0)
			}

			item.done <- true
			continue
		}

		err := marshalResponse(item.writer, responseItems)
		if err != nil {
			if len(failedResponseItems) > 0 {
				log.Warn().
					Int("length", len(failedResponseItems)).
					Msgf("New failed responses appended while there are previous failed responses")
			}

			// There was an error writing the response to the consumer
			// Use this responseItems for the next call
			failedResponseItems = append(failedResponseItems, responseItems...)
			// Set the status at least (unlikely it will be set but worth a try)
			item.writer.WriteHeader(http.StatusInternalServerError)
		} else {
			// TODO: Store the position of the consumer group
		}
		item.done <- true
	}
}

func marshalResponse(w http.ResponseWriter, responseItems []consumerResponseItem) error {
	w.Header().Add("Content-Type", contentType)
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

func (q *groupReadQueue) readNext(connId UUID, w http.ResponseWriter) {
	done := make(chan bool, 1)
	q.items <- readQueueItem{
		connId:    connId,
		writer:    w,
		done:      done,
		timestamp: time.Now(),
	}

	<-done
}

func (q *groupReadQueue) maybeCloseReader(reader *SegmentReader, chunk SegmentChunk) {
	nextReadOffset := chunk.StartOffset()
	if reader.MaxProducedOffset != nil && nextReadOffset > *reader.MaxProducedOffset {
		// There will be no more data, we should dispose the reader
		q.closeReader(reader)

		q.moveOffsetToNextGeneration(reader.Topic)
	}
}

// Moves offset to next generation
func (q *groupReadQueue) moveOffsetToNextGeneration(topicId TopicDataId) {
	nextGens := q.topologyGetter.NextGeneration(topicId.Token, topicId.GenId)
	for _, gen := range nextGens {
		offset := Offset{Offset: 0, Version: gen.Version}
		q.offsetState.Set(
			q.group, topicId.Name, gen.Start, topicId.RangeIndex, offset, OffsetCommitAll)
	}
}

func (q *groupReadQueue) closeReader(reader *SegmentReader) {
	topicId := reader.Topic
	key := readerKey{
		token:      topicId.Token,
		rangeIndex: topicId.RangeIndex,
	}

	readersByTopic, found := q.readers[key]
	if !found {
		return
	}

	delete(readersByTopic, topicId.Name)
	close(reader.Items)
}

func (q *groupReadQueue) useFailedResponseItems(
	failedResponseItems []consumerResponseItem,
	tokens []TokenRanges,
	topics []string,
) []consumerResponseItem {
	result := make([]consumerResponseItem, 0, 1)
	// There was an error writing the previous response to a consumer in this group
	// If the tokens and topic match, give the response to the consumer
	for i := len(failedResponseItems) - 1; i >= 0; i-- {
		r := failedResponseItems[i]
		// Use sequential search, there should be a single (or two) tokens and a handful of topics
		if utils.ContainsToken(tokens, r.topic.Token) && utils.ContainsString(topics, r.topic.Name) {
			// Remove it from failed
			failedResponseItems = append(failedResponseItems[:i], failedResponseItems[i+1:]...)

			offset := q.offsetState.Get(q.group, r.topic.Name, r.topic.Token, r.topic.RangeIndex)
			if offset != nil && offset.Version != r.topic.GenId {
				// Since it failed, there were topology changes and the offset state for the group changed
				// It can be safely ignored
				continue
			}
			result = append(result, r)
		}
	}
	return result
}

// Gets the readers, creating them if necessary
func (q *groupReadQueue) getReaders(tokenRanges []TokenRanges, topics []string) []*SegmentReader {
	result := make([]*SegmentReader, 0, len(tokenRanges)*len(topics))

	for _, t := range tokenRanges {
		currentGen := q.topologyGetter.Generation(t.Token)
		if currentGen == nil {
			// TODO: Maybe the token/broker no longer exists
			log.Warn().Msgf("Information about the generation of token %d could not be found", t.Token)
			continue
		}
		for _, index := range t.Indices {
			key := readerKey{t.Token, index}
			readersByTopic, found := q.readers[key]
			if !found {
				readersByTopic = make(map[string]*SegmentReader)
				q.readers[key] = readersByTopic
			}

			for _, topic := range topics {
				reader, found := readersByTopic[topic]
				if !found {
					log.Info().Msgf("Creating reader for token %d/%d and topic '%s'", t.Token, index, topic)
					offset := q.offsetState.Get(q.group, topic, t.Token, index)
					if offset == nil {
						// We have no information of this group
						//TODO: Verify that we have loaded from other brokers

						// TODO: Start at tail or last segment not archived
						offset = &Offset{
							Offset:  0,
							Version: 1,
							Source:  currentGen.Version,
						}
					}

					if !q.offsetState.CanConsumeToken(q.group, topic, *currentGen) {
						// TODO: Verify that a previous token from a broker that does not exist any more was consumed
						log.Debug().Msgf(
							"Group %s can't consume topic %s for token %d v%d yet",
							q.group, topic, key.token, currentGen.Version)
						continue
					}

					readerVersion := offset.Version
					topicId := TopicDataId{
						Name:       topic,
						Token:      t.Token,
						RangeIndex: index,
						GenId:      readerVersion,
					}

					var maxProducedOffset *uint64 = nil
					if readerVersion != currentGen.Version {
						// We are reading from an old generation
						// We need to set the max offset produced
						value, err := q.getMaxProducedOffset(&topicId)
						if err != nil {
							if q.hasData(&topicId) {
								// Hopefully in the future it will resolve itself
								continue
							}

							log.Info().Msgf("No data found for topic %s, moving offset", &topicId)
							q.moveOffsetToNextGeneration(topicId)
							continue
						}
						maxProducedOffset = &value
					}

					reader, err := NewSegmentReader(
						q.group, topicId, currentGen.Version, q.offsetState, maxProducedOffset, q.config)
					if err != nil {
						// reader could not be initialized, skip for now
						continue
					}

					readersByTopic[topic] = reader
					result = append(result, reader)
				} else {
					result = append(result, reader)
				}
			}
		}
	}

	return result
}

func (q *groupReadQueue) hasData(topicId *TopicDataId) bool {
	// TODO: Implement hasData() by checking folder
	return false
}

func (q *groupReadQueue) getMaxProducedOffset(topicId *TopicDataId) (uint64, error) {
	gen := q.topologyGetter.GenerationInfo(topicId.Token, topicId.GenId)
	if gen == nil {
		log.Warn().Msgf("Past generation could not be retrieved %d v%d", topicId.Token, topicId.GenId)
		// Attempt to get the info from local storage
		return q.offsetState.ProducerOffsetLocal(topicId)
	}

	myOrdinal := q.topologyGetter.Topology().MyOrdinal()
	peers := make([]int, 0)
	for _, ordinal := range gen.Followers {
		if ordinal != myOrdinal {
			peers = append(peers, ordinal)
		}
	}

	c := make(chan *uint64)

	// Get the values from the peers and local storage
	go func() {
		localValue, err := q.offsetState.ProducerOffsetLocal(topicId)
		if err != nil {
			log.Warn().Err(err).Msgf("Max producer offset could not be retrieved from local storage for %s", topicId)
			c <- nil
		} else {
			c <- &localValue
		}
	}()

	for _, ordinalValue := range peers {
		// Closure will capture it
		ordinal := ordinalValue
		go func() {
			value, err := q.gossiper.ReadProducerOffset(ordinal, topicId)
			if err != nil {
				log.Warn().Err(err).Msgf("Offset could not be retrieved from peer B%d for %s", ordinal, topicId)
				c <- nil
			} else {
				c <- &value
			}
		}()
	}

	var result *uint64 = nil

	for i := 0; i < len(peers)+1; i++ {
		value := <-c
		if value != nil {
			if result == nil || *result < *value {
				result = value
			}
		}
	}

	if result == nil {
		message := fmt.Sprintf("Max producer offset could not be retrieved from peers or local for %s", topicId)
		log.Warn().Msgf(message)
		return 0, fmt.Errorf(message)
	}

	return *result, nil
}
