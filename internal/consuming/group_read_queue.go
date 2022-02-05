package consuming

import (
	"encoding/binary"
	"fmt"
	"io"
	"net/http"
	"time"

	. "github.com/google/uuid"
	"github.com/jorgebay/soda/internal/conf"
	. "github.com/jorgebay/soda/internal/data"
	"github.com/jorgebay/soda/internal/discovery"
	"github.com/jorgebay/soda/internal/interbroker"
	. "github.com/jorgebay/soda/internal/types"
	"github.com/jorgebay/soda/internal/utils"
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
	readers        map[readerKey]map[string]*SegmentReader // Readers per token and topic
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
					responseItems = append(responseItems, r)
				}
			}
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
					// There's no more data
					// TODO: Implement empty closing
					// - Check if we are in the latest generation
					// - if not, check the maximum offset with the replica and close it
					// - Use a time since to check from time to time
					// - Do in the background and channel response
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

	// TODO:  look for generation after X (child when reading)
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
			readersByToken, found := q.readers[key]
			if !found {
				readersByToken = make(map[string]*SegmentReader)
				q.readers[key] = readersByToken
			}

			for _, topic := range topics {
				reader, found := readersByToken[topic]
				if !found {
					log.Info().Msgf("Creating reader for token %d/%d and topic '%s'", t, index, topic)
					offset := q.offsetState.Get(q.group, topic, t.Token, index)
					if offset == nil {
						// We have no information of this group
						//TODO: Verify that we have loaded from other brokers

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
							// Hopefully in the future it will resolve itself
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

					readersByToken[topic] = reader
				}
				result = append(result, reader)
			}
		}
	}

	return result
}

func (q *groupReadQueue) getMaxProducedOffset(topicId *TopicDataId) (uint64, error) {
	gen, err := q.topologyGetter.GenerationInfo(topicId.Token, topicId.GenId)
	utils.PanicIfErr(err, "Generation info failed to be retrieved")
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
		log.Error().Msgf(message)
		return 0, fmt.Errorf(message)
	}

	return *result, nil
}

type segmentReadItem struct {
	chunkResult chan SegmentChunk
	errorResult chan error
}

func newSegmentReadItem() *segmentReadItem {
	return &segmentReadItem{
		chunkResult: make(chan SegmentChunk, 1),
		errorResult: make(chan error, 1),
	}
}

func (r *segmentReadItem) SetResult(err error, chunk SegmentChunk) {
	r.chunkResult <- chunk
	r.errorResult <- err
}

func (r *segmentReadItem) result() (err error, chunk SegmentChunk) {
	return <-r.errorResult, <-r.chunkResult
}

type consumerResponseItem struct {
	chunk SegmentChunk
	topic TopicDataId
}

func (i *consumerResponseItem) Marshal(w io.Writer) error {
	if err := binary.Write(w, conf.Endianness, i.topic.Token); err != nil {
		return err
	}
	if err := binary.Write(w, conf.Endianness, i.topic.GenId); err != nil {
		return err
	}
	if err := binary.Write(w, conf.Endianness, uint8(len(i.topic.Name))); err != nil {
		return err
	}
	if _, err := w.Write([]byte(i.topic.Name)); err != nil {
		return err
	}
	if _, err := w.Write(i.chunk.DataBlock()); err != nil {
		return err
	}
	return nil
}

type readerKey struct {
	token      Token
	rangeIndex RangeIndex
}
