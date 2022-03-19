package consuming

import (
	"encoding/binary"
	"fmt"
	"math"
	"net/http"
	"os"
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
	fetchMaxWait  = 1 * time.Second
	refreshPeriod = 2 * time.Second
	requeueDelay  = 200 * time.Millisecond
)

const offsetCompleted = math.MaxInt64
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
	go queue.refreshPeriodically()
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

func (q *groupReadQueue) refreshReaders() {
	// TODO: Validate readers for which I'm not longer the leader and close them
	for _, readersByTopic := range q.readers {
		for _, reader := range readersByTopic {
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
				maxOffset, err := q.getMaxProducedOffset(topicId)
				if err != nil {
					log.Warn().Err(err).Msgf("Max offset could not be retrieved for %s", topicId)
					continue
				}
				if maxOffset == offsetNoData {
					log.Error().Msgf("Unexpected no data found for open reader in previous generation")
				}
				reader.MaxProducedOffset = &maxOffset
			}
		}
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

		q.moveOffsetToNextGeneration(reader.Topic, reader.SourceVersion)
	}
}

// Moves offset to next generation
func (q *groupReadQueue) moveOffsetToNextGeneration(topicId TopicDataId, source GenId) bool {
	log.Debug().Msgf("Looking for next generation of %s", topicId.GenId())
	nextGens := q.topologyGetter.NextGeneration(topicId.GenId())
	if len(nextGens) == 0 {
		return false
	}

	if len(nextGens) == 1 && len(nextGens[0].Parents) == 2 {
		// Scale down from 2 tokens to 1
		log.Debug().Msgf("Setting offset for previous generation that was scaled down for %s", &topicId)
		offset := Offset{
			Offset:  offsetCompleted,
			Version: topicId.Version,
			Source:  source,
		}
		q.offsetState.Set(
			q.group, topicId.Name, topicId.Token, topicId.RangeIndex, offset, OffsetCommitAll)

		var otherParent GenId
		for _, id := range nextGens[0].Parents {
			if id.Start != topicId.Token {
				otherParent = id
			}
		}

		otherParentOffset := q.offsetState.Get(
			q.group, topicId.Name, otherParent.Start, topicId.RangeIndex)

		if otherParentOffset != nil {
			log.Debug().Msgf("Other parent: %v; offset: %v", otherParent, *otherParentOffset)
			if otherParentOffset.Version == otherParent.Version && otherParentOffset.Offset == offsetCompleted {
				log.Info().Msgf("Moving offset of joined generation for %s", &topicId)
				token := nextGens[0].Start
				version := nextGens[0].Version

				offset := Offset{
					Offset:  0,
					Version: version,
					Source:  source,
				}
				q.offsetState.Set(
					q.group, topicId.Name, token, topicId.RangeIndex, offset, OffsetCommitAll)
			}
		}

		return true
	}

	for _, gen := range nextGens {
		offset := Offset{
			Offset:  0,
			Version: gen.Version,
			Source:  source,
		}
		q.offsetState.Set(
			q.group, topicId.Name, topicId.Token, topicId.RangeIndex, offset, OffsetCommitAll)
	}
	return true
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
			if offset != nil && offset.Version != r.topic.Version {
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
			// Maybe the token/broker no longer exists
			log.Warn().Msgf("Information about the generation of token %d could not be found", t.Token)
			continue
		}
		if currentGen.Leader != q.topologyGetter.Topology().MyOrdinal() {
			continue
		}

		// In the case of scaling down, an owned token range might be the result of two previous tokens
		tokens := []Token{t.Token}
		tokens = appendJoined(tokens, currentGen)

		for _, token := range tokens {
			for _, index := range t.Indices {
				key := readerKey{token, index}
				readersByTopic, found := q.readers[key]
				if !found {
					readersByTopic = make(map[string]*SegmentReader)
					q.readers[key] = readersByTopic
				}

				for _, topic := range topics {
					reader, found := readersByTopic[topic]
					if !found {
						reader = q.createReader(topic, token, index, currentGen)
						if reader == nil {
							continue
						}

						readersByTopic[topic] = reader
					}

					result = append(result, reader)
				}
			}
		}
	}

	return result
}

func (q *groupReadQueue) createReader(
	topic string,
	token Token,
	index RangeIndex,
	source *Generation,
) *SegmentReader {

	if token != source.Start {
		sourceTokenOffset := q.offsetState.Get(q.group, topic, source.Start, index)
		if sourceTokenOffset != nil && sourceTokenOffset.Version == source.Version {
			// Once the offset for the source token is on the last generation, we can ignore this previous token
			return nil
		}
	}

	log.Debug().Msgf("Considering creating reader for token %d/%d and topic '%s'", token, index, topic)
	offset := q.offsetState.Get(q.group, topic, token, index)

	if offset == nil {
		// We have no information of this group
		//TODO: Verify that we have loaded from other brokers

		offset = &Offset{
			Offset:  0,
			Version: 1,
			Source:  source.Id(),
		}
	}

	if offset.Offset == offsetCompleted {
		// Probably waiting for the reader generation to be moved
		return nil
	}

	readerVersion := offset.Version
	topicId := TopicDataId{
		Name:       topic,
		Token:      token,
		RangeIndex: index,
		Version:    readerVersion,
	}

	var maxProducedOffset *int64 = nil
	if topicId.GenId() != source.Id() {
		// We are reading from an old generation
		// We need to set the max offset produced
		value, err := q.getMaxProducedOffset(&topicId)
		if err != nil {
			// Hopefully in the future it will resolve itself
			return nil
		}
		if value == offsetNoData {
			// No data was produced for this token+index
			log.Info().Msgf("No data was produced for %s, moving offset", &topicId)
			if movedOffset := q.moveOffsetToNextGeneration(topicId, source.Id()); !movedOffset {
				log.Error().Msgf(
					"Offset could not be moved for %s, as next generation could not be found", &topicId)
			}
			return nil
		}
		maxProducedOffset = &value
	}

	log.Info().Msgf("Creating reader for token %d/%d and topic '%s'", token, index, topic)
	reader, err := NewSegmentReader(q.group, topicId, source.Id(), q.offsetState, maxProducedOffset, q.config)
	if err != nil {
		// reader could not be initialized, skip for now
		return nil
	}

	return reader
}

func appendJoined(tokens []Token, gen *Generation) []Token {
	if len(gen.Parents) != 2 {
		return tokens
	}

	for _, id := range gen.Parents {
		if id.Start != tokens[0] {
			tokens = append(tokens, id.Start)
		}
	}
	return tokens
}

func (q *groupReadQueue) getMaxProducedOffset(topicId *TopicDataId) (int64, error) {
	gen := q.topologyGetter.GenerationInfo(topicId.GenId())
	if gen == nil {
		log.Warn().Msgf("Past generation could not be retrieved %s", topicId.GenId())
		// Attempt to get the info from local storage
		return q.offsetState.ProducerOffsetLocal(topicId)
	}

	topology := q.topologyGetter.Topology()
	myOrdinal := topology.MyOrdinal()
	peers := make([]int, 0)
	for _, ordinal := range gen.Followers {
		if ordinal != myOrdinal {
			peers = append(peers, ordinal)
		}
	}

	c := make(chan *int64)
	notFound := new(int64)
	*notFound = offsetNoData

	// Get the values from the peers and local storage
	go func() {
		localValue, err := q.offsetState.ProducerOffsetLocal(topicId)
		if err != nil {
			if os.IsNotExist(err) {
				c <- notFound
			} else {
				log.Warn().Err(err).Msgf("Max producer offset could not be retrieved from local storage for %s", topicId)
				c <- nil
			}
		} else {
			c <- &localValue
		}
	}()

	for _, ordinalValue := range peers {
		// Closure will capture it
		ordinal := ordinalValue
		go func() {
			if ordinal >= len(topology.Brokers) {
				if q.gossiper.IsHostUp(ordinal) {
					// Optimistically attempt to get the info from the broker that is leaving the cluster
					value, err := q.gossiper.ReadProducerOffset(ordinal, topicId)
					if err == nil {
						c <- &value
						return
					}
				}
				c <- notFound
				return
			}
			value, err := q.gossiper.ReadProducerOffset(ordinal, topicId)
			if err != nil {
				if err == GossipGetNotFound {
					c <- notFound
				} else {
					log.Warn().Err(err).Msgf("Producer offset could not be retrieved from peer B%d for %s", ordinal, topicId)
					c <- nil
				}
			} else {
				c <- &value
			}
		}()
	}

	var result *int64 = nil
	notFoundCount := 0

	for i := 0; i < len(peers)+1; i++ {
		value := <-c
		if value == notFound {
			notFoundCount++
			continue
		}
		if value != nil {
			if result == nil || *result < *value {
				result = value
			}
		}
	}

	if result == nil {
		if notFoundCount == len(peers)+1 {
			// We can safely assume that no data was produced for this token+range
			return offsetNoData, nil
		}

		message := fmt.Sprintf("Max producer offset could not be retrieved from peers or local for %s", topicId)
		log.Warn().Msgf(message)
		return 0, fmt.Errorf(message)
	}

	return *result, nil
}
