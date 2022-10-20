package consuming

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/barcostreams/barco/internal/conf"
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
	gossiper       interbroker.Gossiper
	rrFactory      ReplicationReaderFactory
	config         conf.ConsumerConfig
	readerIndex    uint16
	readers        map[readerKey]map[string]*SegmentReader // Uses token+index as keys and map of readers per topic as values
	decoder        *zstd.Decoder                           // Decoder used for json consumer responses
	decoderBuffer  []byte                                  // Small buffer for reading the decoded payload
}

func newGroupReadQueue(
	group string,
	state *ConsumerState,
	offsetState OffsetState,
	topologyGetter discovery.TopologyGetter,
	gossiper interbroker.Gossiper,
	rrFactory ReplicationReaderFactory,
	config conf.ConsumerConfig,
) *groupReadQueue {
	decoder, err := zstd.NewReader(bytes.NewReader(make([]byte, 0)),
		zstd.WithDecoderConcurrency(1), zstd.WithDecoderMaxMemory(uint64(config.MaxGroupSize())))
	utils.PanicIfErr(err, "Invalid zstd reader settings")

	queue := &groupReadQueue{
		items:          make(chan readQueueItem),
		readers:        make(map[readerKey]map[string]*SegmentReader),
		group:          group,
		state:          state,
		offsetState:    offsetState,
		topologyGetter: topologyGetter,
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

		// Mark as completed
		offset := Offset{
			Offset:  OffsetCompleted,
			Version: topicId.Version,
			Source:  source,
		}
		q.offsetState.Set(q.group, topicId.Name, topicId.Token, topicId.RangeIndex, offset, OffsetCommitAll)

		targetToken := nextGens[0].Start
		var otherOffset *Offset
		var targetRangeIndex RangeIndex

		otherRangeIndex := topicId.RangeIndex - 1
		if topicId.RangeIndex%2 == 0 {
			otherRangeIndex = topicId.RangeIndex + 1
		}

		// The range was joined, we need to find the indices this falls into
		// To move the target offset to zero with next gen, we need to make sure the other range completed as well
		if topicId.Token == targetToken {
			// Its the token that continues to be in the cluster
			targetRangeIndex = topicId.RangeIndex / 2
			otherOffset = q.offsetState.Get(q.group, topicId.Name, topicId.Token, otherRangeIndex)
		} else {
			targetRangeIndex = RangeIndex(q.config.ConsumerRanges())/2 + topicId.RangeIndex/2
			otherOffset = q.offsetState.Get(q.group, topicId.Name, topicId.Token, otherRangeIndex)
		}

		otherIsCompleted :=
			otherOffset != nil &&
				// Either it was marked as completed or the other range already passed the generation
				(otherOffset.Offset == OffsetCompleted || otherOffset.Version >= nextGens[0].Version)

		if otherIsCompleted {
			log.Info().Msgf("After scaling down, setting group offset '%s' for topic '%s' to %d/%d v%d",
				q.group, topicId.Name, targetToken, targetRangeIndex, nextGens[0].Version)

			q.offsetState.Set(q.group, topicId.Name, targetToken, targetRangeIndex, Offset{
				Offset:  0,
				Version: nextGens[0].Version,
				Source:  source,
			}, OffsetCommitAll)
		} else {
			log.Info().Msgf("After scaling down, could not move offset '%s' for topic '%s' to %d/%d v%d: other offset %s (of %d/%d)",
				q.group, topicId.Name, targetToken, targetRangeIndex, nextGens[0].Version, otherOffset, topicId.Token, otherRangeIndex)
		}

		return true
	}

	if len(nextGens) == 1 && len(nextGens[0].Parents) == 1 {
		gen := nextGens[0]
		offset := Offset{
			Offset:  0,
			Version: gen.Version,
			Source:  source,
		}
		q.offsetState.Set(
			q.group, topicId.Name, topicId.Token, topicId.RangeIndex, offset, OffsetCommitAll)

		return true
	}

	if len(nextGens) == 2 {
		// Scale up from 1 token to two
		rangesLength := q.config.ConsumerRanges()
		originalGenerationIndex := utils.FindGenByToken(nextGens, topicId.Token)
		if originalGenerationIndex == -1 {
			log.Panic().Msgf("Original generation not found")
		}
		var rangeIndices []RangeIndex
		var gen Generation

		// The range was split, we need to find the token where this falls into
		if topicId.RangeIndex < RangeIndex(rangesLength)/2 {
			// Original token
			gen = nextGens[originalGenerationIndex]
			rangeIndices = []RangeIndex{topicId.RangeIndex * 2, topicId.RangeIndex*2 + 1}
		} else {
			genIndex := originalGenerationIndex + 1
			if originalGenerationIndex == 1 {
				genIndex = 0
			}
			gen = nextGens[genIndex]

			startRangeIndex := topicId.RangeIndex*2 - RangeIndex(rangesLength)
			rangeIndices = []RangeIndex{startRangeIndex, startRangeIndex + 1}
		}

		offset := Offset{
			Offset:  0,
			Version: gen.Version,
			Source:  source,
		}
		for _, rangeIndex := range rangeIndices {
			q.offsetState.Set(
				q.group, topicId.Name, gen.Start, rangeIndex, offset, OffsetCommitAll)
		}

		return true
	}

	log.Panic().
		Int("Next", len(nextGens)).Int("Parents", len(nextGens[0].Parents)).
		Msgf("Unexpected number of next generations")
	return false
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
	// Check that previous tokens (scaled down), whether the reader can ignored because all the data has been consumed
	if token != source.Start {
		// Joined tokens, calculate the index change
		// The source index starts at the middle of the range
		sourceIndex := RangeIndex(q.config.ConsumerRanges())/2 + index/2
		sourceTokenOffset := q.offsetState.Get(q.group, topic, source.Start, sourceIndex)
		if sourceTokenOffset != nil && sourceTokenOffset.Version == source.Version {
			// The offset for the source token is on the last generation, we can ignore this previous token
			return nil
		}
	}

	offset := q.offsetState.Get(q.group, topic, token, index)
	log.Debug().Msgf("May create for group %s, token %d/%d and topic '%s' with offset %s", q.group, token, index, topic, offset)

	if offset == nil {
		// We have no information of this group
		//TODO: Verify that we have loaded from other brokers

		offset = &Offset{
			Offset:  0,
			Version: 1,
			Source:  source.Id(),
		}
	}

	if offset.Offset == OffsetCompleted {
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

	isLeader := true
	topology := q.topologyGetter.Topology()
	topicGen := q.topologyGetter.GenerationInfo(topicId.GenId())
	if topicGen == nil {
		log.Error().Msgf("Generation %s could not be retrieved to create a reader", topicId.GenId())
		return nil
	} else if topicGen.Leader != topology.MyOrdinal() {
		isLeader = false
	}

	var replicationReader ReplicationReader
	if isLeader {
		log.Info().Msgf("Creating reader for group %s and topic %s", q.group, &topicId)
	} else {
		log.Info().Msgf("Creating reader for group %s and topic %s as follower", q.group, &topicId)
		replicationReader = q.rrFactory.GetOrCreate(&topicId, topology, topicGen, q.offsetState)
	}

	reader, err := NewSegmentReader(
		q.group,
		isLeader,
		replicationReader,
		topicId,
		source.Id(),
		offset.Offset,
		q.offsetState,
		maxProducedOffset,
		q.config)

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
