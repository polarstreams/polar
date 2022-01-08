package consuming

import (
	"encoding/binary"
	"io"
	"net/http"

	. "github.com/google/uuid"
	"github.com/jorgebay/soda/internal/conf"
	. "github.com/jorgebay/soda/internal/data"
	"github.com/jorgebay/soda/internal/discovery"
	. "github.com/jorgebay/soda/internal/types"
	"github.com/jorgebay/soda/internal/utils"
	"github.com/rs/zerolog/log"
)

// Receives read requests on a single thread and dispatches them
// track outstanding per topic/group
// close segments / groups
type groupReadQueue struct {
	items          chan readQueueItem
	group          string
	state          *ConsumerState
	offsetState    OffsetState
	topologyGetter discovery.TopologyGetter
	config         conf.DatalogConfig
	readerIndex    uint16
	readers        map[Token]map[string]*SegmentReader // Readers per token and topic
}

func newGroupReadQueue(
	group string, state *ConsumerState,
	topologyGetter discovery.TopologyGetter,
	config conf.DatalogConfig,
) *groupReadQueue {
	queue := &groupReadQueue{
		items:          make(chan readQueueItem),
		group:          group,
		state:          state,
		topologyGetter: topologyGetter,
		config:         config,
	}
	go queue.process()
	return queue
}

type readQueueItem struct {
	connId  UUID
	writer  http.ResponseWriter
	refresh bool // Determines whether the item was meant for the read queue to re-evaluate internal maps
}

func (q *groupReadQueue) process() {
	for item := range q.items {
		if item.refresh {
			// TODO: Implement
			// Check which readers can be closed
			continue
		}

		group, tokens, topics := logsToServe(q.state, q.topologyGetter, item.connId)
		if group != q.group {
			// There was a change in topology, tell the client to poll again
			utils.NoContentResponse(item.writer, 0)
			continue
		}
		if len(tokens) == 0 || len(topics) == 0 {
			utils.NoContentResponse(item.writer, consumerNoOwnedDataDelay)
			continue
		}

		readers := q.getReaders(tokens, topics)
		responseItems := make([]consumerResponseItem, 0, 1)
		for i := 0; i < len(readers); i++ {
			// Use an incremental index to try to be fair between calls by round robin through readers
			reader := readers[int(q.readerIndex)%len(readers)]
			q.readerIndex++
			segmentReadItem := newSegmentReadItem()
			reader.Items <- segmentReadItem
			err, chunk := segmentReadItem.result()

			if err != nil {
				log.Warn().Err(err).Msgf("There was an error reading for %s", &reader.Topic)
				continue
			}
			responseItems = append(responseItems, consumerResponseItem{chunk: chunk, topic: reader.Topic})
		}

		if len(responseItems) == 0 {
			// No data to poll
			utils.NoContentResponse(item.writer, 0)
		} else {
			err := marshalResponse(item.writer, responseItems)
			if err != nil {
				// There was an error writing the response to the consumer
				// TODO: Do not move forward, use this responseItems for the next call
			} else {
				// TODO: Store the position of the consumer group
			}
		}
	}

	// normal read
	// check generation
	// recheck new segment file for new file within the same generation
	// look for generation after X (parent)
	// for item := range q.items {
	// 	// find the appropriate file
	// }

	// TODO: Check can consume from local token data
	// When there's an split token range, it should check with previous broker
	// To see whether it can serve to this consumer group (e.g. B3 should check with B0 about T3 for consumer group X)

	// TODO: Define single thread per consumer group? shared?
	// for _, topic := range topics {
	// 	for _, token := range tokens {
	// 		// TODO: get or create readers per group/topic/token
	// 		// Most likely issue reads in a single channel
	// 		fmt.Println("--Temp", token, topic, group)
	// 	}
	// }
}

func marshalResponse(w http.ResponseWriter, responseItems []consumerResponseItem) error {
	w.Header().Add("Content-Type", contentType)
	if err := binary.Write(w, conf.Endianness, uint16(len(responseItems))); err != nil {
		// There was an issue writing to the wire
		return err
	}
	for _, item := range responseItems {
		err := item.Marshal(w)
		if err != nil {
			return err
		}
	}

	return nil
}

func (q *groupReadQueue) readNext(connId UUID, w http.ResponseWriter) {
	q.items <- readQueueItem{
		connId: connId,
		writer: w,
	}
}

// Gets the readers, creating them if necessary
func (q *groupReadQueue) getReaders(tokens []Token, topics []string) []*SegmentReader {
	result := make([]*SegmentReader, 0, len(tokens)*len(topics))

	for _, token := range tokens {
		readersByToken, found := q.readers[token]
		if !found {
			readersByToken = make(map[string]*SegmentReader)
			q.readers[token] = q.readers[token]
		}

		for _, topic := range topics {
			reader, found := readersByToken[topic]
			if !found {
				log.Info().Msgf("Creating reader for token %d and topic '%s'", token, topic)
				offset := q.offsetState.Get(q.group, token)
				topicId := TopicDataId{
					Name:  topic,
					Token: token,
					GenId: offset.Version,
				}
				reader = NewSegmentReader(topicId, q.config)
				readersByToken[topic] = reader
			}
			result = append(result, reader)
		}
	}

	return result
}

type segmentReadItem struct {
	chunkResult chan SegmentChunk
	errorResult chan error
}

func newSegmentReadItem() *segmentReadItem {
	return &segmentReadItem{
		chunkResult: make(chan SegmentChunk),
		errorResult: make(chan error),
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

// func (r *dataRequest) Marshal(w types.StringWriter, header *header) {
// 	writeHeader(w, header)
// 	binary.Write(w, conf.Endianness, r.meta)
// 	w.WriteString(r.topic)
// 	w.Write(r.data)
// }
