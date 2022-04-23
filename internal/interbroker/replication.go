package interbroker

import (
	"fmt"
	"io"
	"time"

	. "github.com/barcostreams/barco/internal/types"
	"github.com/rs/zerolog/log"
	"golang.org/x/net/context"
)

const replicationTimeout = 1 * time.Second
const streamFileTimeout = 10 * time.Second

func (g *gossiper) SendToFollowers(
	replicationInfo ReplicationInfo,
	topic TopicDataId,
	segmentId int64,
	chunk SegmentChunk,
) error {
	peerClients, ok := g.connections.Load().(clientMap)
	if !ok {
		log.Error().Msg("Peer clients are not loaded")
		return fmt.Errorf("Peer clients are not loaded")
	}
	if g.config.DevMode() {
		return nil
	}

	sent := 0
	response := make(chan dataResponse, 1)

	// The provided body is only valid for the lifetime of this call
	// By using a context, we make sure the client doesn't use the
	// body after this function returned (e.g. timed out or one of the replicas responded with success)
	ctxt, cancel := context.WithTimeout(context.Background(), replicationTimeout)
	defer cancel()

	request := &chunkReplicationRequest{
		meta: dataRequestMeta{
			SegmentId:    segmentId,
			Token:        topic.Token,
			GenVersion:   topic.Version,
			RangeIndex:   topic.RangeIndex,
			StartOffset:  chunk.StartOffset(),
			RecordLength: chunk.RecordLength(),
			TopicLength:  uint8(len(topic.Name)),
		},
		topic:    topic.Name,
		ctxt:     ctxt,
		data:     chunk.DataBlock(),
		response: response,
	}

	for _, broker := range replicationInfo.Followers {
		c, ok := peerClients[broker.Ordinal]
		if !ok {
			log.Error().Msgf("No data client found for B%d", broker.Ordinal)
			continue
		}
		sent += 1
		go func() {
			c.dataMessages <- request
		}()
	}

	if sent == 0 {
		return fmt.Errorf("Chunk for topic %s (%d) could not be sent to replicas", topic.Name, segmentId)
	}

	// Return as soon there's a successful response
	for i := 0; i < sent; i++ {
		var r dataResponse
		select {
		case <-ctxt.Done():
			return ctxt.Err()
		case r = <-response:
			break
		}

		if eResponse, isError := r.(*errorResponse); isError {
			if i < sent-1 {
				// Let's wait for the next response
				continue
			}

			err := fmt.Errorf("Received error when replicating: %s", eResponse.message)
			log.Debug().Err(err).Msg("Data could not be replicated")
			return err
		}

		if eResponse, ok := r.(*emptyResponse); ok && eResponse.op == chunkReplicationResponseOp {
			return nil
		} else {
			if i < sent-1 {
				// Let's wait for the next response
				continue
			}

			log.Error().Msg("Unexpected response from data server")
			return fmt.Errorf("Invalid response from the data server")
		}
	}

	return nil
}

func (g *gossiper) StreamFile(
	peers []int,
	segmentId int64,
	topic *TopicDataId,
	startOffset int64,
	maxRecords int,
	buf []byte,
) (int, error) {
	peerClients, ok := g.connections.Load().(clientMap)
	if !ok {
		log.Error().Msg("Peer clients are not loaded")
		return 0, fmt.Errorf("Peer clients are not loaded")
	}

	response := make(chan dataResponse, 1)
	ctxt, cancel := context.WithTimeout(context.Background(), streamFileTimeout)
	defer cancel()

	request := &fileStreamRequest{
		meta: dataRequestMeta{
			SegmentId:    segmentId,
			Token:        topic.Token,
			GenVersion:   topic.Version,
			RangeIndex:   topic.RangeIndex,
			StartOffset:  startOffset,
			RecordLength: uint32(maxRecords),
			TopicLength:  uint8(len(topic.Name)),
		},
		maxSize:  uint32(len(buf)),
		topic:    topic.Name,
		ctxt:     ctxt,
		response: response,
	}

	lastError := "<empty>"

	for _, ordinal := range peers {
		c, ok := peerClients[ordinal]
		if !ok {
			log.Error().Msgf("No data client found for B%d", ordinal)
			continue
		}
		c.dataMessages <- request
		var r dataResponse
		select {
		case <-ctxt.Done():
			return 0, ctxt.Err()
		case r = <-response:
			break
		}

		if eResponse, isError := r.(*errorResponse); isError {
			lastError = eResponse.message
			log.Debug().Str("err", lastError).Msg("Data could not be streamed from peer")
			continue
		}

		if res, ok := r.(*fileStreamResponse); ok && res.op == fileStreamResponseOp {
			n, err := io.ReadFull(res.reader, buf)
			if err != nil {
				log.Warn().Err(err).Msg("There was an error streaming data from peer")
			}
			if n > 0 {
				return n, nil
			}
		}
	}

	return 0, fmt.Errorf("Data could not be retrieved from any of the peers, last error: %s", lastError)
}
