package interbroker

import (
	"fmt"
	"time"

	"github.com/jorgebay/soda/internal/types"
	"github.com/rs/zerolog/log"
	"golang.org/x/net/context"
)

const replicationTimeout = 1 * time.Second

func (g *gossiper) SendToFollowers(
	replicationInfo types.ReplicationInfo,
	topic types.TopicDataId,
	segmentId uint64,
	chunk types.SegmentChunk,
) error {
	peers, ok := g.connections.Load().(clientMap)
	if !ok {
		log.Error().Msg("Peer clients are not loaded")
		return fmt.Errorf("Peer clients are not loaded")
	}
	sent := 0
	response := make(chan dataResponse, 1)

	// The provided body is only valid for the lifetime of this call
	// By using a context, we make sure the client doesn't use the
	// body after this function returned (e.g. timed out or one of the replicas responded with success)
	ctxt, cancel := context.WithTimeout(context.Background(), replicationTimeout)
	defer cancel()

	request := &dataRequest{
		meta: dataRequestMeta{
			SegmentId:    segmentId,
			Token:        topic.Token,
			GenId:        topic.GenId,
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
		c, ok := peers[broker.Ordinal]
		if !ok {
			log.Error().Msgf("No data client found for peer with ordinal %d", broker.Ordinal)
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

		if eResponse, ok := r.(*emptyResponse); ok && eResponse.op == dataReplicationResponseOp {
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
