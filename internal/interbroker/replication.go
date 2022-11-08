package interbroker

import (
	"fmt"
	"sync"
	"time"

	. "github.com/barcostreams/barco/internal/types"
	"github.com/rs/zerolog/log"
	"golang.org/x/net/context"
)

const streamFileTimeout = 10 * time.Second

func (g *gossiper) SendToFollowers(
	replicationInfo ReplicationInfo,
	topic TopicDataId,
	segmentId int64,
	chunk SegmentChunk,
) error {
	if g.config.DevMode() {
		return nil
	}
	peerClients, ok := g.connections.Load().(clientMap)
	if !ok {
		log.Error().Msg("Peer clients are not loaded")
		return fmt.Errorf("Peer clients are not loaded")
	}

	sent := make([]*chunkReplicationRequest, 0)
	response := make(chan dataResponse, len(replicationInfo.Followers))

	// The provided body is only valid for the lifetime of this call
	// We should  make sure the client doesn't use the body after this function returned
	// (e.g. timed out or one of the replicas responded with success)
	wg := sync.WaitGroup{}

	meta := dataRequestMeta{
		SegmentId:    segmentId,
		Token:        topic.Token,
		GenVersion:   topic.Version,
		RangeIndex:   topic.RangeIndex,
		StartOffset:  chunk.StartOffset(),
		RecordLength: chunk.RecordLength(),
		TopicLength:  uint8(len(topic.Name)),
	}

	for _, broker := range replicationInfo.Followers {
		c, ok := peerClients[broker.Ordinal]
		if !ok {
			log.Error().Msgf("No data client found for B%d", broker.Ordinal)
			continue
		}

		wg.Add(1)
		request := &chunkReplicationRequest{
			meta:     meta,
			topic:    topic.Name,
			data:     chunk.DataBlock(),
			writeWg:  &wg,
			response: response,
		}
		sent = append(sent, request)

		// Capture it
		requestChan := c.dataMessages
		go func() {
			requestChan <- request
		}()
	}

	if len(sent) == 0 {
		return fmt.Errorf("Chunk for topic %s (%d) could not be sent to replicas", topic.Name, segmentId)
	}

	timer := time.NewTimer(g.config.ReplicationTimeout())
	writeTimer := time.NewTimer(g.config.ReplicationWriteTimeout())
	lastResponseIndex := len(sent) - 1

	// Return as soon there's a successful response
	for i := 0; i < len(sent); i++ {
		var r dataResponse
		timedOut := false
		select {
		case <-timer.C:
			timedOut = true
		case r = <-response:
			timer.Stop()
		}

		if timedOut {
			// Cancel all request write
			for _, r := range sent {
				r.TrySetAsWritten()
			}
			return fmt.Errorf("Sending to followers timed out")
		}

		if eResponse, isError := r.(*errorResponse); isError {
			err := fmt.Errorf("Received error when replicating: %s", eResponse.message)
			log.Debug().Err(err).Msg("Data could not be replicated on a replica")

			if i < lastResponseIndex {
				// Let's wait for the next response
				continue
			}

			return err
		}

		eResponse, ok := r.(*emptyResponse)
		if !ok || eResponse.op != chunkReplicationResponseOp {
			log.Error().Interface("response", r).Msg("Unexpected response from data server")
			if i < lastResponseIndex {
				// Let's wait for the next response
				continue
			}
			return fmt.Errorf("Invalid response from the data server")
		}

		// We have a valid response
		if len(sent) > 1 && i < lastResponseIndex {
			cancelTimer := make(chan bool, 1)
			go func() {
				select {
				case <-cancelTimer:
					return
				case <-writeTimer.C:
					break
				}

				// We should wait for the other requests to be written to the other replica
				for _, r := range sent {
					r.TrySetAsWritten()
				}
			}()

			wg.Wait()
			cancelTimer <- true
		}

		break
	}

	writeTimer.Stop()

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
		log.Error().Msg("Peer clients are not loaded for file streaming")
		return 0, fmt.Errorf("Peer clients are not loaded for file streaming")
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
		topic:       topic.Name,
		maxSize:     uint32(len(buf)),
		responseBuf: buf,
		response:    response,
	}

	lastError := "<empty>"

	// Send to the peers serially, the first that succeeds returns
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
			if res.readBytes > 0 {
				return res.readBytes, nil
			}
		}
	}

	return 0, fmt.Errorf("Data could not be retrieved from any of the peers, last error: %s", lastError)
}
