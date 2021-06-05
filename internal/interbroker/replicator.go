package interbroker

import (
	"fmt"

	"github.com/jorgebay/soda/internal/types"
	"github.com/rs/zerolog/log"
)

type Replicator interface {
	// Sends a message to be stored as replica of current broker's datalog
	SendToFollowers(replicationInfo types.ReplicationInfo, topic types.TopicDataId, segmentId int64, body []byte) error
}

func (g *gossiper) SendToFollowers(
	replicationInfo types.ReplicationInfo,
	topic types.TopicDataId,
	segmentId int64,
	body []byte,
) error {
	peers, ok := g.connections.Load().(clientMap)
	if !ok {
		log.Error().Msg("Peer clients are not loaded")
		return fmt.Errorf("Peer clients are not loaded")
	}
	sent := 0
	response := make(chan dataResponse, 1)
	for _, broker := range replicationInfo.Followers {
		c, ok := peers[broker.Ordinal]
		if !ok {
			log.Error().Msgf("No data client found for peer with ordinal %d", broker.Ordinal)
			continue
		}
		sent += 1
		request := &dataRequest{
			meta: dataRequestMeta{
				segmentId:   segmentId,
				token:       topic.Token,
				genId:       topic.GenId,
				topicLength: uint8(len(topic.Name)),
			},
			topic: topic.Name,
			// TODO: Copy the byte slice, otherwise is unsafe!
			data:     body,
			response: response,
		}
		go func() {
			c.dataMessages <- request
		}()
	}

	if sent == 0 {
		return fmt.Errorf("Chunk for topic %s (%d) could not be sent to replicas", topic.Name, segmentId)
	}

	// Return as soon there's a successful response
	for i := 0; i < sent; i++ {
		r := <-response

		if eResponse, ok := r.(*errorResponse); ok {
			if i < sent-1 {
				// Let's wait for the next response
				continue
			}

			err := fmt.Errorf("Received error when replicating: %s", eResponse.message)
			log.Debug().Err(err).Msg("Data could not be replicated")
			return err
		}

		if eResponse, ok := r.(*emptyResponse); ok && eResponse.op == dataResponseOp {
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
