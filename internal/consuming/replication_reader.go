package consuming

import (
	"sync"
	"time"

	"github.com/polarstreams/polar/internal/interbroker"
	. "github.com/polarstreams/polar/internal/types"
	"github.com/rs/zerolog/log"
)

const maxSyncWait = 500 * time.Millisecond
const retryDelay = 1 * time.Second

// A replication reader that reads and consolidates the file structure only once
type replicationReader struct {
	topic     *TopicDataId
	peers     []int
	gossiper  interbroker.Gossiper
	offset    int64
	mergeOnce sync.Once
}

func newReplicationReader(
	topic TopicDataId,
	topology *TopologyInfo,
	gen *Generation,
	gossiper interbroker.Gossiper,
) *replicationReader {
	// Set the peers that need to be involved in the streaming of file structure
	peers := make([]int, 0, 2)
	if gen.Leader != topology.MyOrdinal() {
		peers = append(peers, gen.Leader)
	}
	for _, ordinal := range gen.Followers {
		if ordinal != topology.MyOrdinal() {
			peers = append(peers, ordinal)
		}
	}

	r := &replicationReader{
		topic:    &topic,
		peers:    peers,
		gossiper: gossiper,
		offset:   0, // We read the file names, starting at offset zero
	}

	return r
}

func (r *replicationReader) MergeFileStructure() (bool, error) {
	c := make(chan bool)
	go func() {
		r.mergeOnce.Do(r.mergeFileOnce)
		c <- true
	}()

	done := false

	// If it's something that can be awaited upon for a small period of time, ok
	// Otherwise, the reader will call later in time
	select {
	case done = <-c:
	case <-time.After(maxSyncWait):
	}

	return done, nil
}

func (r *replicationReader) mergeFileOnce() {
	for {
		log.Info().Msgf("Merging topic files for %s", r.topic)
		err := r.gossiper.MergeTopicFiles(r.peers, r.topic, r.offset)
		if err == nil {
			// Successfully merged
			break
		}

		log.Warn().Msgf("Topic files for %s could not be merged, retrying: %s", r.topic, err)
		time.Sleep(retryDelay)
	}
}

func (r *replicationReader) StreamFile(
	segmentId int64,
	topic *TopicDataId,
	startOffset int64,
	maxRecords int,
	buf []byte,
) (int, error) {
	return r.gossiper.StreamFile(r.peers, segmentId, topic, startOffset, maxRecords, buf)
}
