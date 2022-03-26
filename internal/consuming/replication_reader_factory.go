package consuming

import (
	"github.com/barcostreams/barco/internal/data"
	"github.com/barcostreams/barco/internal/interbroker"
	. "github.com/barcostreams/barco/internal/types"
	. "github.com/barcostreams/barco/internal/utils"
)

type replicationReaderFactory struct {
	readers  *CopyOnWriteMap
	gossiper interbroker.Gossiper
}

func newReplicationReaderFactory(gossiper interbroker.Gossiper) *replicationReaderFactory {
	return &replicationReaderFactory{
		readers:  NewCopyOnWriteMap(),
		gossiper: gossiper,
	}
}

func (f *replicationReaderFactory) GetOrCreate(
	topic *TopicDataId,
	topology *TopologyInfo,
	topicGen *Generation,
	offsetState OffsetState,
) data.ReplicationReader {
	reader, _, _ := f.readers.LoadOrStore(*topic, func() (interface{}, error) {
		minOffset := offsetState.MinOffset(topic.Name, topic.Token, topic.RangeIndex)
		return newReplicationReader(*topic, topology, topicGen, f.gossiper, minOffset), nil
	})
	return reader.(data.ReplicationReader)
}
