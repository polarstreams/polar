package consuming

import (
	"sync"

	"github.com/jorgebay/soda/internal/discovery"
	"github.com/jorgebay/soda/internal/interbroker"
	"github.com/jorgebay/soda/internal/localdb"
	. "github.com/jorgebay/soda/internal/types"
	"github.com/rs/zerolog/log"
)

func newDefaultOffsetState(
	localDb localdb.Client,
	topologyGetter discovery.TopologyGetter,
	gossiper interbroker.Gossiper,
) OffsetState {
	state := &defaultOffsetState{
		offsetMap:      map[offsetStoreKey]*Offset{},
		commitChan:     make(chan *offsetStoreKeyValue, 64),
		localDb:        localDb,
		gossiper:       gossiper,
		topologyGetter: topologyGetter,
	}
	go state.processCommit()
	return state
}

type defaultOffsetState struct {
	offsetMap      map[offsetStoreKey]*Offset
	mu             sync.RWMutex              // We need synchronization for doing CAS operation per key/value
	commitChan     chan *offsetStoreKeyValue // We need to commit offset in order
	localDb        localdb.Client
	gossiper       interbroker.Gossiper
	topologyGetter discovery.TopologyGetter
}

func (s *defaultOffsetState) Get(group string, topic string, token Token, rangeIndex RangeIndex) *Offset {
	key := offsetStoreKey{group, topic, token, rangeIndex}
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.offsetMap[key]
}

func (s *defaultOffsetState) Set(
	group string,
	topic string,
	token Token,
	rangeIndex RangeIndex,
	value Offset,
	commit bool,
) {
	key := offsetStoreKey{group, topic, token, rangeIndex}
	// We could use segment logs in the future
	s.mu.Lock()
	defer s.mu.Unlock()
	existingValue := s.offsetMap[key]

	if isOldValue(existingValue, &value) {
		// We have a newer value in our map, don't override
		return
	}

	s.offsetMap[key] = &value

	if commit {
		// Process commits in order but don't await for it to complete
		s.commitChan <- &offsetStoreKeyValue{key, value}
	}
}

func (s *defaultOffsetState) processCommit() {
	for kv := range s.commitChan {
		key := kv.key
		if err := s.localDb.SaveOffset(key.group, key.topic, key.token, key.rangeIndex, kv.value); err != nil {
			log.Err(err).Msgf("Offset could not be stored in the local db")
		} else {
			log.Debug().Msgf(
				"Offset stored in the local db for group %s topic '%s' %d/%d", key.group, key.topic, key.token, key.rangeIndex)
		}

		// Send to followers in the background with no order guarantees
		// The local OffsetState of the follower will verify for new values
		go s.sendToFollowers(kv)
	}
}

func isOldValue(existing *Offset, newValue *Offset) bool {
	if existing == nil {
		// There's no previous value
		return false
	}
	if existing.Source < newValue.Source {
		// The source of the previous value is old
		return false
	}

	if existing.Source == newValue.Source {
		if existing.Version < newValue.Version {
			return false
		}
		if existing.Version == newValue.Version && existing.Offset <= newValue.Offset {
			return false
		}
	}

	return true
}

func (s *defaultOffsetState) sendToFollowers(kv *offsetStoreKeyValue) {
	gen := s.topologyGetter.Generation(kv.key.token)
	if gen == nil {
		log.Error().
			Stringer("token", kv.key.token).
			Msgf("Generation could not be retrieved when saving offset")
		return
	}

	topology := s.topologyGetter.Topology()
	key := kv.key

	for _, follower := range gen.Followers {
		if follower == topology.MyOrdinal() {
			continue
		}

		ordinal := follower
		go func() {
			err := s.gossiper.SendCommittedOffset(ordinal, key.group, key.topic, key.token, key.rangeIndex, kv.value)
			if err != nil {
				log.Err(err).Int("ordinal", ordinal).Msgf("Offset could not be sent to follower")
			} else {
				log.Debug().Int("ordinal", ordinal).Msgf("Offset sent to follower")
			}
		}()
	}
}

func (s *defaultOffsetState) CanConsumeToken(group string, topic string, gen Generation) bool {
	//TODO: Implement CanConsumeToken
	return true
}

type offsetStoreKey struct {
	group      string
	topic      string
	token      Token
	rangeIndex RangeIndex
}

type offsetStoreKeyValue struct {
	key   offsetStoreKey
	value Offset
}
