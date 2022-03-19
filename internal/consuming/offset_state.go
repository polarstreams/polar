package consuming

import (
	"sync"

	"github.com/barcostreams/barco/internal/conf"
	"github.com/barcostreams/barco/internal/data"
	"github.com/barcostreams/barco/internal/discovery"
	"github.com/barcostreams/barco/internal/interbroker"
	"github.com/barcostreams/barco/internal/localdb"
	. "github.com/barcostreams/barco/internal/types"
	"github.com/rs/zerolog/log"
)

func newDefaultOffsetState(
	localDb localdb.Client,
	topologyGetter discovery.TopologyGetter,
	gossiper interbroker.Gossiper,
	config conf.ConsumerConfig,
) OffsetState {
	state := &defaultOffsetState{
		offsetMap:      make(map[OffsetStoreKey]*Offset),
		commitChan:     make(chan *OffsetStoreKeyValue, 64),
		localDb:        localDb,
		gossiper:       gossiper,
		topologyGetter: topologyGetter,
		config:         config,
	}
	go state.processCommit()
	return state
}

type defaultOffsetState struct {
	offsetMap      map[OffsetStoreKey]*Offset
	mu             sync.RWMutex              // We need synchronization for doing CAS operation per key/value
	commitChan     chan *OffsetStoreKeyValue // We need to commit offset in order
	localDb        localdb.Client
	gossiper       interbroker.Gossiper
	topologyGetter discovery.TopologyGetter
	config         conf.ConsumerConfig
}

func (s *defaultOffsetState) Init() error {
	// Load local offsets into memory
	values, err := s.localDb.Offsets()
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	for _, kv := range values {
		s.offsetMap[kv.Key] = &kv.Value
	}

	return nil
}

func (s *defaultOffsetState) Get(group string, topic string, token Token, rangeIndex RangeIndex) *Offset {
	key := OffsetStoreKey{Group: group, Topic: topic, Token: token, RangeIndex: rangeIndex}
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
	commit OffsetCommitType,
) {
	key := OffsetStoreKey{Group: group, Topic: topic, Token: token, RangeIndex: rangeIndex}
	// We could use segment logs in the future
	s.mu.Lock()
	defer s.mu.Unlock()
	existingValue := s.offsetMap[key]

	if isOldValue(existingValue, &value) {
		// We have a newer value in our map, don't override
		return
	}

	s.offsetMap[key] = &value

	if commit != OffsetCommitNone {
		// Store commits locally in order but don't await for it to complete
		kv := &OffsetStoreKeyValue{Key: key, Value: value}
		s.commitChan <- kv

		if commit == OffsetCommitAll {
			// Send to followers in the background with no order guarantees
			// The local OffsetState of the follower will verify for new values
			go s.sendToFollowers(kv)
		}
	}
}

func (s *defaultOffsetState) processCommit() {
	for kv := range s.commitChan {
		if err := s.localDb.SaveOffset(kv); err != nil {
			log.Err(err).Msgf("Offset could not be stored in the local db")
		} else {
			log.Debug().Msgf(
				"Offset stored in the local db for group %s topic '%s' %d/%d",
				kv.Key.Group, kv.Key.Topic, kv.Key.Token, kv.Key.RangeIndex)
		}
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

func (s *defaultOffsetState) sendToFollowers(kv *OffsetStoreKeyValue) {
	gen := s.topologyGetter.Generation(kv.Key.Token)
	if gen == nil {
		log.Error().
			Stringer("token", kv.Key.Token).
			Msgf("Generation could not be retrieved when saving offset")
		return
	}

	topology := s.topologyGetter.Topology()

	for _, follower := range gen.Followers {
		if follower == topology.MyOrdinal() {
			continue
		}

		ordinal := follower
		go func() {
			err := s.gossiper.SendCommittedOffset(ordinal, kv)
			if err != nil {
				log.Err(err).Msgf("Offset could not be sent to follower B%d", ordinal)
			} else {
				log.Debug().Msgf(
					"Offset sent to follower B%d for group %s topic '%s' %d/%d",
					ordinal, kv.Key.Group, kv.Key.Topic, kv.Key.Token, kv.Key.RangeIndex)
			}
		}()
	}
}

func (s *defaultOffsetState) CanConsumeToken(group string, topic string, gen *Generation) bool {
	// parents := gen.Parents

	/*
		topologyGetter


		gen := d.GenerationInfo(token, version)
		if gen == nil {
			return nil
		}

		if current := d.Generation(token); current != nil && current.Version == version {
			return nil
		}

		nextGens, err := d.localDb.GenerationsByParent(gen)
		utils.PanicIfErr(err, "Generations by parent failed to be retrieved")

		// TODO: Handle the case where this token was joined with another one and v+1 does not exist

		return nextGens
	*/

	//TODO: Implement CanConsumeToken
	return true
}

func (s *defaultOffsetState) ProducerOffsetLocal(topic *TopicDataId) (uint64, error) {
	return data.ReadProducerOffset(topic, s.config)
}
