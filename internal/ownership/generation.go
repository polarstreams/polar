package ownership

import (
	"fmt"
	"math"
	"math/rand"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/jorgebay/soda/internal/discovery"
	"github.com/jorgebay/soda/internal/interbroker"
	"github.com/jorgebay/soda/internal/localdb"
	. "github.com/jorgebay/soda/internal/types"
	"github.com/rs/zerolog/log"
)

const (
	baseDelayMs                     = 150
	maxDuration       time.Duration = 1<<63 - 1
	maxInitElapsed                  = 10 * time.Second
	replicationFactor               = 3
)

var emptyGeneration = Generation{Start: math.MaxInt64, Version: 0, Status: StatusCancelled}

type Generator interface {
	Initializer
	StartGenerations()
}

type generator struct {
	// items can be a local or remote
	items      chan genMessage
	discoverer discovery.TopologyGetter
	gossiper   interbroker.GenerationGossiper
	localDb    localdb.Client
	nextUuid   func() uuid.UUID // allow injecting it from tests
}

func NewGenerator(discoverer discovery.TopologyGetter, gossiper interbroker.GenerationGossiper, localDb localdb.Client) Generator {
	o := &generator{
		discoverer: discoverer,
		gossiper:   gossiper,
		localDb:    localDb,
		nextUuid:   nextUuid,
		items:      make(chan genMessage),
	}

	go o.process()
	return o
}

func nextUuid() uuid.UUID {
	return uuid.New()
}

func (o *generator) Init() error {
	// At this point in time, the cluster has been discovered but no connection to peers have been made
	o.gossiper.RegisterGenListener(o)
	return nil
}

func (o *generator) OnNewRemoteGeneration(existing *Generation, new *Generation) error {
	//TODO: Create message
	return nil
}

func (o *generator) OnRemoteSetAsAccepted(newGen *Generation) error {
	//TODO: Create message to channel
	return nil
}

func (o *generator) StartGenerations() {
	started := make(chan bool, 1)
	go func() {
		started <- true
		if err := o.startNew(maxInitElapsed); err != nil {
			log.Warn().Msgf("Initial generation could not be created after %.0fs", maxInitElapsed.Seconds())

			// TODO: Retry or Panic
		}
	}()
	<-started
}

func (o *generator) startNew(maxElapsed time.Duration) error {

	//TODO: Rewrite everything
	self := o.discoverer.LocalInfo()
	brokers := o.discoverer.Brokers()
	followers := make([]int, int(math.Min(float64(len(brokers)), replicationFactor-1)))

	// Get the next peers in the ring as followers
	for i := 0; i < len(followers); i++ {
		followers[i] = brokers[(self.Ordinal+1+i)%len(brokers)].Ordinal
	}

	// initial delay
	time.Sleep(time.Duration(baseDelayMs*self.Ordinal) * time.Millisecond)
	start := time.Now()

	for i := 0; ; i++ {
		if time.Since(start) > maxElapsed {
			return fmt.Errorf(
				"New generation creation timed out after %d attempts and %.2fs",
				i, time.Since(start).Seconds())
		}

		newGeneration := Generation{
			Start:     o.discoverer.TokenByOrdinal(self.Ordinal),
			End:       o.discoverer.TokenByOrdinal((self.Ordinal + 1) % len(brokers)),
			Leader:    self.Ordinal,
			Followers: followers,
		}

		log.Debug().Msgf("Starting new generation for token %d with followers %v", newGeneration.Start, followers)

		message := localGenMessage{
			generation: newGeneration,
			result:     make(chan error, 1),
		}

		o.items <- &message
		if err := <-message.result; err == nil {
			log.Info().Msg("New generation for token %d was created")
			break
		}

		if i > 0 && (i+1)%3 == 0 {
			log.Warn().
				Msgf("New generation for token %d could not be created after %d attempts", newGeneration.Start, i+1)
		}

		time.Sleep(getDelay())
	}

	return nil
}

// process Processes events in order.
//
// Reject (not block) concurrent event processing to allow it to be retried quickly.
// Friendly rejecting concurrency prevents deadlocks (and longer timed out requests), with the
// addition of delayed retries, results in a less "racy" end result.
func (o *generator) process() {
	processingFlag := new(int32)
	for message := range o.items {
		canProcess := atomic.CompareAndSwapInt32(processingFlag, 0, 1)
		if !canProcess {
			// Fast reject
			message.setResult(fmt.Errorf("Concurrent generation creation rejected"))
			continue
		}

		// Process it in the background
		message := message
		go func() {
			message.setResult(o.processGeneration(message))
			atomic.StoreInt32(processingFlag, 0)
		}()
	}
}

// processGeneration() returns nil when the generation was created, otherwise an error.
func (o *generator) processGeneration(message genMessage) error {
	// Consider a channel if state is needed across multiple items, i.e. "serialItems"
	if localGen, ok := message.(*localGenMessage); ok {
		return o.processLocal(localGen.generation)
	} else {
		m := message.(*remoteGenMessage)
		return o.processRemote(m.existing, m.new)
	}
}

func (o *generator) setAsAccepted(newGen *Generation) error {
	//TODO: Implement
	return nil
}

func (o *generator) processRemote(existing *Generation, new *Generation) error {
	log.Debug().Msg("Processing a generation item started remotely")

	return o.localDb.UpsertGeneration(existing, new)
}

func checkState(gens []Generation, accepted, proposed *Generation) (*Generation, *Generation) {
	for _, gen := range gens {
		if gen.Status == StatusAccepted {
			if gen.Version > accepted.Version {
				accepted = &gen
			}
		} else if gen.Status == StatusProposed {
			if gen.Version > proposed.Version {
				proposed = &gen
			}
		}
	}
	return accepted, proposed
}

func getDelay() time.Duration {
	return time.Duration(rand.Intn(baseDelayMs)+baseDelayMs) * time.Millisecond
}
