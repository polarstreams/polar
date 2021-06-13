package ownership

import (
	"errors"
	"fmt"
	"math"
	"math/rand"
	"sync/atomic"
	"time"

	"github.com/jorgebay/soda/internal/discovery"
	"github.com/jorgebay/soda/internal/interbroker"
	"github.com/jorgebay/soda/internal/localdb"
	. "github.com/jorgebay/soda/internal/types"
	"github.com/rs/zerolog/log"
)

const baseDelayMs = 150
const maxDuration time.Duration = 1<<63 - 1
const maxInitElapsed = 10 * time.Second

type status int

var emptyGeneration = Generation{Token: math.MaxInt64, Version: -1, Status: int(statusCancelled)}

const (
	statusCancelled status = iota
	statusProposed
	statusAccepted
)

type Generator interface {
	Initializer
	StartGenerations()
}

type generator struct {
	// items can be a local or remote
	items      chan genMessage
	discoverer discovery.Discoverer
	gossiper   interbroker.GenerationGossiper
	localDb    localdb.Client
}

type genMessage struct {
	generation Generation
	result     chan error
}

func NewGenerator(discoverer discovery.Discoverer, gossiper interbroker.GenerationGossiper, localDb localdb.Client) Generator {
	o := &generator{
		discoverer: discoverer,
		gossiper:   gossiper,
		localDb:    localDb,
		items:      make(chan genMessage),
	}

	go o.process()
	return o
}

func (o *generator) Init() error {
	// At this point in time, the cluster has been discovered but no connection to peers have been made
	return nil
}

func (o *generator) StartGenerations() {
	started := make(chan bool, 1)
	go func() {
		started <- true
		if err := o.startNew(maxInitElapsed); err != nil {
			log.Warn().Msgf("Initial generation could not be created after %.0fs", maxInitElapsed.Seconds())

			// TODO: Retry
		}
	}()
	<-started
}

func (o *generator) startNew(maxElapsed time.Duration) error {
	self := o.discoverer.GetBrokerInfo()
	peers := o.discoverer.Peers()
	followers := make([]int, int(math.Min(float64(len(peers)), 2)))

	// Get the next peers in the ring as followers
	for i := range followers {
		followers[i] = peers[(self.Ordinal+1+i)%len(followers)].Ordinal
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
			Token:     o.discoverer.TokenByOrdinal(self.Ordinal),
			Leader:    self.Ordinal,
			Followers: followers,
		}

		log.Debug().Msgf("Starting new generation %v", newGeneration)

		message := genMessage{
			generation: newGeneration,
			result:     make(chan error, 1),
		}

		o.items <- message
		if err := <-message.result; err == nil {
			log.Info().Msg("New generation for token %d was created")
			break
		}

		if i > 0 && (i+1)%3 == 0 {
			log.Warn().
				Msgf("New generation for token %d could not be created after %d attempts", newGeneration.Token, i+1)
		}

		time.Sleep(getDelay())
	}

	return nil
}

func (o *generator) process() {
	// Process events in order
	// Reject (not block) concurrent event processing to allow it to be retried.
	// Friendly rejecting concurrency prevents deadlocks (and longer timed out requests), with the
	// addition of delayed retries, results in a less "racy" end result.
	processingFlag := new(int32)
	for message := range o.items {
		canProcess := atomic.CompareAndSwapInt32(processingFlag, 0, 1)
		if !canProcess {
			// Fast reject
			message.result <- fmt.Errorf("Concurrent generation creation rejected")
			continue
		}

		// Process it in the background
		message := message
		go func() {
			message.result <- o.processGeneration(message.generation)
			atomic.StoreInt32(processingFlag, 0)
		}()
	}
}

// processGeneration() returns nil when the generation was created, otherwise an error.
func (o *generator) processGeneration(gen Generation) error {
	// Consider a channel if state is needed across multiple items, i.e. "serialItems"
	if gen.Leader == o.discoverer.GetBrokerInfo().Ordinal {
		return o.processLocalGeneration(gen)
	} else {
		log.Debug().Msg("Processing a generation item started remotely")
		return nil
	}
}

func (o *generator) processLocalGeneration(gen Generation) error {
	log.Debug().Msg("Processing a generation started locally")

	accepted := &emptyGeneration
	proposed := &emptyGeneration

	// Retrieve locally
	if gens, err := o.localDb.GetGenerationsByToken(gen.Token); err != nil {
		log.Fatal().Err(err).Msg("Generations could not be retrieved")
	} else {
		accepted, proposed = checkState(gens, accepted, proposed)
	}

	// Retrieve from followers
	for _, follower := range gen.Followers {
		gens, err := o.gossiper.GetGenerations(follower, gen.Token)
		if err != nil {
			log.Debug().Msgf("Error when trying to retrieve generations from broker %d: %s", follower, err.Error())
			continue
		}

		log.Debug().Msgf("Obtained generations %v from broker %d", gens, follower)
		accepted, proposed = checkState(gens, accepted, proposed)
	}

	if proposed != &emptyGeneration {
		// TODO: Check whether it's old, cancel and override
		log.Info().Msgf("There's already a previous proposed generation TODO FIX")
		return errors.New("There's already a proposed generation for this token")
	}

	//TODO: Continue

	return nil
}

func checkState(gens []Generation, accepted, proposed *Generation) (*Generation, *Generation) {
	for _, gen := range gens {
		if status(gen.Status) == statusAccepted {
			if gen.Version > accepted.Version {
				accepted = &gen
			}
		} else if status(gen.Status) == statusProposed {
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
