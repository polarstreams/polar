package ownership

import (
	"fmt"
	"math"
	"math/rand"
	"sync/atomic"
	"time"

	"github.com/jorgebay/soda/internal/discovery"
	"github.com/jorgebay/soda/internal/types"
	"github.com/rs/zerolog/log"
)

const baseDelayMs = 150
const maxDuration time.Duration = 1<<63 - 1
const maxInitElapsed = 10 * time.Second

type Generator interface {
	types.Initializer
	StartGenerations()
}

type generator struct {
	// items can be a local or remote
	items      chan genMessage
	discoverer discovery.Discoverer
}

type genMessage struct {
	generation types.Generation
	result     chan error
}

func NewGenerator(discoverer discovery.Discoverer) Generator {
	o := &generator{
		discoverer: discoverer,
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

		newGeneration := types.Generation{
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
			o.processMessage(message)
			atomic.StoreInt32(processingFlag, 0)
		}()
	}
}

func (o *generator) processMessage(message genMessage) {
	// Consider a channel if state is needed across multiple items, i.e. "serialItems"
	if message.generation.Leader == o.discoverer.GetBrokerInfo().Ordinal {
		o.processLocalGeneration(message)
	} else {
		log.Debug().Msg("Processing a generation item started remotely")
	}
}

func (o *generator) processLocalGeneration(message genMessage) {
	log.Debug().Msg("Processing a generation started locally")

	// Create to followers
}

func getDelay() time.Duration {
	return time.Duration(rand.Intn(baseDelayMs)+baseDelayMs) * time.Millisecond
}
