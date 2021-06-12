package ownership

import (
	"fmt"
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
	// At this point in time, the cluster has been discovered
	return o.startNew(maxInitElapsed)
}

func (o *generator) startNew(maxElapsed time.Duration) error {
	self := o.discoverer.GetBrokerInfo()
	peers := o.discoverer.Peers()
	peerOrdinals := make([]int, len(peers))
	for i, p := range peers {
		peerOrdinals[i] = p.Ordinal
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
			Followers: peerOrdinals,
		}

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
	for range o.items {
		canProcess := atomic.CompareAndSwapInt32(processingFlag, 0, 1)
		if !canProcess {
			//TODO: REJECT
			// item.result <- err
			continue
		}

		// Process it in the background
		go func() {
			o.processItem()
			atomic.StoreInt32(processingFlag, 0)
		}()
	}
}

func (o *generator) processItem() {
	// Consider a channel if state is needed across multiple items, i.e. "serialItems"
}

func getDelay() time.Duration {
	return time.Duration(rand.Intn(baseDelayMs)+baseDelayMs) * time.Millisecond
}
