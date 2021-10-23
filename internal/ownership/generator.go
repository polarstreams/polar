package ownership

import (
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
	baseDelay       = 150 * time.Millisecond
	maxDelay          = 300 * time.Millisecond
	replicationFactor = 3
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

func (o *generator) OnRemoteSetAsProposed(newGen *Generation, expectedTx *uuid.UUID) error {
	// Create message to channel
	message := remoteGenProposedMessage{
		gen:        newGen,
		expectedTx: expectedTx,
		result:     make(chan error),
	}
	o.items <- &message
	return <-message.result
}

func (o *generator) OnRemoteSetAsCommitted(token Token, tx uuid.UUID, origin int) error {
	message := remoteGenCommittedMessage{
		token:  token,
		tx:     tx,
		origin: origin,
		result: make(chan error),
	}
	o.items <- &message
	return <-message.result
}

func (o *generator) StartGenerations() {
	started := make(chan bool, 1)
	go func() {
		started <- true
		o.startNew()
	}()
	<-started
}

func (o *generator) startNew() {
	reason := o.determineStartReason()

	if reason == scalingUp {
		// TODO: Send a message to broker n-1 to start the process
		// of splitting the token range
		return
	}

	message := localGenMessage{
		reason: reason,
		result: make(chan creationError),
	}

	// Send to the processing queue
	o.items <- &message

	for err := <-message.result; err != nil; {
		log.Err(err).Msg("Could not create generation when starting")

		if !err.canBeRetried() {
			log.Panic().Err(err).Msg("Non retryable error found when starting")
		}
		time.Sleep(getDelay())

		o.items <- &message
	}
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
			message.setResult(newCreationError("Concurrent generation creation rejected"))
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
func (o *generator) processGeneration(message genMessage) creationError {
	// Consider a channel if state is needed across multiple items, i.e. "serialItems"
	if m, ok := message.(*localGenMessage); ok {
		return o.processLocal(m)
	}

	if m, ok := message.(*remoteGenProposedMessage); ok {
		return o.processRemoteProposed(m)
	}

	if m, ok := message.(*remoteGenCommittedMessage); ok {
		return o.processRemoteCommitted(m)
	}

	log.Panic().Msg("Unhandled generation internal message type")
	return nil
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

// getDelay gets a value between base delay and max delay
func getDelay() time.Duration {
	return time.Duration(rand.Intn(int(baseDelay.Milliseconds()))) * time.Millisecond + baseDelay
}
