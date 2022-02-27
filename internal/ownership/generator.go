package ownership

import (
	"fmt"
	"math"
	"math/rand"
	"sync/atomic"
	"time"

	"github.com/barcostreams/barco/internal/discovery"
	"github.com/barcostreams/barco/internal/interbroker"
	"github.com/barcostreams/barco/internal/localdb"
	. "github.com/barcostreams/barco/internal/types"
	"github.com/barcostreams/barco/internal/utils"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
)

const (
	baseDelay           = 150 * time.Millisecond
	maxDelay            = 300 * time.Millisecond
	maxWaitForPrevious  = 10 * time.Second
	waitForPreviousStep = 500 * time.Millisecond
	maxWaitForSplit     = 5 * time.Minute
	waitForSplitStep    = 5 * time.Second
	replicationFactor   = 3
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
	gossiper   interbroker.Gossiper
	localDb    localdb.Client
	nextUuid   func() uuid.UUID // allow injecting it from tests
}

func NewGenerator(discoverer discovery.TopologyGetter, gossiper interbroker.Gossiper, localDb localdb.Client) Generator {
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

func (o *generator) OnRemoteSetAsProposed(newGen *Generation, newGen2 *Generation, expectedTx *uuid.UUID) error {
	if newGen2 != nil && newGen2.Status != StatusAccepted {
		return fmt.Errorf("Multiple generations can only be accepted (not proposed)")
	}

	// Create message to channel
	message := remoteGenProposedMessage{
		gen:        newGen,
		gen2:       newGen2,
		expectedTx: expectedTx,
		result:     make(chan error),
	}
	o.items <- &message
	return <-message.result
}

func (o *generator) OnRemoteSetAsCommitted(token1 Token, token2 *Token, tx uuid.UUID, origin int) error {
	message := remoteGenCommittedMessage{
		token1: token1,
		token2: token2,
		tx:     tx,
		origin: origin,
		result: make(chan error),
	}
	o.items <- &message
	return <-message.result
}

func (o *generator) OnRemoteRangeSplitStart(origin int) error {
	topology := o.discoverer.Topology()

	if origin >= len(topology.Brokers) {
		return utils.CreateErrAndLog(
			"Received split range request from B%d but topology does not contain it (length: %d)",
			origin,
			len(topology.Brokers))
	}

	if origin != topology.NextBroker().Ordinal {
		return utils.CreateErrAndLog(
			"Received split range request from B%d but it's not the next broker (B%d)",
			origin,
			topology.NextBroker().Ordinal)
	}

	message := localSplitRangeGenMessage{
		topology: topology,
		origin:   origin,
		result:   make(chan creationError),
	}
	o.items <- &message
	return <-message.result
}

func (o *generator) StartGenerations() {
	// Register UP/DOWN handler on the main thread
	o.gossiper.RegisterHostUpDownListener(o)

	started := make(chan bool, 1)
	go func() {
		started <- true
		o.startNew()
	}()
	<-started
}

func (o *generator) OnHostDown(broker BrokerInfo) {
	// Check position of the down broker
	topology := o.discoverer.Topology()
	brokerIndex := topology.GetIndex(broker.Ordinal)
	followers := topology.NaturalFollowers(brokerIndex)
	if followers[0] != topology.MyOrdinal() {
		log.Debug().Msgf("Generator detected %s as DOWN but we are not the next broker in the ring", &broker)
		return
	}

	previousGen := o.discoverer.Generation(topology.GetToken(brokerIndex))
	if previousGen == nil {
		log.Warn().Msgf("Broker B%d detected down without owning a generation", broker.Ordinal)
		return
	}

	log.Info().Msgf("Generator detected %s as DOWN, trying to become the leader of T%d", &broker, broker.Ordinal)

	go func() {
		// If host comes back up, it's expected to try to retake its token
		for !o.gossiper.IsHostUp(broker.Ordinal) && !o.localDb.IsShuttingDown() {
			message := localFailoverGenMessage{
				brokerIndex: brokerIndex,
				broker:      broker,
				topology:    topology,
				previousGen: previousGen,
				result:      make(chan creationError, 1),
			}

			// Send to the processing queue
			o.items <- &message

			// Wait for the result
			err := <-message.result
			if err == nil {
				log.Debug().Msgf("Generator was able to become leader of T%d", broker.Ordinal)
				break
			}

			time.Sleep(baseDelay)
		}
	}()
}

func (o *generator) OnHostUp(broker BrokerInfo) {
	log.Debug().Msgf("Generator detected %s as UP", &broker)
}

func (o *generator) startNew() {
	topology := o.discoverer.Topology()

	if topology.MyOrdinal() != 0 {
		o.waitForPreviousRange(topology)
	}

	reason := o.determineStartReason()

	if reason == scalingUp {
		// Send a message to broker n-1 to start the process
		// of splitting the token range
		log.Info().Msgf("Broker considered as scaling up")
		o.requestRangeSplit(topology)

		return
	}

	// Restarting or starting fresh
	for {
		message := localGenMessage{
			isNew:  reason == newCluster,
			result: make(chan creationError),
		}

		// Send to the processing queue
		o.items <- &message
		err := <-message.result

		if err == nil {
			break
		}

		log.Err(err).Msg("Could not create generation when starting")

		if !err.canBeRetried() {
			log.Panic().Err(err).Msg("Non retryable error found when starting")
		}
		time.Sleep(getDelay())
	}
}

// Waits for the previous broker to create the generation for the previous token first
func (o *generator) waitForPreviousRange(topology *TopologyInfo) {
	start := time.Now()

	for time.Since(start) <= maxWaitForPrevious {
		prev := topology.PreviousBroker()
		prevToken := topology.GetToken(topology.GetIndex(prev.Ordinal))
		if result := o.gossiper.GetGenerations(prev.Ordinal, prevToken); result.Committed != nil {
			// The previous broker has information about it's own token
			log.Debug().Msgf("Waited %dms for a successful previous range generation", time.Since(start).Milliseconds())
			return
		}
		time.Sleep(waitForPreviousStep)
	}
	log.Panic().Msgf("Waited for previous range generation for more than %s", maxWaitForPrevious)
}

// Sends a message to the broker in the position n-1 to request token split and waits for generation creation/
//
// It panics when after waiting for a long time
func (o *generator) requestRangeSplit(topology *TopologyInfo) {
	token := topology.MyToken()
	prevOrdinal := topology.PreviousBroker().Ordinal

	// Add initial delay based on the position in the ring to minimize concurrent creation collision
	prevIndex := topology.GetIndex(prevOrdinal)
	if prevIndex > 0 {
		// We could wait for node in n-2 to have their generations
		i := prevIndex / 2 // The ring double the size
		delay := waitForSplitStep * time.Duration(i)
		log.Info().Msgf("Waiting %s before requesting range split", delay)
		time.Sleep(delay)
	}

	start := time.Now()
	for o.discoverer.Generation(token) == nil {
		log.Info().Msgf("Sending message to previous broker B%d to request token range split", prevOrdinal)

		if len(topology.Brokers) != len(o.discoverer.Topology().Brokers) {
			log.Panic().Msgf("There was a change in the topology since starting")
		}

		if time.Since(start) > maxWaitForSplit {
			log.Panic().Msgf("There was a change in the topology since starting")
		}

		if err := o.gossiper.RangeSplitStart(prevOrdinal); err != nil {
			log.Err(err).Msgf("There was an error requesting token range split, retrying")
		}

		time.Sleep(utils.Jitter(waitForSplitStep))
	}

	log.Info().Msgf("Waited %dms for B%d to split ranges", time.Since(start).Milliseconds(), prevOrdinal)
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
			err := o.processGeneration(message)
			message.setResult(err)
			atomic.StoreInt32(processingFlag, 0)

			if err != nil {
				log.Warn().Err(err).Msgf("Processing generation resulted in error")
			}
		}()
	}
}

// processGeneration() returns nil when the generation was created, otherwise an error.
func (o *generator) processGeneration(message genMessage) creationError {
	// Consider a channel if state is needed across multiple items, i.e. "serialItems"
	if m, ok := message.(*localGenMessage); ok {
		return o.processLocalMyToken(m)
	}

	if m, ok := message.(*remoteGenProposedMessage); ok {
		return o.processRemoteProposed(m)
	}

	if m, ok := message.(*remoteGenCommittedMessage); ok {
		return o.processRemoteCommitted(m)
	}

	if m, ok := message.(*localFailoverGenMessage); ok {
		return o.processLocalFailover(m)
	}

	if m, ok := message.(*localSplitRangeGenMessage); ok {
		return o.processLocalSplitRange(m)
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
	return time.Duration(rand.Intn(int(baseDelay.Milliseconds())))*time.Millisecond + baseDelay
}
