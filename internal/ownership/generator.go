package ownership

import (
	"fmt"
	"math"
	"math/rand"
	"sync/atomic"
	"time"

	"github.com/barcostreams/barco/internal/conf"
	"github.com/barcostreams/barco/internal/discovery"
	"github.com/barcostreams/barco/internal/interbroker"
	"github.com/barcostreams/barco/internal/localdb"
	. "github.com/barcostreams/barco/internal/types"
	"github.com/barcostreams/barco/internal/utils"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
)

const (
	baseDelay                   = 150 * time.Millisecond
	maxDelay                    = 300 * time.Millisecond
	maxWaitForPrevious          = 2 * time.Minute
	waitForPreviousStep         = 500 * time.Millisecond
	maxWaitForSplit             = 5 * time.Minute
	waitForSplitStep            = 5 * time.Second
	waitForJoinBase             = 5 * time.Second
	maxShutdownTakeOverAttempts = 5
	shutdownTakeOverDelay       = 1 * time.Second
	replicationFactor           = 3
)

var emptyGeneration = Generation{Start: math.MaxInt64, Version: 0, Status: StatusCancelled}

type Generator interface {
	Initializer
	StartGenerations()
}

type generator struct {
	// items can be a local or remote
	items      chan genMessage
	config     conf.BasicConfig
	discoverer discovery.TopologyGetter
	gossiper   interbroker.Gossiper
	localDb    localdb.Client
	nextUuid   func() uuid.UUID // allow injecting it from tests
}

func NewGenerator(
	config conf.BasicConfig,
	discoverer discovery.TopologyGetter,
	gossiper interbroker.Gossiper,
	localDb localdb.Client,
) Generator {
	o := &generator{
		config:     config,
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

	if !topology.AmIIncluded() {
		return utils.CreateErrAndLog("Ignoring remote range split as I'm leaving the cluster")
	}

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
	// Register UP/DOWN handler on the main thread, after all peers are up
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
	if !topology.AmIIncluded() {
		log.Debug().Msgf("Broker B%d detected but I'm leaving the cluster", broker.Ordinal)
		return
	}

	brokerIndex := topology.GetIndex(broker.Ordinal)
	followers := topology.NaturalFollowers(brokerIndex)
	if followers[0] != topology.MyOrdinal() {
		log.Debug().Msgf("Generator detected %s as DOWN but we are not the next broker in the ring", &broker)
		return
	}

	previousGen := o.discoverer.Generation(topology.GetToken(brokerIndex))
	if previousGen == nil {
		// TODO: Determine when this could happen and how to mitigate it
		log.Warn().Msgf("Broker B%d detected down without owning a generation", broker.Ordinal)
		return
	}

	if previousGen.Leader == topology.MyOrdinal() {
		log.Debug().Msgf(
			"Broker B%d detected as DOWN and we are already leaders of T%d ", broker.Ordinal, broker.Ordinal)
		return
	}

	log.Info().Msgf("Generator detected %s as DOWN, trying to become the leader of T%d", &broker, broker.Ordinal)

	go func() {
		// If host comes back up, it's expected to try to retake its token
		for !o.gossiper.IsHostUp(broker.Ordinal) && !o.localDb.IsShuttingDown() {
			topology := o.discoverer.Topology()
			if !topology.AmIIncluded() {
				log.Info().Msgf("Not attempting broker down failover as I'm leaving the cluster")
				break
			}

			message := localFailoverGenMessage{
				broker:   broker,
				topology: topology,
				result:   make(chan creationError, 1),
			}

			// Send to the processing queue
			o.items <- &message

			// Wait for the result
			err := <-message.result
			if err == nil {
				log.Debug().Msgf("Generator was able to become leader of T%d", broker.Ordinal)
				break
			}

			time.Sleep(getDelay())
		}
	}()
}

func (o *generator) OnHostUp(broker BrokerInfo) {
	log.Debug().Msgf("Generator detected %s as UP", &broker)
}

func (o *generator) OnHostShuttingDown(broker BrokerInfo) {
	topology := o.discoverer.Topology()
	if !topology.AmIIncluded() {
		log.Debug().Msgf("B%d detected as shutting down but I'm leaving the cluster", broker.Ordinal)
		return
	}

	if broker.Ordinal >= len(topology.Brokers) {
		log.Info().Msgf("B%d detected as shutting down but it is already not included in the topology", broker.Ordinal)
		return
	}

	if broker.Ordinal != topology.PreviousBroker().Ordinal {
		log.Info().Msgf("B%d detected as shutting down but it's not the previous broker", broker.Ordinal)
		return
	}

	log.Info().Msgf("Attempting to take over T%d as the result of B%d shutting down", broker.Ordinal, broker.Ordinal)

	go func() {
		// Let's delay it for a bit for topology to changes catch up (if anything)
		time.Sleep(shutdownTakeOverDelay)

		// Retry a few times but not forever
		// In any case, "shutting down" should be taken as a hint
		// If this fails, normal failover mechanisms will kick in
		for i := 0; i < maxShutdownTakeOverAttempts; i++ {
			topology := o.discoverer.Topology()
			if !topology.AmIIncluded() {
				log.Info().Msgf("Not attempting shutdown failover as I'm leaving the cluster")
				break
			}

			m := &localFailoverGenMessage{
				broker:         broker,
				topology:       topology,
				isShuttingDown: true,
				result:         make(chan creationError),
			}

			o.items <- m
			if err := <-m.result; err == nil {
				break
			}
			time.Sleep(getDelay())
		}
	}()
}

func (o *generator) startNew() {
	topology := o.discoverer.Topology()

	if o.config.DevMode() {
		o.createDevGeneration()
		return
	}

	if topology.MyOrdinal() != 0 {
		o.waitForPreviousRange(topology)
	}

	reason := o.determineStartReason()

	if reason == scalingUp || reason == restarted {
		// TODO: Retrieve current generations from Bn-1 & Bn+1
	}

	if reason == scalingUp {
		// Send a message to broker n-1 to start the process
		// of splitting the token range
		log.Info().Msgf("Broker considered as scaling up")
		o.requestRangeSplit(topology)

		return
	}

	// Restarting or starting fresh
	for {
		topology := o.discoverer.Topology()
		if topology.AmIIncluded() {
			message := localGenMessage{
				isNew:    reason == newCluster,
				topology: topology,
				result:   make(chan creationError),
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

		if !o.gossiper.IsHostUp(prevOrdinal) {
			log.Error().Msgf("B%d is DOWN waiting for it to be UP to request a range split", prevOrdinal)
		} else if err := o.gossiper.RangeSplitStart(prevOrdinal); err != nil {
			log.Err(err).Msgf("There was an error requesting token range split, retrying")
		}

		time.Sleep(utils.Jitter(waitForSplitStep))
	}

	log.Info().Msgf("Waited %dms for B%d to split ranges", time.Since(start).Milliseconds(), prevOrdinal)
}

func (o *generator) OnJoinRange(previousTopology *TopologyInfo, topology *TopologyInfo) {
	if !topology.AmIIncluded() {
		log.Error().Msgf("Ignoring join range call as I'm leaving the cluster")
	}

	go func() {
		// Avoid unnecessary noise
		if topology.LocalIndex > 0 {
			delay := time.Duration(topology.LocalIndex) * waitForJoinBase
			log.Debug().Msgf("Waiting for %s before start joining the token ranges", delay)
			time.Sleep(delay)
		}

		// As long as the topology doesn't change
		for len(topology.Brokers) == len(o.discoverer.Topology().Brokers) && !o.localDb.IsShuttingDown() {
			message := localJoinRangeGenMessage{
				topology:         topology,
				previousTopology: previousTopology,
				result:           make(chan creationError, 1),
			}

			// Send to the processing queue
			o.items <- &message

			// Wait for the result
			err := <-message.result
			if err == nil {
				log.Debug().Msgf(
					"Generator was able to become leader of joined range for T%d", topology.MyOrdinal())
				break
			}

			// Don't wait for long time as it can result in unavailability of the system
			time.Sleep(getDelay())
		}
	}()
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

	if m, ok := message.(*localJoinRangeGenMessage); ok {
		return o.processLocalJoinRange(m)
	}

	log.Panic().Msg("Unhandled generation internal message type")
	return nil
}

func (o *generator) createDevGeneration() {
	gen := &Generation{
		Start:     StartToken,
		End:       StartToken,
		Version:   1,
		Timestamp: time.Now().UnixMicro(),
		Leader:    0,
		Followers: []int{},
		TxLeader:  0,
		Tx:        uuid.New(),
		Status:    StatusCommitted,
		Parents:   []GenId{},
	}
	o.discoverer.RepairCommitted(gen)
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

// Gets a value between base delay and max delay
func getDelay() time.Duration {
	diffDelay := maxDelay - baseDelay
	randomDelay := time.Duration(rand.Int63n(diffDelay.Milliseconds())) * time.Millisecond
	return baseDelay + randomDelay
}
