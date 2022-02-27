package ownership

import (
	"time"

	. "github.com/barcostreams/barco/internal/interbroker"
	. "github.com/barcostreams/barco/internal/types"
	"github.com/barcostreams/barco/internal/utils"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
)

// Processes creation of new generation for tokens that are naturally owned by this broker.
// The reason of the creation it can be that we are starting new
// or this broker came back online and it's trying to recover
func (o *generator) processLocalMyToken(message *localGenMessage) creationError {
	topology := o.discoverer.Topology()
	token := topology.MyToken()
	timestamp := time.Now()

	gen := Generation{
		Start:     token,
		End:       topology.GetToken(topology.LocalIndex + 1),
		Version:   0,
		Timestamp: timestamp.UnixMicro(),
		Leader:    topology.MyOrdinal(),
		Followers: topology.NaturalFollowers(topology.LocalIndex),
		Tx:        uuid.New(),
		TxLeader:  topology.MyOrdinal(),
		Status:    StatusProposed,
		Parents:   make([]GenParent, 0),
	}

	log.Info().Msgf(
		"Processing a generation started locally for T%d (%d) with B%d and B%d as followers",
		topology.MyOrdinal(), topology.MyToken(), gen.Followers[0], gen.Followers[1])

	// Perform a read from followers
	readResults := o.readStateFromFollowers(&gen)

	if readResults[0].Error != nil && readResults[1].Error != nil {
		// No point in continuing
		return newCreationError("Followers state could not be read")
	}

	if message.isNew && (readResults[0].Committed != nil || readResults[1].Committed != nil) {
		return newCreationError("Unexpected information found in peer for new token")
	}

	localCommitted, localProposed := o.discoverer.GenerationProposed(token)

	if isInProgress(localProposed) {
		return newCreationError("In progress generation in local broker")
	}

	if isInProgress(readResults[0].Proposed) || isInProgress(readResults[1].Proposed) {
		return newCreationError("In progress generation in remote broker")
	}

	if message.isNew {
		// After reading, we can continue by performing a CAS operation for proposed
		log.Info().Msgf("Proposing myself as a first time leader of T%d (%d)", topology.MyOrdinal(), topology.MyToken())
		gen.Version = GenVersion(1)
	} else {
		parentVersion := utils.MaxVersion(
			localCommitted,
			readResults[0].Committed,
			readResults[1].Committed)
		gen.Version = parentVersion + 1
		gen.Parents = append(gen.Parents, GenParent{
			Start:   token,
			Version: parentVersion,
		})
		log.Info().Msgf(
			"Proposing myself as leader of T%d (%d) in v%d", topology.MyOrdinal(), topology.MyToken(), gen.Version)
	}

	followerErrors := o.setStateToFollowers(&gen, nil, readResults)
	if followerErrors[0] != nil && followerErrors[1] != nil {
		return newCreationError("Followers state could not be set to proposed")
	}

	var localTx *uuid.UUID
	if localProposed != nil {
		localTx = &localProposed.Tx
	}

	if err := o.discoverer.SetGenerationProposed(&gen, nil, localTx); err != nil {
		log.Err(err).Msg("Unexpected error when setting as proposed locally")
		// Don't retry
		return newNonRetryableError("Unexpected local error")
	}

	log.Info().Msgf("Accepting myself as a leader for T%d (%d)", topology.MyOrdinal(), topology.MyToken())
	gen.Status = StatusAccepted

	followerErrors = o.setStateToFollowers(&gen, followerErrors, readResults)
	if followerErrors[0] != nil && followerErrors[1] != nil {
		return newCreationError("Followers state could not be set to accepted")
	}

	if err := o.discoverer.SetGenerationProposed(&gen, nil, &gen.Tx); err != nil {
		log.Err(err).Msg("Unexpected error when setting as proposed locally")
		return newCreationError("Unexpected local error")
	}

	// Now we have a majority of replicas
	log.Info().Msgf("Setting transaction for T%d (%d) as committed", topology.MyOrdinal(), topology.MyToken())

	// We can now start receiving producer traffic for this token
	if err := o.discoverer.SetAsCommitted(gen.Start, nil, gen.Tx, topology.MyOrdinal()); err != nil {
		log.Err(err).Msg("Set as committed locally failed (probably local db related)")
		return newCreationError("Set as committed locally failed")
	}

	gen.Status = StatusCommitted
	followerErrors = o.setStateToFollowers(&gen, followerErrors, readResults)
	if followerErrors[0] != nil && followerErrors[1] != nil {
		// The transaction is still considered committed and
		// will be roll forward by the followers
		log.Warn().Msgf(
			"Setting transaction for T%d (%d) as committed failed on followers",
			topology.MyOrdinal(),
			topology.MyToken())
	}

	return nil
}

func isInProgress(proposed *Generation) bool {
	return proposed != nil && time.Since(proposed.Time()) > maxDelay
}

func (o *generator) setStateToFollowers(
	gen *Generation,
	previousErrors []error,
	readResults []GenReadResult,
) []error {
	error1 := make(chan error)
	error2 := make(chan error)

	if previousErrors == nil {
		previousErrors = []error{nil, nil}
	}

	setFunc := func(i int, errorChan chan error) {
		o.setRemoteState(gen.Followers[i], gen, previousErrors[i], readResults[i], errorChan)
	}

	go setFunc(0, error1)
	go setFunc(1, error2)

	return []error{<-error1, <-error2}
}

func (o *generator) setRemoteState(
	ordinal int,
	gen *Generation,
	previousError error,
	readResult GenReadResult,
	errorChan chan error,
) {
	if previousError != nil {
		errorChan <- previousError
		return
	}
	if readResult.Error != nil {
		errorChan <- readResult.Error
		return
	}
	var tx *uuid.UUID
	if gen.Status != StatusProposed {
		// On the following steps, use the gen tx to compare
		tx = &gen.Tx
	} else if readResult.Proposed != nil {
		tx = &readResult.Proposed.Tx
	}

	if gen.Status != StatusCommitted {
		// Use proposed CAS
		errorChan <- o.gossiper.SetGenerationAsProposed(ordinal, gen, nil, tx)
	} else {
		// Use committed CAS
		errorChan <- o.gossiper.SetAsCommitted(ordinal, gen.Start, nil, *tx)
	}
}

// Reads from followers, when there's no information, nil property values are returned
func (o *generator) readStateFromFollowers(gen *Generation) []GenReadResult {
	r1 := make(chan GenReadResult)
	r2 := make(chan GenReadResult)

	go func() {
		r1 <- o.gossiper.GetGenerations(gen.Followers[0], gen.Start)
	}()

	go func() {
		r2 <- o.gossiper.GetGenerations(gen.Followers[1], gen.Start)
	}()

	return []GenReadResult{<-r1, <-r2}
}

func (o *generator) determineStartReason() startReason {
	log.Info().Msgf("Trying to determine whether its a new cluster")
	topology := o.discoverer.Topology()
	myToken := topology.MyToken()

	if cond, err := o.gossiper.IsTokenRangeCovered(topology.PreviousBroker().Ordinal, myToken); cond {
		// When the previous broker has in range the token that
		// belongs to the current broker, that signals that it should
		// be splitted up
		return scalingUp
	} else if err != nil {
		log.Panic().Err(err).Msgf("Gossip query failed for token range")
	}

	// Just to make sure, we query the next broker
	// Maybe my local data was loss and the current broker is being replaced
	if cond, err := o.gossiper.HasTokenHistoryForToken(topology.NextBroker().Ordinal, myToken); cond {
		return restarted
	} else if err != nil {
		log.Panic().Err(err).Msgf("Gossip query failed for token history")
	}

	return newCluster
}

type startReason int

const (
	restarted startReason = iota
	newCluster
	scalingUp
)
