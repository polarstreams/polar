package ownership

import (
	"time"

	"github.com/google/uuid"
	. "github.com/jorgebay/soda/internal/interbroker"
	. "github.com/jorgebay/soda/internal/types"
	"github.com/jorgebay/soda/internal/utils"
	"github.com/rs/zerolog/log"
)

func (o *generator) processLocal(message *localGenMessage) creationError {
	topology := o.discoverer.Topology()
	token := topology.MyToken()
	timestamp := time.Now()

	generation := Generation{
		Start:     token,
		End:       topology.GetToken(topology.LocalIndex + 1),
		Version:   0,
		Timestamp: timestamp.UnixMicro(),
		Leader:    topology.MyOrdinal(),
		Followers: topology.NaturalFollowers(),
		Tx:        uuid.New(),
		TxLeader:  topology.MyOrdinal(),
		Status:    StatusProposed,
		ToDelete:  false,
	}

	log.Info().Msgf(
		"Processing a generation started locally for T%d (%d) with B%d and B%d as followers",
		topology.MyOrdinal(), topology.MyToken(), generation.Followers[0], generation.Followers[1])

	// Perform a read from followers
	readResults := o.readStateFromFollowers(&generation)

	if readResults[0].Error != nil && readResults[1].Error != nil {
		// No point in continuing
		return newCreationError("Followers state could not be read")
	}

	localCommitted, localProposed := o.discoverer.GenerationProposed(token)

	generation.Version = utils.MaxVersion(
		localCommitted,
		readResults[0].Committed,
		readResults[1].Committed) + 1

	if isInProgress(localProposed) {
		return newCreationError("In progress generation in local broker")
	}

	if isInProgress(readResults[0].Proposed) || isInProgress(readResults[1].Proposed) {
		return newCreationError("In progress generation in remote broker")
	}

	// After reading, we can continue by performing a CAS operation for proposed
	log.Info().Msgf("Proposing myself as a leader for T%d (%d)", topology.MyOrdinal(), topology.MyToken())

	followerErrors := o.setStateToFollowers(&generation, []error{nil, nil}, readResults)
	if followerErrors[0] != nil && followerErrors[1] != nil {
		return newCreationError("Followers state could not be set to proposed")
	}

	var localTx *uuid.UUID
	if localProposed != nil {
		localTx = &localProposed.Tx
	}

	if err := o.discoverer.SetGenerationProposed(&generation, localTx); err != nil {
		log.Err(err).Msg("Unexpected error when setting as proposed locally")
		// Don't retry
		return newCreationError("Unexpected local error")
	}

	log.Info().Msgf("Accepting myself as a leader for T%d (%d)", topology.MyOrdinal(), topology.MyToken())
	generation.Status = StatusAccepted

	followerErrors = o.setStateToFollowers(&generation, followerErrors, readResults)
	if followerErrors[0] != nil && followerErrors[1] != nil {
		return newCreationError("Followers state could not be set to accepted")
	}

	if err := o.discoverer.SetGenerationProposed(&generation, &generation.Tx); err != nil {
		log.Err(err).Msg("Unexpected error when setting as proposed locally")
		return newCreationError("Unexpected local error")
	}

	// Now we have a majority of replicas
	log.Info().Msgf("Setting transaction for T%d (%d) as committed", topology.MyOrdinal(), topology.MyToken())

	// We can now start receiving producer traffic for this token
	if err := o.discoverer.SetAsCommitted(generation.Start, generation.Tx, topology.MyOrdinal()); err != nil {
		log.Err(err).Msg("Set as committed locally failed (probably local db related)")
		return newCreationError("Set as committed locally failed")
	}

	generation.Status = StatusCommitted
	followerErrors = o.setStateToFollowers(&generation, followerErrors, readResults)
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
		errorChan <- o.gossiper.SetGenerationAsProposed(ordinal, gen, tx)
	} else {
		// Use committed CAS
		errorChan <- o.gossiper.SetAsCommitted(ordinal, gen.Start, *tx)
	}
}

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

	if !o.localDb.DbWasNewlyCreated() {
		// There's local data, it signals that it has been restarted
		return restarted
	}

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