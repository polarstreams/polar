package ownership

import (
	"fmt"
	"time"

	"github.com/google/uuid"
	. "github.com/jorgebay/soda/internal/interbroker"
	. "github.com/jorgebay/soda/internal/types"
	"github.com/jorgebay/soda/internal/utils"
	"github.com/rs/zerolog/log"
)

func (o *generator) processLocal(message *localGenMessage) error {
	topology := o.discoverer.Topology()
	token := topology.MyToken()
	timestamp := time.Now()

	log.Info().Msgf("Processing a generation started locally for T-%d (%d)", topology.MyOrdinal(), topology.MyToken())

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

	// Perform a read from followers
	readResults := o.readStateFromFollowers(&generation)

	if readResults[0].Error != nil && readResults[1].Error != nil {
		// No point in continuing
		defer o.retryStartLocal(&generation)
		return fmt.Errorf("Followers state could not be read")
	}

	localCommitted, localProposed := o.discoverer.GenerationProposed(token)

	generation.Version = utils.MaxVersion(
		localCommitted,
		readResults[0].Committed,
		readResults[1].Committed) + 1

	if isInProgress(localProposed) {
		defer o.retryStartLocal(&generation)
		return fmt.Errorf("In progress generation in local broker")
	}

	if isInProgress(readResults[0].Proposed) || isInProgress(readResults[1].Proposed) {
		defer o.retryStartLocal(&generation)
		return fmt.Errorf("In progress generation in remote broker")
	}

	// After reading, we can continue by performing a CAS operation for proposed

	log.Info().Msgf("Proposing myself for T-%d (%d)", topology.MyOrdinal(), topology.MyToken())

	//TODO: o.gossiper.SetGenerationAsProposed()

	return nil
}

func isInProgress(proposed *Generation) bool {
	return proposed != nil && time.Since(proposed.Time()) > maxDelay
}

func (o *generator) retryStartLocal(gen *Generation) {
	//TODO: Implement
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
