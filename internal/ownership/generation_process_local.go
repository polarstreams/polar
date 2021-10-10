package ownership

import (
	"errors"
	"fmt"

	. "github.com/jorgebay/soda/internal/types"
	"github.com/rs/zerolog/log"
)

func (o *generator) processLocal(newGen Generation) error {
	log.Debug().Msg("Processing a generation started locally")

	accepted := &emptyGeneration
	proposed := &emptyGeneration

	// Retrieve locally
	if gens, err := o.localDb.GetGenerationsByToken(newGen.Start); err != nil {
		log.Fatal().Err(err).Msg("Generations could not be retrieved")
	} else {
		accepted, proposed = checkState(gens, accepted, proposed)
	}

	//TODO: Local accepted and proposed
	_, localProposed := accepted, proposed

	genByFollower := make([][]Generation, len(newGen.Followers))

	// Retrieve from followers
	for i, follower := range newGen.Followers {
		gens, err := o.gossiper.GetGenerations(follower, newGen.Start)
		if err != nil {
			log.Debug().Msgf("Error when trying to retrieve generations from broker %d: %s", follower, err.Error())
			continue
		}

		log.Debug().Msgf("Obtained generations %v from broker %d", gens, follower)
		accepted, proposed = checkState(gens, accepted, proposed)
		genByFollower[i] = gens
	}

	if proposed != &emptyGeneration {
		// TODO: Check whether it's old, cancel and override
		log.Info().Msgf("There's already a previous proposed generation TODO FIX")
		return errors.New("There's already a proposed generation for this token")
	}

	newGen.Version = accepted.Version + 1
	newGen.Tx = o.nextUuid()
	newGen.Status = StatusProposed

	failedFollowers := make([]int, 0)

	// Start by trying to propose the generation on the followers
	for i, follower := range newGen.Followers {
		gens := genByFollower[i]
		var existingGen *Generation = nil
		if len(gens) > 0 && gens[0].Status == StatusProposed {
			existingGen = &gens[0]
		}

		// TODO: In parallel!
		if err := o.gossiper.UpsertGeneration(follower, existingGen, newGen); err != nil {
			log.Warn().Err(err).Msgf("Upsert generation failed in peer %d", follower)
			failedFollowers = append(failedFollowers, follower)
		}
	}

	if len(failedFollowers) >= len(newGen.Followers)/2 {
		return fmt.Errorf("Generation creation was rejected by proposed followers")
	}

	//TODO: Heal failed followers

	if localProposed == &emptyGeneration {
		localProposed = nil
	}

	log.Debug().Msgf("Proposing locally new generation %+v", newGen)

	if err := o.localDb.UpsertGeneration(localProposed, &newGen); err != nil {
		log.Error().Msg("Generation could not be proposed locally")
		return fmt.Errorf("Generation could not be proposed locally")
	}

	return o.setAsAccepted(&newGen)
}

func (o *generator) determineStartReason() startReason {
	if !o.localDb.DbWasNewlyCreated() {
		// There's local data, it signals that it has been restarted
		return restarted
	}

	topology := o.discoverer.Topology()
	myToken := topology.MyToken()

	// When the previous broker has in range the token that
	// belongs to the current broker, that signals that it should
	// be splitted up
	if cond, err := o.gossiper.IsTokenRangeCovered(topology.PreviousBroker().Ordinal, myToken); cond {
		return scalingUp
	} else if err != nil {
		log.Fatal().Err(err).Msgf("Gossip query failed for token range")
	}

	// Just to make sure, we query the next broker
	// Maybe my local data was loss and the current broker is being replaced
	if cond, err := o.gossiper.HasTokenHistoryForToken(topology.NextBroker().Ordinal, myToken); cond {
		return restarted
	} else if err != nil {
		log.Fatal().Err(err).Msgf("Gossip query failed for token history")
	}

	return newCluster
}

type startReason int

const (
	restarted startReason = iota
	newCluster
	scalingUp
)
