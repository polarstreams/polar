package ownership

import (
	"sync"
	"time"

	. "github.com/barcostreams/barco/internal/types"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
)

func (o *generator) processLocalSplitRange(m *localSplitRangeGenMessage) creationError {
	const reason = "split range"
	topology := m.topology
	newBrokerOrdinal := m.origin
	newBrokerIndex := topology.GetIndex(newBrokerOrdinal)
	myToken := topology.MyToken()
	myCurrentGen := o.discoverer.Generation(myToken)
	newToken := topology.GetToken(newBrokerIndex)

	if o.discoverer.Generation(newToken) != nil {
		// Already created
		return nil
	}
	if myCurrentGen == nil {
		return newCreationError("Could not split range as generation not found for T%d", topology.MyOrdinal())
	}
	if myCurrentGen.Leader != topology.MyOrdinal() {
		return newCreationError("Could not split range as I'm not the leader of my token T%d", topology.MyOrdinal())
	}

	nextBrokers := topology.NextBrokers(topology.LocalIndex, 3)
	for _, b := range nextBrokers {
		if !o.gossiper.IsHostUp(b.Ordinal) {
			return newCreationError("Could not split range as B%d is not UP", b.Ordinal)
		}
	}

	log.Info().Msgf("Processing token range split T%d-T%d", topology.MyOrdinal(), nextBrokers[1].Ordinal)

	version := o.lastKnownVersion(newToken, nextBrokers) + 1
	log.Debug().Msgf("Identified v%d for T%d (%d)", version, newBrokerOrdinal, newToken)

	// Use the same transaction id for both generations
	tx := uuid.New()

	myGen := Generation{
		Start:     myToken,
		End:       newToken,
		Version:   myCurrentGen.Version + 1,
		Timestamp: time.Now().UnixMicro(),
		Leader:    topology.MyOrdinal(),
		Followers: ordinals(nextBrokers[:2]),
		TxLeader:  topology.MyOrdinal(),
		Tx:        tx,
		Status:    StatusProposed,
		Parents: []GenParent{{
			Start:   myCurrentGen.Start,
			Version: myCurrentGen.Version,
		}},
	}

	// Generation for the second part of the range
	nextTokenGen := Generation{
		Start:     newToken,
		End:       myCurrentGen.End,
		Version:   version,
		Timestamp: time.Now().UnixMicro(),
		Leader:    newBrokerOrdinal,
		Followers: ordinals(nextBrokers[1:3]),
		TxLeader:  topology.MyOrdinal(),
		Tx:        tx,
		Status:    StatusProposed,
		Parents:   myGen.Parents,
	}

	_, err := o.rangeSplitPropose(&myGen, &nextTokenGen)
	if err != nil {
		return err
	}

	// Start accepting flow
	myGen.Status = StatusAccepted
	nextTokenGen.Status = StatusAccepted

	// Accept on the new broker
	if err := o.gossiper.SetGenerationAsProposed(newBrokerOrdinal, &myGen, &nextTokenGen, &tx); err != nil {
		return wrapCreationError(err)
	}
	// Accept locally
	if err := o.discoverer.SetGenerationProposed(&myGen, &nextTokenGen, &tx); err != nil {
		return newNonRetryableError("Unexpected error when accepting split locally: %s", err)
	}

	// Accept on the common follower index Bn+2
	if err := o.gossiper.SetGenerationAsProposed(nextTokenGen.Followers[0], &myGen, &nextTokenGen, &tx); err != nil {
		return wrapCreationError(err)
	}

	// At this moment, we have a majority of replicas

	// Mark as accepted on index Bn+3, ignore if it fails
	backgroundDone := make(chan error)
	go func() {
		backgroundDone <- o.gossiper.SetGenerationAsProposed(nextTokenGen.Followers[1], &myGen, &nextTokenGen, &tx)
	}()

	log.Info().Msgf(
		"Setting transaction for T%d v%d and T%d v%d as committed",
		topology.MyOrdinal(), myGen.Version, newBrokerOrdinal, nextTokenGen.Version)

	if err := o.discoverer.SetAsCommitted(myToken, &newToken, tx, topology.MyOrdinal()); err != nil {
		return wrapCreationError(err)
	}

	<-backgroundDone
	var wg sync.WaitGroup
	for _, b := range nextBrokers {
		ordinal := b.Ordinal
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := o.gossiper.SetAsCommitted(ordinal, myToken, &newToken, tx); err != nil {
				log.Err(err).Msgf("There was an error when committing on B%d", ordinal)
			}
		}()
	}
	wg.Wait()

	return nil
}

func (o *generator) rangeSplitPropose(myGen *Generation, nextTokenGen *Generation) (*splitProposeResult, creationError) {
	readResults := o.readStateFromFollowers(myGen)
	if readResults[0].Error != nil && readResults[1].Error != nil {
		return nil, newCreationError("Followers state could not be read")
	}
	if isInProgress(readResults[0].Proposed) || isInProgress(readResults[1].Proposed) {
		return nil, newCreationError("In progress generation in remote broker")
	}
	nextTokenLeaderRead := o.gossiper.GetGenerations(nextTokenGen.Leader, nextTokenGen.Start)
	if nextTokenLeaderRead.Error != nil {
		return nil, newCreationError("Next token leader generation state could not be read: %s", nextTokenLeaderRead.Error)
	}

	followerErrors := o.setStateToFollowers(myGen, nil, readResults)
	if followerErrors[0] != nil && followerErrors[1] != nil {
		return nil, newCreationError("Followers state could not be set to proposed")
	}

	backgroundDone := make(chan error)
	go func() {
		// Set as gen1 as proposed on Bn+3 (second follower of next token)
		backgroundDone <- o.gossiper.SetGenerationAsProposed(nextTokenGen.Followers[1], myGen, nil, nil)
	}()

	// Proposing locally, one at a time as it might have different original transactions
	if err := o.discoverer.SetGenerationProposed(myGen, nil, o.getLocalTx(myGen.Start)); err != nil {
		log.Err(err).Msg("Unexpected error when setting as proposed locally")
		return nil, newNonRetryableError("Unexpected local error")
	}
	if err := o.discoverer.SetGenerationProposed(nextTokenGen, nil, o.getLocalTx(nextTokenGen.Start)); err != nil {
		log.Err(err).Msg("Unexpected error when setting as proposed locally")
		return nil, newNonRetryableError("Unexpected local error")
	}

	log.Debug().Msgf(
		"Proposed myself as a leader for T%d-T%d [%d, %d] as part of range splitting",
		myGen.Leader, nextTokenGen.Leader, myGen.Start, myGen.End)

	// Read the state from of nextTokenGen followers
	readResults = o.readStateFromFollowers(nextTokenGen)
	if readResults[0].Error != nil && readResults[1].Error != nil {
		return nil, newCreationError("Followers state could not be read")
	}
	if isInProgress(readResults[0].Proposed) || isInProgress(readResults[1].Proposed) {
		return nil, newCreationError("In progress generation in remote broker")
	}

	// Set state in leader
	if err := o.gossiper.SetGenerationAsProposed(nextTokenGen.Leader, nextTokenGen, nil, getTx(nextTokenLeaderRead.Proposed)); err != nil {
		return nil, newCreationError("Next token leader generation state could not be set: %s", err)
	}
	nextTokenFollowerErrors := o.setStateToFollowers(nextTokenGen, nil, readResults)
	if nextTokenFollowerErrors[0] != nil && nextTokenFollowerErrors[1] != nil {
		return nil, newCreationError("Followers state could not be set to proposed")
	}

	log.Info().Msgf(
		"Proposed B%d as a leader for T%d-T%d [%d, %d] as part of range splitting",
		nextTokenGen.Leader, nextTokenGen.Leader, nextTokenGen.Followers[0], nextTokenGen.Start, nextTokenGen.End)

	<-backgroundDone
	return nil, nil
}

func (o *generator) getLocalTx(token Token) *uuid.UUID {
	_, proposed := o.discoverer.GenerationProposed(token)
	if proposed == nil {
		return nil
	}
	return &proposed.Tx
}

type splitProposeResult struct {
}

func (o *generator) lastKnownVersion(token Token, peers []BrokerInfo) GenVersion {
	version := GenVersion(0)
	// As I'm the only one
	if gen, err := o.discoverer.GetTokenHistory(token); err != nil {
		log.Panic().Err(err).Msgf("Error retrieving token history")
	} else if gen != nil {
		version = gen.Version
	}

	peerVersions := make([]chan GenVersion, 0)
	for _, b := range peers {
		broker := b
		c := make(chan GenVersion)
		peerVersions = append(peerVersions, c)
		go func() {
			gen, err := o.gossiper.ReadTokenHistory(broker.Ordinal, token)
			if err != nil {
				log.Err(err).Msgf("Error retrieving token history from B%d", broker.Ordinal)
			}

			if gen != nil {
				c <- gen.Version
			} else {
				c <- GenVersion(0)
			}
		}()
	}

	for i := 0; i < len(peers); i++ {
		v := <-peerVersions[i]
		if v > version {
			version = v
		}
	}

	return version
}

func ordinals(brokers []BrokerInfo) []int {
	result := make([]int, len(brokers))
	for i, b := range brokers {
		result[i] = b.Ordinal
	}
	return result
}
