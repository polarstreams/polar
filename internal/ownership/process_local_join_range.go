package ownership

import (
	"time"

	"github.com/barcostreams/barco/internal/interbroker"
	. "github.com/barcostreams/barco/internal/types"
	"github.com/barcostreams/barco/internal/utils"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
)

func (o *generator) processLocalJoinRange(m *localJoinRangeGenMessage) creationError {
	topology := m.topology
	previousTopology := m.previousTopology
	myToken := topology.MyToken()
	nextBroker := previousTopology.NextBroker()
	nextBrokers := ordinals(previousTopology.NextBrokers(previousTopology.LocalIndex, 4))
	nextToken := previousTopology.GetToken(previousTopology.NextIndex())
	newNextBroker := topology.NextBroker()

	log.Info().Msgf(
		"Start joining ranges T%d-T%d and T%d-T%d into T%d-T%d [%d, %d]",
		topology.MyOrdinal(), nextBroker.Ordinal, nextBroker.Ordinal, newNextBroker.Ordinal,
		topology.MyOrdinal(), newNextBroker.Ordinal, myToken, topology.GetToken(topology.NextIndex()))

	// Read state from peers as soon as possible
	myGenReadResults := o.readStateFromPeers(myToken, nextBrokers)
	nextTokenReadResults := o.readStateFromPeers(nextToken, nextBrokers)

	if allReadsErrored(nextTokenReadResults) {
		return newCreationError("All reads errored for T%d generation", nextBroker.Ordinal)
	}

	if err := o.readRepairFromPeers(nextToken, nextTokenReadResults); err != nil {
		return newNonRetryableError("There was an error repairing: %s", err.Error())
	}

	// Read state of my current token generation
	localCommitted1, localProposed1 := o.discoverer.GenerationProposed(myToken)
	if localCommitted1 == nil {
		log.Warn().Msgf("No local committed information for T%d", topology.MyOrdinal())
		if allReadsErrored(myGenReadResults[:2]) {
			return newCreationError("All reads errored for my token T%d generation", topology.MyOrdinal())
		}
	}

	parentVersion1 := utils.MaxVersion(
		localCommitted1,
		myGenReadResults[0].Committed,
		myGenReadResults[1].Committed,
		myGenReadResults[2].Committed,
		myGenReadResults[3].Committed)

	localCommitted2, localProposed2 := o.discoverer.GenerationProposed(nextToken)
	// Read repair should have provided us with the info
	parentVersion2 := localCommitted2.Version

	tx := uuid.New()
	gen := &Generation{
		Start:     myToken,
		End:       topology.GetToken(topology.NextIndex()),
		Version:   parentVersion1 + 1,
		Timestamp: time.Now().UnixMicro(),
		Leader:    topology.MyOrdinal(),
		Followers: topology.NaturalFollowers(topology.LocalIndex),
		TxLeader:  topology.MyOrdinal(),
		Tx:        tx,
		Status:    StatusProposed,
		Parents:   []GenId{{Start: myToken, Version: parentVersion1}, {Start: nextToken, Version: parentVersion2}},
	}

	toDeleteGen := &Generation{
		Start:     nextToken,
		End:       gen.End,
		Version:   parentVersion2 + 1, // This next version is not going to be recorded
		Timestamp: time.Now().UnixMicro(),
		Leader:    -1,
		TxLeader:  topology.MyOrdinal(),
		Tx:        tx,
		Status:    StatusProposed,
		Parents:   []GenId{{Start: nextToken, Version: parentVersion2}},
		ToDelete:  true, // Mark it that is not going to be active any more
	}

	// Set as proposed on followers first
	myGenProposeResults := toErrors(o.proposeInPeers(gen, nextBrokers, myGenReadResults))
	if myGenProposeResults[1] != nil && myGenProposeResults[3] != nil {
		// if we can't proposed on the peers that are staying (new followers), there's no point to continue
		return newCreationError("New generation could not be proposed to B%d and B%d", nextBrokers[1], nextBrokers[3])
	}

	if err := o.discoverer.SetGenerationProposed(gen, nil, getTx(localProposed1)); err != nil {
		return newNonRetryableError("Unexpected error when proposing join locally: %s", err)
	}
	if err := o.discoverer.SetGenerationProposed(toDeleteGen, nil, getTx(localProposed2)); err != nil {
		return newNonRetryableError("Unexpected error when proposing join locally: %s", err)
	}

	log.Debug().Msgf("Proposing to delete on next brokers")
	_ = toErrors(o.proposeInPeers(toDeleteGen, nextBrokers, nextTokenReadResults))

	gen.Status = StatusAccepted
	toDeleteGen.Status = StatusAccepted

	acceptResults := toErrors(utils.InParallel(len(nextBrokers), func(i int) error {
		ordinal := nextBrokers[i]
		return o.gossiper.SetGenerationAsProposed(ordinal, gen, toDeleteGen, &tx)
	}))

	if acceptResults[1] != nil && acceptResults[3] != nil {
		// if we can't accept it on the peers that are staying (new followers), there's no point to continue
		return newCreationError("New generation could not be proposed to B%d and B%d", nextBrokers[1], nextBrokers[3])
	}

	if err := o.discoverer.SetGenerationProposed(gen, toDeleteGen, &tx); err != nil {
		return newNonRetryableError("Unexpected error when accepting join locally: %s", err)
	}

	log.Info().Msgf(
		"Setting transaction for joined range T%d-T%d v%d as committed",
		topology.MyOrdinal(), newNextBroker.Ordinal, gen.Version)

	// We have a majority of replicas
	if err := o.discoverer.SetAsCommitted(gen.Start, &toDeleteGen.Start, tx, previousTopology.MyOrdinal()); err != nil {
		return newNonRetryableError("Unexpected error when committing join locally: %s", err)
	}

	_ = toErrors(utils.InParallel(len(nextBrokers), func(i int) error {
		ordinal := nextBrokers[i]
		return o.gossiper.SetAsCommitted(ordinal, gen.Start, &toDeleteGen.Start, tx)
	}))

	return nil
}

func (o *generator) proposeInPeers(gen *Generation, peers []int, readResults []interbroker.GenReadResult) []chan error {
	return utils.InParallel(len(readResults), func(i int) error {
		ordinal := peers[i]
		read := readResults[i]
		if read.Error != nil {
			log.Warn().Msgf("Not setting as proposed on B%d due to error: %s", ordinal, read.Error)
			return read.Error
		}
		return o.gossiper.SetGenerationAsProposed(ordinal, gen, nil, getTx(read.Proposed))
	})
}

// Amends the local information of a generation based on read results
func (o *generator) readRepairFromPeers(token Token, readResults []interbroker.GenReadResult) error {
	gen := o.discoverer.Generation(token)
	version := GenVersion(0)

	if gen != nil {
		version = gen.Version
	}

	var newerGeneration *Generation

	for _, read := range readResults {
		peerGeneration := read.Committed
		if peerGeneration == nil {
			continue
		}
		if peerGeneration.Version > version {
			version = peerGeneration.Version
			newerGeneration = peerGeneration
		}
	}

	if newerGeneration == nil {
		return nil
	}
	return o.discoverer.RepairCommitted(newerGeneration)
}

func toErrors(channels []chan error) []error {
	result := make([]error, len(channels))
	for i, c := range channels {
		result[i] = <-c
	}
	return result
}

func allReadsErrored(readResults []interbroker.GenReadResult) bool {
	for _, r := range readResults {
		if r.Error == nil {
			return false
		}
	}
	return true
}
