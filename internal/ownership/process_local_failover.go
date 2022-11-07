package ownership

import (
	"reflect"
	"time"

	. "github.com/barcostreams/barco/internal/types"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
)

func (o *generator) processLocalFailover(m *localFailoverGenMessage) creationError {
	const reason = "failover"
	topology := m.topology
	downBroker := m.broker.Ordinal

	if downBroker >= len(topology.Brokers) {
		return newNonRetryableError(
			"Could not process failover for B%d as it's already not included in the topology",
			downBroker)
	}

	index := topology.GetIndex(downBroker)
	peerFollower := topology.NaturalFollowers(index)[1]
	token := topology.GetToken(index)

	previousGen := o.discoverer.Generation(token)
	if previousGen == nil {
		return newCreationError(
			"Could not process token failover B%d because it does not own a generation", downBroker)
	}

	if previousGen.Leader == topology.MyOrdinal() {
		log.Debug().Msgf("Failover not needed, we are already the leader of T%d", downBroker)
		return nil
	}

	if downBroker != topology.PreviousBroker().Ordinal {
		return newNonRetryableError(
			"Could not process failover for B%d as it's not our previous broker (ring size: %d)",
			downBroker, len(topology.Brokers))
	}

	log.Debug().Msgf("Processing token failover for T%d", downBroker)
	isUp, err := o.gossiper.ReadBrokerIsUp(peerFollower, downBroker)
	if err != nil {
		return wrapCreationError(err)
	}

	if isUp {
		return newCreationError("Broker B%d is still consider as UP by B%d", downBroker, peerFollower)
	}

	gen := Generation{
		Start:     token,
		End:       topology.GetToken(index + 1),
		Version:   previousGen.Version + 1,
		Timestamp: time.Now().UnixMicro(),
		Leader:    topology.MyOrdinal(),
		Followers: []int{peerFollower, downBroker},
		TxLeader:  topology.MyOrdinal(),
		Tx:        uuid.New(),
		Status:    StatusProposed,
		Parents: []GenId{{
			Start:   token,
			Version: previousGen.Version,
		}},
		ClusterSize: topology.TotalBrokers(),
	}

	log.Info().
		Str("reason", reason).
		Msgf("Proposing myself as leader of T%d (%d) in v%d", downBroker, token, gen.Version)

	committed, proposed := o.discoverer.GenerationProposed(token)

	if !reflect.DeepEqual(previousGen, committed) {
		log.Error().Msgf(
			"Unexpected new committed generation found for T%d (v%d)", downBroker, committed.Version)
		return nil
	}

	peerFollowerGenInfo := o.gossiper.GetGenerations(peerFollower, gen.Start)
	if peerFollowerGenInfo.Error != nil {
		return newCreationError(
			"Generation info could not be read from follower: %s", peerFollowerGenInfo.Error.Error())
	}

	if err := o.discoverer.SetGenerationProposed(&gen, nil, getTx(proposed)); err != nil {
		return wrapCreationError(err)
	}

	if err := o.gossiper.SetGenerationAsProposed(peerFollower, &gen, nil, getTx(peerFollowerGenInfo.Proposed)); err != nil {
		return wrapCreationError(err)
	}

	log.Info().
		Str("reason", reason).
		Msgf("Accepting myself as leader of T%d (%d) in v%d", downBroker, token, gen.Version)
	gen.Status = StatusAccepted

	if err := o.gossiper.SetGenerationAsProposed(peerFollower, &gen, nil, &gen.Tx); err != nil {
		return wrapCreationError(err)
	}

	if err := o.discoverer.SetGenerationProposed(&gen, nil, &gen.Tx); err != nil {
		log.Err(err).Msg("Unexpected error when setting as accepted locally")
		return newCreationError("Unexpected local error")
	}

	// Now we have a majority of replicas
	log.Info().
		Str("reason", reason).
		Msgf("Setting transaction for T%d (%d) as committed (failover)", downBroker, token)

	// We can now start receiving producer traffic for this token
	if err := o.discoverer.SetAsCommitted(gen.Start, nil, gen.Tx, topology.MyOrdinal()); err != nil {
		log.Err(err).Msg("Set as committed locally failed (probably local db related)")
		return newCreationError("Set as committed locally failed")
	}
	_ = o.gossiper.SetAsCommitted(peerFollower, gen.Start, nil, gen.Tx)

	return nil
}

func getTx(gen *Generation) *uuid.UUID {
	if gen == nil {
		return nil
	}
	return &gen.Tx
}
