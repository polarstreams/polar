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
	index := m.brokerIndex
	downBroker := m.broker
	peerFollower := topology.NaturalFollowers(index)[1]

	log.Debug().Msgf("Processing local token failover B%d", downBroker.Ordinal)
	isUp, err := o.gossiper.ReadBrokerIsUp(peerFollower, downBroker.Ordinal)
	if err != nil {
		return wrapCreationError(err)
	}

	if isUp {
		return newCreationError("Broker B%d is still consider as UP by B%d", downBroker.Ordinal, peerFollower)
	}

	token := topology.GetToken(index)

	gen := Generation{
		Start:     token,
		End:       topology.GetToken(index + 1),
		Version:   m.previousGen.Version + 1,
		Timestamp: time.Now().UnixMicro(),
		Leader:    topology.MyOrdinal(),
		Followers: []int{peerFollower, downBroker.Ordinal},
		TxLeader:  topology.MyOrdinal(),
		Tx:        uuid.New(),
		Status:    StatusProposed,
		Parents: []GenParent{{
			Start:   token,
			Version: m.previousGen.Version,
		}},
	}

	log.Info().
		Str("reason", reason).
		Msgf("Proposing myself as leader of T%d (%d) in v%d", downBroker.Ordinal, token, gen.Version)

	committed, proposed := o.discoverer.GenerationProposed(token)

	if !reflect.DeepEqual(m.previousGen, committed) {
		log.Info().Msgf("New committed generation found, aborting creation")
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
		Msgf("Accepting myself as leader of T%d (%d) in v%d", downBroker.Ordinal, token, gen.Version)
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
		Msgf("Setting transaction for T%d (%d) as committed", downBroker.Ordinal, token)

	// We can now start receiving producer traffic for this token
	if err := o.discoverer.SetAsCommitted(gen.Start, nil, gen.Tx, topology.MyOrdinal()); err != nil {
		log.Err(err).Msg("Set as committed locally failed (probably local db related)")
		return newCreationError("Set as committed locally failed")
	}
	o.gossiper.SetAsCommitted(peerFollower, gen.Start, nil, gen.Tx)

	return nil
}

func getTx(gen *Generation) *uuid.UUID {
	if gen == nil {
		return nil
	}
	return &gen.Tx
}
