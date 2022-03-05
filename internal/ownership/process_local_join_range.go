package ownership

import "github.com/rs/zerolog/log"

func (o *generator) processLocalJoinRange(m *localJoinRangeGenMessage) creationError {
	topology := m.topology
	myToken := topology.MyToken()
	nextBroker := m.previousTopology.NextBroker()
	// nextToken := previousTopology.GetToken(previousTopology.NextIndex())
	// newNextToken := topology.GetToken(topology.NextIndex())
	newNextBroker := topology.NextBroker()

	log.Info().Msgf(
		"Start joining ranges T%d-T%d and T%d-T%d into T%d-T%d",
		topology.MyOrdinal(), nextBroker.Ordinal, nextBroker.Ordinal, newNextBroker.Ordinal,
		topology.MyOrdinal(), newNextBroker.Ordinal)

	// Read state of my current token generation
	_, _ = o.discoverer.GenerationProposed(myToken)

	// TODO: Continue

	return nil
}
