package ownership

import "github.com/rs/zerolog/log"

func (o *generator) processRemoteProposed(m *remoteGenProposedMessage) creationError {
	log.Debug().Msgf(
		"Setting generation for token %d with remote leader B%d version %d as %s",
		m.gen.Start,
		m.gen.Leader,
		m.gen.Version,
		m.gen.Status)

	err := o.discoverer.SetGenerationProposed(m.gen, m.expectedTx)
	if err != nil {
		log.Err(err).Msgf(
			"Failed to set generation for token %d with remote leader B%d version %d as %s",
			m.gen.Start,
			m.gen.Leader,
			m.gen.Version,
			m.gen.Status)
	}
	return wrapIfErr(err)
}

func (o *generator) processRemoteCommitted(m *remoteGenCommittedMessage) creationError {
	log.Debug().Msgf("Setting generation for token %d tx %s as committed", m.token, m.tx)

	err := o.discoverer.SetAsCommitted(m.token, m.tx, m.origin)
	if err != nil {
		log.Err(err).Msgf("Failed to set generation for token %d tx %s as committed", m.token, m.tx)
	}
	return wrapIfErr(err)
}
