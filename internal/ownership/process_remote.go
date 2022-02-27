package ownership

import "github.com/rs/zerolog/log"

func (o *generator) processRemoteProposed(m *remoteGenProposedMessage) creationError {
	log.Debug().Msgf(
		"Setting generation for token %d with remote leader B%d version %d as %s",
		m.gen.Start,
		m.gen.Leader,
		m.gen.Version,
		m.gen.Status)

	if m.gen2 != nil {
		log.Debug().Msgf(
			"Also setting generation for token %d with remote leader B%d version %d as %s",
			m.gen2.Start,
			m.gen2.Leader,
			m.gen2.Version,
			m.gen2.Status)
	}

	err := o.discoverer.SetGenerationProposed(m.gen, m.gen2, m.expectedTx)
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
	log.Debug().Msgf("Setting generation for token %d tx %s as committed", m.token1, m.tx)
	if m.token2 != nil {
		log.Debug().Msgf("Also setting generation for token %d tx %s as committed", m.token2, m.tx)
	}

	err := o.discoverer.SetAsCommitted(m.token1, m.token2, m.tx, m.origin)
	if err != nil {
		log.Err(err).Msgf("Failed to set generation for token %d tx %s as committed", m.token1, m.tx)
	}
	return wrapIfErr(err)
}
