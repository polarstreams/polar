package discovery

import (
	"fmt"
	"sync/atomic"

	. "github.com/google/uuid"
	. "github.com/jorgebay/soda/internal/types"
	"github.com/rs/zerolog/log"
)

type GenerationState interface {
	// Generation gets a snapshot of the active generation by token.
	// This is part of the hot path.
	Generation(token Token) *Generation

	// GenerationProposed reads a snapshot of the current committed and proposed generations
	GenerationProposed(token Token) (committed *Generation, proposed *Generation)

	// SetProposed compares and sets the proposed/accepted generation.
	//
	// Checks that the previous tx matches or is null.
	// Also checks that provided gen.version is equal to committed plus one.
	SetGenerationProposed(gen *Generation, expectedTx *UUID) error

	// SetAsCommitted sets the transaction as committed, storing the history and
	// setting the proposed generation as committed
	//
	// Returns an error when transaction does not match
	SetAsCommitted(token Token, tx UUID, origin int) error

	// Determines whether there's active range containing (but not starting) the token
	IsTokenInRange(token Token) bool

	// Determines whether there's history matching the token
	HasTokenHistory(token Token) (bool, error)
}

func (d *discoverer) Generation(token Token) *Generation {
	existingMap := d.generations.Load().(genMap)

	if v, ok := existingMap[token]; ok {
		return &v
	}
	return nil
}

func (d *discoverer) GenerationProposed(token Token) (committed *Generation, proposed *Generation) {
	defer d.genMutex.Unlock()
	// All access to gen proposed must be lock protected
	d.genMutex.Lock()

	proposed = nil
	if v, ok := d.genProposed[token]; ok {
		proposed = &v
	}

	committed = d.Generation(token)
	return
}

func (d *discoverer) IsTokenInRange(token Token) bool {
	generationMap := d.generations.Load().(genMap)

	// O(n) is not that bad given the number of
	// active generations managed by broker
	for _, gen := range generationMap {
		// containing the token but not the start token
		if token <= gen.End && token > gen.Start {
			return true
		}
	}
	return false
}

func (d *discoverer) HasTokenHistory(token Token) (bool, error) {
	result, err := d.localDb.GetGenerationsByToken(token)
	return len(result) > 0, err
}

func (d *discoverer) SetGenerationProposed(gen *Generation, expectedTx *UUID) error {
	defer d.genMutex.Unlock()
	d.genMutex.Lock()

	var currentTx *UUID = nil
	if existingGen, ok := d.genProposed[gen.Start]; ok {
		currentTx = &existingGen.Tx
	}

	if currentTx != nil && expectedTx == nil {
		return fmt.Errorf("Existing transaction is not nil")
	}

	if currentTx == nil && expectedTx != nil {
		return fmt.Errorf("Existing transaction is nil and expected not to be")
	}

	if expectedTx != nil && currentTx != nil && *currentTx != *expectedTx {
		return fmt.Errorf("Existing proposed does not match: %s (expected %s)", currentTx, expectedTx)
	}

	// Get existing committed
	committed := d.Generation(gen.Start)

	if committed != nil && gen.Version <= committed.Version {
		return fmt.Errorf(
			"Proposed version is not the next version of committed: committed = %d, proposed = %d",
			committed.Version,
			gen.Version)
	}

	log.Info().Msgf(
		"%s version %d with leader %d for range [%d, %d]",
		gen.Status, gen.Version, gen.Leader, gen.Start, gen.End)

	// Replace entire proposed value
	d.genProposed[gen.Start] = *gen

	return nil
}

func (d *discoverer) SetAsCommitted(token Token, tx UUID, origin int) error {
	defer d.genMutex.Unlock()
	d.genMutex.Lock()

	// Set the transaction and the generation value as committed
	gen, ok := d.genProposed[token]

	if !ok {
		return fmt.Errorf("No proposed value found")
	}

	if gen.Tx != tx {
		return fmt.Errorf("Transaction does not match")
	}

	log.Info().Msgf(
		"Setting committed version %d with leader %d for range [%d, %d]", gen.Version, gen.Leader, gen.Start, gen.End)
	gen.Status = StatusCommitted

	// Store the history and the tx table first
	// that way db failures don't affect local state
	if err := d.localDb.CommitGeneration(&gen); err != nil {
		return err
	}

	copyAndStore(&d.generations, gen)

	// Remove from proposed
	delete(d.genProposed, token)
	return nil
}

func copyAndStore(generations *atomic.Value, gen Generation) {
	existingMap := generations.Load().(genMap)

	// Shallow copy existing
	newMap := make(genMap, len(existingMap))
	for k, v := range existingMap {
		newMap[k] = v
	}

	if !gen.ToDelete {
		newMap[gen.Start] = gen
	} else {
		delete(newMap, gen.Start)
	}
	generations.Store(newMap)
}
