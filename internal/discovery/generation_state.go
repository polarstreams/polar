package discovery

import (
	"fmt"
	"sync/atomic"

	. "github.com/polarstreams/polar/internal/types"
	"github.com/polarstreams/polar/internal/utils"
	. "github.com/google/uuid"
	"github.com/rs/zerolog/log"
)

type GenerationState interface {
	// Generation gets a snapshot of the active generation by token.
	// This is part of the hot path.
	Generation(token Token) *Generation

	// GenerationInfo gets the information of a past committed generation.
	// Returns nil when not found.
	GenerationInfo(id GenId) *Generation

	// For an old generation, get's the following generation (or two in the case of split).
	// Returns nil when not found.
	NextGeneration(id GenId) []Generation

	// GenerationProposed reads a snapshot of the current committed and proposed generations
	GenerationProposed(token Token) (committed *Generation, proposed *Generation)

	// SetProposed compares and sets the proposed/accepted generation.
	// It's possible to accept multiple generations in the same operation by providing gen2.
	//
	// Checks that the previous tx matches or is nil.
	// Also checks that provided gen.version is equal to committed plus one.
	SetGenerationProposed(gen *Generation, gen2 *Generation, expectedTx *UUID) error

	// SetAsCommitted sets the transaction as committed, storing the history and
	// setting the proposed generation as committed
	//
	// Returns an error when transaction does not match
	SetAsCommitted(token1 Token, token2 *Token, tx UUID, origin int) error

	// Sets the transaction as committed, storing the history, without checking proposed values.
	// This can only be called when no other concurrent changes can be made to the generations.
	//
	// Returns an error when the data could not be persisted
	RepairCommitted(gen *Generation) error

	// Determines whether there's active range containing (but not starting) the token
	IsTokenInRange(token Token) bool

	// Determines whether there's history matching the token
	HasTokenHistory(token Token, clusterSize int) (bool, error)

	// Gets the last known committed token from the local persistence
	GetTokenHistory(token Token, clusterSize int) (*Generation, error)

	// Gets the parent token and range for any given token+range based on the generation information
	// For example: T3/0 -> T0/2.
	// When there is no parent, it returns a nil slice
	ParentRanges(gen *Generation, indices []RangeIndex) []GenerationRanges
}

// Loads all generations from local storage
func (d *discoverer) loadGenerations() error {
	defer d.genMutex.Unlock()
	d.genMutex.Lock()

	if existing := d.generations.Load(); existing != nil {
		if existingMap := existing.(genMap); len(existingMap) > 0 {
			return fmt.Errorf("Generation map is not empty")
		}
	}

	genList, err := d.localDb.LatestGenerations()
	if err != nil {
		return err
	}

	newMap := make(genMap)
	for _, gen := range genList {
		newMap[gen.Start] = gen
	}
	d.generations.Store(newMap)
	return nil
}

func (d *discoverer) Generation(token Token) *Generation {
	existingMap := d.generations.Load().(genMap)

	if v, ok := existingMap[token]; ok {
		return &v
	}
	return nil
}

func (d *discoverer) GenerationInfo(id GenId) *Generation {
	gen, err := d.localDb.GenerationInfo(id.Start, id.Version)
	utils.PanicIfErr(err, "Generation info failed to be retrieved")
	return gen
}

func (d *discoverer) NextGeneration(id GenId) []Generation {
	gen := d.GenerationInfo(id)
	if gen == nil {
		return nil
	}

	if current := d.Generation(id.Start); current != nil && current.Version == id.Version {
		return nil
	}

	nextGens, err := d.localDb.GenerationsByParent(gen)
	utils.PanicIfErr(err, "Generations by parent failed to be retrieved")

	return nextGens
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
		// Note: end token is never contained
		if token > gen.Start && (token < gen.End || gen.End == StartToken) {
			return true
		}
	}
	return false
}

func (d *discoverer) HasTokenHistory(token Token, clusterSize int) (bool, error) {
	result, err := d.localDb.GetGenerationsByToken(token, clusterSize)
	return len(result) > 0, err
}

func (d *discoverer) GetTokenHistory(token Token, clusterSize int) (*Generation, error) {
	result, err := d.localDb.GetGenerationsByToken(token, clusterSize)
	if len(result) == 0 {
		return nil, err
	}
	return &result[0], nil
}

func (d *discoverer) SetGenerationProposed(gen *Generation, gen2 *Generation, expectedTx *UUID) error {
	defer d.genMutex.Unlock()
	d.genMutex.Lock()

	if gen2 != nil {
		return d.acceptMultiple(gen, gen2)
	}

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

	if err := d.validateCommitted(gen); err != nil {
		return err
	}

	if !gen.ToDelete {
		log.Info().Msgf(
			"%s v%d with B%d as leader for range [%d, %d]",
			gen.Status, gen.Version, gen.Leader, gen.Start, gen.End)
	} else {
		log.Info().Msgf(
			"%s delete of token range [%d, %d] with last known version v%d",
			gen.Status, gen.Start, gen.End, gen.Parents[0].Version)
	}

	// Replace entire proposed value
	d.genProposed[gen.Start] = *gen

	return nil
}

// Verifies that the new generation is greater than the committed version
func (d *discoverer) validateCommitted(gen *Generation) error {
	if committed := d.Generation(gen.Start); committed != nil && gen.Version <= committed.Version {
		return fmt.Errorf(
			"Proposed version is not the next version of committed: committed = %d, proposed = %d",
			committed.Version,
			gen.Version)
	}
	return nil
}

// Marks multiple generations as Accepted. It should be called after acquiring the lock
func (d *discoverer) acceptMultiple(gen1 *Generation, gen2 *Generation) error {
	if gen1.Status != StatusAccepted || gen2.Status != StatusAccepted {
		return fmt.Errorf("Multiple generations can not be proposed, only accepted")
	}

	if existingGen1, ok := d.genProposed[gen1.Start]; !ok || existingGen1.Tx != gen1.Tx {
		return fmt.Errorf("Existing proposed for generation #1 does not match: (was found %v)", ok)
	}
	if existingGen2, ok := d.genProposed[gen2.Start]; !ok || existingGen2.Tx != gen2.Tx {
		return fmt.Errorf("Existing proposed for generation #2 does not match: (was found %v)", ok)
	}

	// Get existing committed
	if err := d.validateCommitted(gen1); err != nil {
		return err
	}
	if err := d.validateCommitted(gen2); err != nil {
		return err
	}

	d.genProposed[gen1.Start] = *gen1
	d.genProposed[gen2.Start] = *gen2

	if !gen2.ToDelete {
		log.Info().Msgf(
			"Accepted two generations: [%d, %d] v%d with B%d as leader and [%d, %d] v%d with B%d as leader (B%d as tx leader)",
			gen1.Start, gen1.End, gen1.Version, gen1.Leader, gen2.Start, gen2.End, gen2.Version, gen2.Leader, gen2.TxLeader)
	} else {
		log.Info().Msgf(
			"Accepted two generations: [%d, %d] v%d with B%d as leader and a generation to delete range [%d, %d] (B%d as tx leader)",
			gen1.Start, gen1.End, gen1.Version, gen1.Leader, gen2.Start, gen2.End, gen2.TxLeader)
	}
	return nil
}

func (d *discoverer) SetAsCommitted(token1 Token, token2 *Token, tx UUID, origin int) error {
	defer d.genMutex.Unlock()
	d.genMutex.Lock()

	var gen2 *Generation

	// Set the transaction and the generation value as committed
	gen1, ok := d.genProposed[token1]
	if !ok {
		return fmt.Errorf("No proposed value found for token %d", token1)
	}
	if gen1.Tx != tx {
		return fmt.Errorf("Transaction does not match")
	}

	if token2 != nil {
		if g, ok := d.genProposed[*token2]; !ok {
			return fmt.Errorf("No proposed value found for second token %d, %v", *token2, d.genProposed)
		} else if g.Tx != tx {
			return fmt.Errorf("Transaction does not match for token %d (%s != %s)", *token2, g.Tx, tx)
		} else {
			gen2 = &g
		}
	}

	gen1.Status = StatusCommitted

	if gen2 == nil {
		log.Info().Msgf(
			"Committing [%d, %d] v%d with B%d as leader", gen1.Start, gen1.End, gen1.Version, gen1.Leader)
	} else {
		if !gen2.ToDelete {
			log.Info().Msgf(
				"Committing both [%d, %d] v%d with B%d as leader and [%d, %d] v%d with B%d as leader",
				gen1.Start, gen1.End, gen1.Version, gen1.Leader,
				gen2.Start, gen2.End, gen2.Version, gen2.Leader)
		} else {
			log.Info().Msgf(
				"Committing [%d, %d] v%d with B%d as leader for joined ranges",
				gen1.Start, gen1.End, gen1.Version, gen1.Leader)
		}
		gen2.Status = StatusCommitted
	}

	gen2ForDb := gen2
	if gen2 != nil && gen2.ToDelete {
		// Don't persist in the database the removed generation
		gen2ForDb = nil
	}

	// Store the history and the tx table first
	// that way db failures don't affect local state
	if err := d.localDb.CommitGeneration(&gen1, gen2ForDb); err != nil {
		return err
	}

	copyAndStore(&d.generations, gen1, gen2)

	// Remove from proposed
	delete(d.genProposed, token1)
	if token2 != nil {
		delete(d.genProposed, *token2)
	}
	return nil
}

func (d *discoverer) ParentRanges(gen *Generation, indices []RangeIndex) []GenerationRanges {
	if gen == nil {
		panic("Generation can not be nil when looking for parent ranges")
	}
	if len(gen.Parents) == 0 {
		return nil
	}

	currentToken := gen.Start
	if len(gen.Parents) == 1 {
		parentGen := d.GenerationInfo(gen.Parents[0])
		if parentGen == nil {
			log.Error().Msgf("Could not find generation info %s for reader projection", gen.Parents[0])
			return nil
		}
		// Ranges are maintained
		return []GenerationRanges{{Generation: parentGen, Indices: indices}}
	}

	tokens := make([]GenerationRanges, 0)
	middleIndex := RangeIndex(d.config.ConsumerRanges()) / 2
	for _, parentId := range gen.Parents {
		// We've got to project the range indices
		t := parentId.Start
		parentGen := d.GenerationInfo(parentId)
		if parentGen == nil {
			log.Error().Msgf("Could not find generation info %s for reader projection", parentId)
			continue
		}
		for _, index := range indices {
			parentIndices := make([]RangeIndex, 0)
			if t == currentToken {
				if index >= middleIndex {
					// This range is projected into the following range of the second token
					continue
				}

				// For example: range T0/1, gets projected into T0/2 and T0/2 w/ four con consumer ranges
				parentIndices = append(parentIndices, index*2)
				parentIndices = append(parentIndices, index*2+1)
			} else {
				if index < middleIndex {
					// This range is projected into the previous range of the first token
					continue
				}

				// For example: range T0/3, gets projected into T3/2 and T3/3
				parentIndices = append(parentIndices, (index-middleIndex)*2)
				parentIndices = append(parentIndices, (index-middleIndex)*2+1)
			}
			tokens = append(tokens, GenerationRanges{Generation: parentGen, Indices: parentIndices})
		}
	}
	return tokens
}

func (d *discoverer) RepairCommitted(gen *Generation) error {
	if gen.ToDelete {
		log.Panic().Msgf("Repair generations to delete is not supported")
	}

	defer d.genMutex.Unlock()
	d.genMutex.Lock()

	// Set the transaction and the generation value as committed
	gen.Status = StatusCommitted

	log.Info().Msgf(
		"Committing [%d, %d] v%d with B%d as leader as part of repair", gen.Start, gen.End, gen.Version, gen.Leader)

	if err := d.localDb.CommitGeneration(gen, nil); err != nil {
		return err
	}

	copyAndStore(&d.generations, *gen, nil)

	// Remove from proposed
	delete(d.genProposed, gen.Start)
	return nil
}

func copyAndStore(generations *atomic.Value, gen Generation, gen2 *Generation) {
	existingMap := generations.Load().(genMap)

	// Shallow copy existing
	newMap := make(genMap, len(existingMap))
	for k, v := range existingMap {
		newMap[k] = v
	}

	if gen.ToDelete {
		delete(newMap, gen.Start)
	} else {
		newMap[gen.Start] = gen
	}

	if gen2 != nil {
		if gen2.ToDelete {
			delete(newMap, gen2.Start)
		} else {
			newMap[gen2.Start] = *gen2
		}
	}

	generations.Store(newMap)
}
