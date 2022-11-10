package consuming

import (
	"fmt"
	"os"
	"sort"
	"sync"

	"github.com/barcostreams/barco/internal/conf"
	"github.com/barcostreams/barco/internal/data"
	"github.com/barcostreams/barco/internal/discovery"
	"github.com/barcostreams/barco/internal/interbroker"
	"github.com/barcostreams/barco/internal/localdb"
	. "github.com/barcostreams/barco/internal/types"
	"github.com/barcostreams/barco/internal/utils"
	"github.com/rs/zerolog/log"
)

type offsetRange struct {
	// The start token of the range.
	// Note that this accounts for range indices and differs from the the generation range
	start Token
	end   Token
	value Offset
}

func newDefaultOffsetState(
	localDb localdb.Client,
	discoverer discovery.TopologyGetter,
	datalog data.Datalog,
	gossiper interbroker.Gossiper,
	config conf.ConsumerConfig,
) OffsetState {
	state := &defaultOffsetState{
		offsetMap:  make(map[OffsetStoreKey][]offsetRange),
		commitChan: make(chan *OffsetStoreKeyValue, 64),
		localDb:    localDb,
		gossiper:   gossiper,
		datalog:    datalog,
		discoverer: discoverer,
		config:     config,
	}
	go state.processCommit()
	return state
}

// Stores offsets by range, gets and sets offsets in the local storage and in peers.
type defaultOffsetState struct {
	offsetMap  map[OffsetStoreKey][]offsetRange // A map of sorted lists of offset ranges
	mu         sync.RWMutex
	commitChan chan *OffsetStoreKeyValue // We need to commit offset in order
	localDb    localdb.Client
	gossiper   interbroker.Gossiper
	datalog    data.Datalog
	discoverer discovery.TopologyGetter
	config     conf.ConsumerConfig
}

func (s *defaultOffsetState) Init() error {
	// Load local offsets into memory
	values, err := s.localDb.Offsets()
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	for _, kv := range values {
		s.setMap(kv.Key, &kv.Value)
	}

	return nil
}

func (s *defaultOffsetState) String() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return fmt.Sprint(s.offsetMap)
}

func (s *defaultOffsetState) Get(
	group string,
	topic string,
	token Token,
	index RangeIndex,
	clusterSize int,
) (offset *Offset, rangesMatch bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	offsetList, start, end := s.getOffsetRanges(group, topic, token, index, clusterSize)
	length := len(offsetList)
	if length == 0 {
		return
	}

	item := offsetList[length-1]
	rangesMatch = length == 1 && item.start == start && item.end == end
	offset = &item.value
	return offset, rangesMatch
}

func (s *defaultOffsetState) GetAllWithDefaults(
	group string,
	topic string,
	token Token,
	index RangeIndex,
	clusterSize int,
	policy OffsetResetPolicy,
) []Offset {
	s.mu.RLock()
	defer s.mu.RUnlock()
	offsetList, start, end := s.getOffsetRanges(group, topic, token, index, clusterSize)

	result := make([]Offset, 0, len(offsetList))
	if len(offsetList) == 0 || offsetList[0].start > start {
		// We need to create the defaults for the beginning
		toCreateEnd := end
		if len(offsetList) > 0 {
			toCreateEnd = offsetList[0].start
		}
		defaultOffsets := s.getDefaultsForRange(start, toCreateEnd, clusterSize, offsetList, policy)
		result = append(result, defaultOffsets...)
	}

	for _, item := range offsetList {
		result = append(result, item.value)
	}

	if len(offsetList) > 0 && offsetList[len(offsetList)-1].end < end {
		toCreateStart := offsetList[len(offsetList)-1].end
		defaultOffsets := s.getDefaultsForRange(toCreateStart, end, clusterSize, offsetList, policy)
		result = append(result, defaultOffsets...)
	}

	return result
}

// Gets a default offset for ranges that are not present.
// When there are no siblings for that contained range, return the earliest parent.
// When there are siblings, return at most a generation with the same cluster size,
// as we shouldn't go back to past generations of different sizes.
func (s *defaultOffsetState) getDefaultsForRange(
	start Token,
	end Token,
	clusterSize int,
	siblings []*offsetRange,
	policy OffsetResetPolicy,
) []Offset {
	// TODO: Check what's stored for StartFromEarliest instead of starting at 0
	result := make([]Offset, 0)
	rangesPerToken := RangeIndex(s.config.ConsumerRanges())
	// Get the tokens for the current size and intersect them
	for i := 0; i < clusterSize; i++ {
		token := GetTokenAtIndex(clusterSize, i)
		if token > end {
			break
		}
		for index := RangeIndex(0); index < rangesPerToken; index++ {
			rangeStart, rangeEnd := RangeByTokenAndClusterSize(token, index, int(rangesPerToken), clusterSize)
			if !Intersects(start, end, rangeStart, rangeEnd) {
				continue
			}

			// Navigate through the parent until the first ones and set to zero
			for _, genRanges := range s.firstGenerations(token, index, clusterSize, siblings) {
				gen := genRanges.Generation

				for _, genIndex := range genRanges.Indices {
					rangeStart, rangeEnd := RangeByTokenAndClusterSize(gen.Start, genIndex, int(rangesPerToken), gen.ClusterSize)
					if !Intersects(start, end, rangeStart, rangeEnd) {
						// The first generation included parts that we weren't interested, move on
						continue
					}

					result = append(result, Offset{
						Token:       gen.Start,
						Index:       genIndex,
						Version:     gen.Version,
						ClusterSize: gen.ClusterSize,
						Offset:      0, // TODO: Get earliest segment
						Source:      OffsetSource{},
					})
				}
			}
		}
	}

	return result
}

// Navigates through parents until it gets the first, returning an unique set of generations alongside its ranges
func (s *defaultOffsetState) firstGenerations(token Token, index RangeIndex, clusterSize int, siblings []*offsetRange) []GenerationRanges {
	if len(siblings) > 0 {
		return s.firstGenerationBySibling(token, index, clusterSize, siblings[0].value)
	}

	gen, err := s.discoverer.GetTokenHistory(token, clusterSize)
	utils.PanicIfErr(err, "Generation history could not be retrieved")

	if gen == nil {
		return nil
	}

	queue := []GenerationRanges{{Generation: gen, Indices: []RangeIndex{index}}}
	roots := make(map[GenId]map[RangeIndex]bool, 0)
	rootsGen := make(map[GenId]*Generation, 0)
	for len(queue) > 0 {
		item := queue[0]
		queue = queue[1:]
		parents := s.discoverer.ParentRanges(item.Generation, item.Indices)
		if len(parents) == 0 {
			// Append to the root
			gen := item.Generation
			rootsGen[gen.Id()] = gen
			indices, found := roots[gen.Id()]
			if !found {
				indices = make(map[RangeIndex]bool)
				roots[gen.Id()] = indices
			}
			for _, i := range item.Indices {
				indices[i] = true
			}
			continue
		}
		queue = append(queue, parents...)
	}

	// Adapt the format to return
	result := make([]GenerationRanges, 0, len(roots))
	for genId, indicesMap := range roots {
		indices := make([]RangeIndex, 0, len(indicesMap))
		for k := range indicesMap {
			indices = append(indices, k)
		}
		result = append(result, GenerationRanges{Generation: rootsGen[genId], Indices: indices})
	}

	return result
}

// Navigates up through a sibling offset, until the cluster size changes
func (s *defaultOffsetState) firstGenerationBySibling(
	token Token,
	index RangeIndex,
	clusterSize int,
	siblingOffset Offset,
) []GenerationRanges {
	root := s.discoverer.GenerationInfo(siblingOffset.GenId())
	if root == nil {
		return nil
	}
	if root.ClusterSize != siblingOffset.ClusterSize {
		panic("Invalid offset data")
	}
	expectedClusterSize := root.ClusterSize
	if len(root.Parents) == 1 {
		// Navigate up until the cluster size changes
		for len(root.Parents) > 0 {
			parentGen := s.discoverer.GenerationInfo(root.Parents[0])
			if parentGen == nil || parentGen.ClusterSize != expectedClusterSize {
				break
			}
			root = parentGen
		}
	}

	if clusterSize == expectedClusterSize {
		return []GenerationRanges{{Generation: root, Indices: []RangeIndex{index}}}
	}
	// Translate ranges into that cluster size using actual ranges
	result := make([]GenerationRanges, 0)
	projected := ProjectRangeByClusterSize(token, index, s.config.ConsumerRanges(), clusterSize, expectedClusterSize)
	for _, tr := range projected {
		for _, i := range tr.Indices {
			result = append(result, GenerationRanges{Generation: root, Indices: []RangeIndex{i}})
		}
	}
	return result
}

// Retrieves the offsets for the given range
//
// The caller MUST hold the lock
func (s *defaultOffsetState) getOffsetRanges(
	group string,
	topic string,
	token Token,
	index RangeIndex,
	clusterSize int,
) ([]*offsetRange, Token, Token) {
	start, end := RangeByTokenAndClusterSize(token, index, s.config.ConsumerRanges(), clusterSize)
	key := OffsetStoreKey{Group: group, Topic: topic}
	list, found := s.offsetMap[key]
	if !found {
		return nil, start, end
	}

	offsetIndex := binarySearch(list, end)
	foundGreaterRanges := offsetIndex < len(list)
	if !foundGreaterRanges {
		offsetIndex = len(list) - 1
	}
	item := list[offsetIndex]

	if item.start == start && item.end == end {
		return []*offsetRange{&item}, start, end
	}

	// It might not be contained
	if !Intersects(item.start, item.end, start, end) {
		return nil, start, end
	}

	// When its contained, return all the matching ranges
	result := make([]*offsetRange, 0, 2)
	if foundGreaterRanges {
		result = append(result, &list[offsetIndex])
	}
	// result = append([]int{i}, result...)
	for i := offsetIndex - 1; i >= 0; i-- {
		item := list[i]
		if !Intersects(item.start, item.end, start, end) {
			break
		}
		result = append([]*offsetRange{&item}, result...)
	}

	return result, start, end
}

// Returns the index of the first match where the end of the range is greater than or equal to the provided end.
//
// The returned index could be an exact match, the last matching range or len(list).
func binarySearch(list []offsetRange, end Token) int {
	return sort.Search(len(list), func(i int) bool {
		item := list[i]
		return item.end >= end
	})
}

func (s *defaultOffsetState) Set(
	group string,
	topic string,
	value Offset,
	commit OffsetCommitType,
) bool {
	// Set can be called from the reader, a peer or to move offset (queue)
	key := OffsetStoreKey{Group: group, Topic: topic}

	// Callers of setMap() and moveCompletedOffset() must hold the lock
	s.mu.Lock()
	addedToMap := s.setMap(key, &value)
	if value.Offset == OffsetCompleted {
		s.moveCompletedOffset(key, &value)
	}
	s.mu.Unlock()

	if !addedToMap {
		return false
	}

	if commit != OffsetCommitNone {
		// Store commits locally in order but don't await for it to complete
		kv := &OffsetStoreKeyValue{Key: key, Value: value}
		s.commitChan <- kv

		if commit == OffsetCommitAll {
			// Send to followers in the background with no order guarantees
			// The local OffsetState of the follower will verify for new values
			go s.sendToFollowers(kv)
		}
	}

	return true
}

func (s *defaultOffsetState) moveCompletedOffset(key OffsetStoreKey, value *Offset) {
	consumerRanges := RangeIndex(s.config.ConsumerRanges())
	gen := s.discoverer.GenerationInfo(GenId{Start: value.Token, Version: value.Version})
	if gen == nil {
		log.Error().Msgf("Offset could not be moved as gen %d v%d could not be found", value.Token, value.Version)
		return
	}

	nextGens := s.discoverer.NextGeneration(gen.Id())
	if len(nextGens) == 0 {
		log.Error().Msgf("Offset could not be moved as next gen for %d v%d could not be found",
			value.Token, value.Version)
		return
	}

	if len(nextGens) == 1 && nextGens[0].ClusterSize == gen.ClusterSize && nextGens[0].Start == gen.Start {
		// Same range
		newValue := *value
		newValue.Version = nextGens[0].Version
		newValue.Offset = 0
		log.Info().Msgf("Moving offset version for token %d/%d from v%d to v%d",
			gen.Start, newValue.Index, value.Version, newValue.Version)
		s.setMap(key, &newValue)
		return
	}

	if len(nextGens) == 2 && nextGens[0].ClusterSize > gen.ClusterSize {
		// Split range
		range1Value := *value
		range1Value.ClusterSize = nextGens[0].ClusterSize
		range1Value.Offset = 0
		range2Value := *value
		range2Value.ClusterSize = nextGens[0].ClusterSize
		range2Value.Offset = 0
		originalGenerationIndex := utils.FindGenByToken(nextGens, value.Token)
		if originalGenerationIndex == -1 {
			log.Panic().Msgf("Original generation index not found")
		}

		if value.Index < consumerRanges/2 {
			// Same token
			nextGen := nextGens[originalGenerationIndex]
			range1Value.Version = nextGen.Version
			range1Value.Index = value.Index * 2
			range2Value.Version = nextGen.Version
			range2Value.Index = value.Index*2 + 1
			log.Info().Msgf("Moving offset after scaling up to with token from %d/%d v%d to %d/%d v%d and %d/%d v%d",
				gen.Start, value.Index, gen.Version,
				range1Value.Token, range1Value.Index, range1Value.Version,
				range2Value.Token, range2Value.Index, range2Value.Version)
		} else {
			// Next token
			nextTokenGen := nextGens[(originalGenerationIndex+1)%2]
			range1Value.Token = nextTokenGen.Start
			range1Value.Version = nextTokenGen.Version
			range1Value.Index = value.Index*2 - consumerRanges
			range2Value.Token = nextTokenGen.Start
			range2Value.Version = nextTokenGen.Version
			range2Value.Index = value.Index*2 - consumerRanges + 1

			log.Info().Msgf("Moving offset after scaling up to next token from %d/%d v%d to %d/%d v%d and %d/%d v%d",
				gen.Start, value.Index, gen.Version,
				range1Value.Token, range1Value.Index, range1Value.Version,
				range2Value.Token, range2Value.Index, range2Value.Version)
		}

		s.setMap(key, &range1Value)
		s.setMap(key, &range2Value)
		return
	}

	if len(nextGens) == 1 && len(nextGens[0].Parents) == 2 && nextGens[0].ClusterSize < gen.ClusterSize {
		// Join range when sibling completed as well
		siblingRangeIndex := value.Index - 1
		if value.Index%2 == 0 {
			siblingRangeIndex = value.Index + 1
		}

		siblingStart, siblingEnd := RangeByTokenAndClusterSize(
			value.Token, siblingRangeIndex, int(consumerRanges), value.ClusterSize)

		list := s.offsetMap[key]
		siblingIndex := binarySearch(list, siblingEnd)
		if siblingIndex == len(list) {
			// We couldn't find it
			return
		}
		siblingOffset := list[siblingIndex]
		if siblingOffset.start != siblingStart && siblingOffset.end != siblingEnd {
			// We couldn't find the sibling, hopefully it will resolve itself in the future
			return
		}
		if siblingOffset.value.Offset != OffsetCompleted {
			// We must wait until the sibling completes
			return
		}

		var targetRangeIndex RangeIndex
		if value.Token == nextGens[0].Start {
			// Its the token that continues to be in the cluster
			targetRangeIndex = value.Index / 2
		} else {
			targetRangeIndex = consumerRanges/2 + value.Index/2
		}

		log.Info().Msgf("Moving offset after scaling down to %d/%d v%d (cluster size %d)",
			nextGens[0].Start, targetRangeIndex, nextGens[0].Version, nextGens[0].ClusterSize)
		s.setMap(key, &Offset{
			Token:       nextGens[0].Start,
			Version:     nextGens[0].Version,
			ClusterSize: nextGens[0].ClusterSize,
			Offset:      0,
			Index:       targetRangeIndex,
			Source:      value.Source,
		})
		return
	}

	log.Panic().Msgf("Invalid next generations for %s", gen.Id())
}

// Sets the offset in the map
//
// Callers of this function MUST hold the lock
func (s *defaultOffsetState) setMap(key OffsetStoreKey, value *Offset) bool {
	start, end := RangeByTokenAndClusterSize(value.Token, value.Index, s.config.ConsumerRanges(), value.ClusterSize)
	list, found := s.offsetMap[key]
	if !found {
		s.offsetMap[key] = []offsetRange{{
			start: start,
			end:   end,
			value: *value,
		}}
		return true
	}

	offsetIndex := binarySearch(list, end)
	endIsGreater := offsetIndex == len(list)
	if endIsGreater {
		offsetIndex--
	}
	item := list[offsetIndex]

	// Check ranges exact match (most common case)
	if item.start == start && item.end == end {
		if s.isOldValue(&item.value, value) {
			// Ignore
			return false
		}

		// 1x1 replace
		list[offsetIndex].value = *value
		return true
	}

	if !Intersects(item.start, item.end, start, end) {
		// Insert when it does not intersect
		rangeToInsert := offsetRange{
			start: start,
			end:   end,
			value: *value,
		}

		if endIsGreater {
			list = append(list, rangeToInsert)
		} else {
			list = append(list[:offsetIndex+1], list[offsetIndex:]...)
			list[offsetIndex] = rangeToInsert
		}
		s.offsetMap[key] = list
		return true
	}

	// Occurs only when there's a change in the topology
	// Ranges may be needed to be cut, replaced with extended ones ,...
	overlapping := overlappingIndices(list, offsetIndex, start, end)
	if len(overlapping) == 1 && (start > list[overlapping[0]].start || end < list[overlapping[0]].end) {
		return s.offsetSplit(key, value, list, overlapping[0], start, end)
	}

	return s.offsetJoin(key, value, list, overlapping, start, end)
}

// Gets all overlapping ranges in ascending order
func overlappingIndices(list []offsetRange, offsetIndex int, start Token, end Token) []int {
	result := make([]int, 0)
	for i := offsetIndex; i >= 0; i-- {
		item := list[i]
		if !Intersects(item.start, item.end, start, end) {
			break
		}
		result = append([]int{i}, result...)
	}

	return result
}

func (s *defaultOffsetState) offsetJoin(
	key OffsetStoreKey,
	value *Offset,
	list []offsetRange,
	indices []int,
	start Token,
	end Token,
) bool {
	firstIndex := indices[0]
	if s.isOldValue(&list[firstIndex].value, value) {
		return false
	}

	log.Info().Msgf(
		"Joining offset for range (%d, %d), new offset v%d %d (cluster size %d)",
		start, end, value.Version, value.Offset, value.ClusterSize)

	lastIndex := indices[len(indices)-1]

	list = append(list[:firstIndex+1], list[lastIndex+1:]...)
	list[firstIndex] = offsetRange{start: start, end: end, value: *value}
	s.offsetMap[key] = list

	return true
}

func (s *defaultOffsetState) offsetSplit(
	key OffsetStoreKey,
	value *Offset,
	list []offsetRange,
	index int,
	start Token,
	end Token,
) bool {
	existing := list[index]
	// Use the timestamp as a way to check that we don't have newer data
	if s.isOldValue(&existing.value, value) {
		// Old value
		return false
	}

	// Mark the previous one as completed
	existing.value.Offset = OffsetCompleted

	// Remove the existing one and insert in place
	toInsert := make([]offsetRange, 0)

	if existing.start < start {
		// Add the portion at the beginning of the range
		toInsert = append(toInsert, offsetRange{
			start: existing.start,
			end:   start, // Until the beginning of the new portion
			value: existing.value,
		})
	}

	// Insert the new portion
	toInsert = append(toInsert, offsetRange{
		start: start,
		end:   end,
		value: *value,
	})

	if existing.end > end {
		// Add the portion at the end of the range
		toInsert = append(toInsert, offsetRange{
			start: end, // From he end of the new portion
			end:   existing.end,
			value: existing.value,
		})
	}

	log.Info().Msgf(
		"Splitting offset for existing range (%d, %d) into %d portions, new offset v%d %d (cluster size %d)",
		existing.start, existing.end, len(toInsert), value.Version, value.Offset, value.ClusterSize)

	resultList := append(list[:index], toInsert...)
	if index < len(list)-1 {
		resultList = append(resultList, list[index+1:]...)
	}

	s.offsetMap[key] = resultList

	return true
}

func (s *defaultOffsetState) processCommit() {
	for kv := range s.commitChan {
		if err := s.localDb.SaveOffset(kv); err != nil {
			log.Err(err).Interface("offset", *kv).Msgf("Offset could not be stored in the local db")
		}
	}
}

func (s *defaultOffsetState) isOldValue(existing *Offset, newValue *Offset) bool {
	if existing.Source.Id.Start == newValue.Source.Id.Start {
		// Same tokens (most common case)
		if existing.Source.Id.Version < newValue.Source.Id.Version {
			// The source of the previous value is old
			return false
		}

		if existing.Source.Id.Version > newValue.Source.Id.Version {
			// The new value's source is old
			return true
		}
	}

	if existing.Token == newValue.Token && existing.Index == newValue.Index {
		if existing.Version < newValue.Version {
			return false
		}
		if existing.Version == newValue.Version && existing.Offset <= newValue.Offset {
			return false
		}
		return true
	}

	return existing.Source.Timestamp > newValue.Source.Timestamp
}

func (s *defaultOffsetState) sendToFollowers(kv *OffsetStoreKeyValue) {
	id := GenId{Start: kv.Value.Token, Version: kv.Value.Version}
	gen := s.discoverer.GenerationInfo(id)
	if gen == nil {
		log.Error().
			Interface("id", id).
			Msgf("Generation could not be retrieved when saving offset")
		return
	}

	topology := s.discoverer.Topology()

	for _, follower := range gen.Followers {
		if follower == topology.MyOrdinal() {
			continue
		}

		ordinal := follower
		go func() {
			err := s.gossiper.SendCommittedOffset(ordinal, kv)
			if err != nil {
				if topology.HasBroker(ordinal) {
					log.Err(err).Msgf("Offset could not be sent to follower B%d", ordinal)
				}
			} else {
				log.Debug().Msgf(
					"Offset sent to follower B%d for group %s topic '%s' %d/%d",
					ordinal, kv.Key.Group, kv.Key.Topic, kv.Value.Token, kv.Value.Index)
			}
		}()
	}
}

func (s *defaultOffsetState) MaxProducedOffset(topicId *TopicDataId) (int64, error) {
	gen := s.discoverer.GenerationInfo(topicId.GenId())
	if gen == nil {
		log.Warn().Msgf("Past generation could not be retrieved %s", topicId.GenId())
		// Attempt to get the info from local storage
		return s.datalog.ReadProducerOffset(topicId)
	}

	topology := s.discoverer.Topology()
	myOrdinal := topology.MyOrdinal()
	peers := make([]int, 0)
	for _, ordinal := range gen.Followers {
		if ordinal != myOrdinal {
			peers = append(peers, ordinal)
		}
	}

	c := make(chan *int64)
	notFound := new(int64)
	*notFound = offsetNoData

	// Get the values from the peers and local storage
	go func() {
		localValue, err := s.datalog.ReadProducerOffset(topicId)
		if err != nil {
			if os.IsNotExist(err) {
				c <- notFound
			} else {
				log.Warn().Err(err).Msgf("Max producer offset could not be retrieved from local storage for %s", topicId)
				c <- nil
			}
		} else {
			c <- &localValue
		}
	}()

	for _, ordinalValue := range peers {
		// Closure will capture it
		ordinal := ordinalValue
		go func() {
			if ordinal >= len(topology.Brokers) {
				if s.gossiper.IsHostUp(ordinal) {
					// Optimistically attempt to get the info from the broker that is leaving the cluster
					value, err := s.gossiper.ReadProducerOffset(ordinal, topicId)
					if err == nil {
						c <- &value
						return
					}
				}
				c <- notFound
				return
			}
			value, err := s.gossiper.ReadProducerOffset(ordinal, topicId)
			if err != nil {
				if err == GossipGetNotFound {
					log.Debug().Msgf("Producer offset not found on peer B%d for %s", ordinal, topicId)
					c <- notFound
				} else {
					log.Warn().Err(err).Msgf("Producer offset could not be retrieved from peer B%d for %s", ordinal, topicId)
					c <- nil
				}
			} else {
				c <- &value
			}
		}()
	}

	var result *int64 = nil
	notFoundCount := 0

	for i := 0; i < len(peers)+1; i++ {
		value := <-c
		if value == notFound {
			notFoundCount++
			continue
		}
		if value != nil {
			if result == nil || *result < *value {
				result = value
			}
		}
	}

	if result == nil {
		if notFoundCount == len(peers)+1 {
			// We can safely assume that no data was produced for this token+range
			return offsetNoData, nil
		}

		message := fmt.Sprintf("Max producer offset could not be retrieved from peers or local for %s", topicId)
		log.Warn().Msgf(message)
		return 0, fmt.Errorf(message)
	}

	return *result, nil
}
