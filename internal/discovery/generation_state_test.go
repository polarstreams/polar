package discovery

import (
	"fmt"
	"sync/atomic"

	. "github.com/polarstreams/polar/internal/test"
	cMocks "github.com/polarstreams/polar/internal/test/conf/mocks"
	"github.com/polarstreams/polar/internal/test/localdb/mocks"
	. "github.com/polarstreams/polar/internal/types"
	. "github.com/google/uuid"
	. "github.com/onsi/gomega"
	"github.com/stretchr/testify/mock"
)

var _ = Describe("GenerationState", func() {
	Describe("Generation()", func() {
		It("should load existing", func() {
			s := state()
			gen := Generation{
				Start: Token(123),
				End:   Token(345),
			}
			storeCommitted(s, gen)
			Expect(s.Generation(Token(123))).To(Equal(&gen))
		})

		It("should return nil when not found", func() {
			s := state()
			Expect(s.Generation(Token(123))).To(BeNil())
		})
	})

	Describe("SetGenerationProposed()", func() {
		It("should return error when previous transaction does not match", func() {
			s := state()

			gen := Generation{
				Start:  Token(123),
				End:    Token(345),
				Tx:     Must(NewRandom()),
				Status: StatusProposed,
			}
			s.genProposed[gen.Start] = gen

			tx := Must(NewRandom())
			err := s.SetGenerationProposed(&gen, nil, &tx)
			Expect(err).To(MatchError(MatchRegexp(fmt.Sprintf(
				"Existing proposed does not match.*expected %s", tx))))
		})

		It("should return error when existing transaction is nil and does not match", func() {
			s := state()

			gen := Generation{
				Start:  Token(123),
				End:    Token(345),
				Tx:     Must(NewRandom()),
				Status: StatusProposed,
			}

			tx := Must(NewRandom())
			err := s.SetGenerationProposed(&gen, nil, &tx)
			Expect(err).To(MatchError("Existing transaction is nil and expected not to be"))
		})

		It("should return error when tx match and version is not higher", func() {
			s := state()

			storeCommitted(s, Generation{
				Start:   Token(123),
				End:     Token(345),
				Version: 1,
			})

			tx := Must(NewRandom())
			existingProposed := Generation{
				Start:   Token(123),
				End:     Token(345),
				Tx:      tx,
				Version: 1,
				Status:  StatusProposed,
			}
			s.genProposed[existingProposed.Start] = existingProposed

			newGen := Generation{
				Start:   existingProposed.Start,
				End:     existingProposed.End,
				Tx:      tx,
				Version: 1,
				Status:  StatusAccepted,
			}

			err := s.SetGenerationProposed(&newGen, nil, &tx)
			Expect(err).To(MatchError(
				"Proposed version is not the next version of committed: committed = 1, proposed = 1"))
		})

		It("should replace existing when tx match and version is higher", func() {
			s := state()

			storeCommitted(s, Generation{
				Start:   Token(123),
				End:     Token(345),
				Version: 1,
			})

			tx := Must(NewRandom())
			existingProposed := Generation{
				Start:   Token(123),
				End:     Token(345),
				Tx:      tx,
				Version: 2,
				Status:  StatusProposed,
			}
			s.genProposed[existingProposed.Start] = existingProposed

			newGen := Generation{
				Start:   existingProposed.Start,
				End:     existingProposed.End,
				Tx:      tx,
				Version: 2,
				Status:  StatusAccepted,
			}

			err := s.SetGenerationProposed(&newGen, nil, &tx)
			Expect(err).NotTo(HaveOccurred())

			Expect(s.genProposed[existingProposed.Start]).To(Equal(newGen))
		})

		XIt("should replace existing generations when accepting multiple")
	})

	Describe("ParentRanges()", func() {
		config := new(cMocks.Config)
		config.On("ConsumerRanges").Return(4)
		t3 := Token(-6148914691236517888)

		It("should return the same range when single parent", func() {
			gen := &Generation{
				Start:       StartToken,
				Version:     2,
				ClusterSize: 3,
				Parents: []GenId{{
					Start:   StartToken,
					Version: 1,
				}},
			}
			parentGen := Generation{Start: gen.Parents[0].Start, Version: gen.Version - 1, ClusterSize: 3}

			dbClient := new(mocks.Client)
			dbClient.On("GenerationInfo", mock.Anything, mock.Anything).Return(&parentGen, nil)
			s := NewDiscoverer(config, dbClient).(*discoverer)

			Expect(s.ParentRanges(gen, []RangeIndex{1})).
				To(Equal([]GenerationRanges{{Generation: &parentGen, Indices: []RangeIndex{1}}}))
			Expect(s.ParentRanges(gen, []RangeIndex{3})).
				To(Equal([]GenerationRanges{{Generation: &parentGen, Indices: []RangeIndex{3}}}))
		})

		It("should project range when multiple parents when using 4 ranges", func() {
			gen := &Generation{
				Start:       StartToken,
				Version:     2,
				ClusterSize: 3,
				Parents: []GenId{{
					Start:   StartToken,
					Version: 1,
				}, {
					Start:   t3,
					Version: 1,
				}},
			}
			parent1Gen := Generation{Start: StartToken, ClusterSize: 6}
			parent2Gen := Generation{Start: t3, ClusterSize: 6}

			dbClient := new(mocks.Client)
			dbClient.On("GenerationInfo", StartToken, mock.Anything).Return(&parent1Gen, nil)
			dbClient.On("GenerationInfo", t3, mock.Anything).Return(&parent2Gen, nil)
			s := NewDiscoverer(config, dbClient).(*discoverer)

			Expect(s.ParentRanges(gen, []RangeIndex{0})).
				To(Equal([]GenerationRanges{{Generation: &parent1Gen, Indices: []RangeIndex{0, 1}}}))

			Expect(s.ParentRanges(gen, []RangeIndex{1})).
				To(Equal([]GenerationRanges{{Generation: &parent1Gen, Indices: []RangeIndex{2, 3}}}))

			// Next token
			Expect(s.ParentRanges(gen, []RangeIndex{2})).
				To(Equal([]GenerationRanges{{Generation: &parent2Gen, Indices: []RangeIndex{0, 1}}}))

			Expect(s.ParentRanges(gen, []RangeIndex{3})).
				To(Equal([]GenerationRanges{{Generation: &parent2Gen, Indices: []RangeIndex{2, 3}}}))
		})
	})

	Describe("SetAsCommitted()", func() {
		It("should store committed and delete proposed", func() {
			s := state()

			gen := Generation{
				Start:  Token(123),
				End:    Token(345),
				Tx:     Must(NewRandom()),
				Status: StatusAccepted,
			}
			s.genProposed[gen.Start] = gen

			err := s.SetAsCommitted(gen.Start, nil, gen.Tx, 3)
			Expect(err).ToNot(HaveOccurred())
			Expect(s.genProposed).To(HaveLen(0))

			committed := s.generations.Load().(genMap)
			obtained, found := committed[gen.Start]
			Expect(found).To(BeTrue())
			gen.Status = StatusCommitted
			Expect(obtained).To(Equal(gen))
		})

		It("should error when no proposed is found", func() {
			s := state()
			tx := Must(NewRandom())

			err := s.SetAsCommitted(Token(123), nil, tx, 2)
			Expect(err).To(MatchError("No proposed value found for token 123"))
		})

		It("should error when transaction does not match", func() {
			s := state()

			gen := Generation{
				Start:  Token(123),
				End:    Token(345),
				Tx:     Must(NewRandom()),
				Status: StatusAccepted,
			}
			s.genProposed[gen.Start] = gen

			err := s.SetAsCommitted(gen.Start, nil, Must(NewRandom()), 4)
			Expect(err).To(MatchError("Transaction does not match"))
		})
	})
})

func state() *discoverer {
	dbClient := new(mocks.Client)
	dbClient.On("CommitGeneration", mock.Anything, mock.Anything).Return(nil)
	return NewDiscoverer(nil, dbClient).(*discoverer)
}

func storeCommitted(s *discoverer, gen Generation) {
	m := genMap{}
	m[gen.Start] = gen
	v := atomic.Value{}
	v.Store(m)
	s.generations = v
}
