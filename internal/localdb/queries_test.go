package localdb

import (
	"io/ioutil"
	"path/filepath"
	"testing"

	. "github.com/jorgebay/soda/internal/types"
	"github.com/jorgebay/soda/internal/utils"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestCompression(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "LocalDB Suite")
}

var _ = Describe("Client", func() {
	Describe("GetGenerationPerToken()", func() {
		It("Should retrieve an empty generations when no info is found", func() {
			client := newTestClient()
			err := client.Init()
			Expect(err).NotTo(HaveOccurred())

			result, err := client.GetGenerationsByToken(1000)
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(BeEmpty())
		})

		It("Should return the last 2 generations", func() {
			start := Token(1001)
			end := Token(2001)
			client := newTestClient()
			err := client.Init()
			Expect(err).NotTo(HaveOccurred())

			// Insert test data
			for i := 1; i <= 10; i++ {
				insertGeneration(client, Generation{
					Start:     start,
					End:       end,
					Version:   i,
					Tx:        []byte{0, 1},
					Status:    StatusAccepted,
					Leader:    2,
					Followers: []int{0, 1},
				})
			}

			result, err := client.GetGenerationsByToken(Token(start))
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(HaveLen(2))
			Expect([]int{result[0].Version, result[1].Version}).To(Equal([]int{10, 9}))
			for i := 0; i < 2; i++ {
				Expect(result[i].Start).To(Equal(Token(start)))
				Expect(result[i].End).To(Equal(Token(end)))
				Expect(result[i].Tx).To(Equal([]byte{0, 1}))
				Expect(result[i].Status).To(Equal(StatusAccepted))
				Expect(result[i].Leader).To(Equal(2))
				Expect(result[i].Followers).To(Equal([]int{0, 1}))
			}
		})
	})

	Describe("UpsertGeneration()", func() {
		It("Should insert generation when existing is not provided", func() {
			client := newTestClient()
			err := client.Init()
			Expect(err).NotTo(HaveOccurred())

			gen := Generation{
				Start:     1010,
				End:       2010,
				Version:   1,
				Tx:        []byte{0, 1, 2, 3},
				Status:    StatusProposed,
				Leader:    0,
				Followers: []int{1, 2},
			}
			err = client.UpsertGeneration(gen.Start, nil, &gen)
			Expect(err).NotTo(HaveOccurred())

			obtained, err := client.GetGenerationsByToken(gen.Start)
			Expect(err).NotTo(HaveOccurred())
			Expect(gen).To(Equal(obtained[0]))
		})

		It("Should update when existing is provided", func() {
			client := newTestClient()
			err := client.Init()
			Expect(err).NotTo(HaveOccurred())

			gen := Generation{
				Start:     1021,
				End:       2021,
				Version:   2,
				Tx:        []byte{0, 1, 2, 3},
				Status:    StatusProposed,
				Leader:    0,
				Followers: []int{1, 2},
			}

			insertGeneration(client, gen)

			newGen := gen
			newGen.Leader = 1
			newGen.Followers = []int{2, 0}
			newGen.Tx = []byte{0, 1, 2, 3}

			err = client.UpsertGeneration(Token(0), &gen, &newGen)
			Expect(err).NotTo(HaveOccurred())

			obtained, err := client.GetGenerationsByToken(gen.Start)
			Expect(err).NotTo(HaveOccurred())
			Expect(newGen).To(Equal(obtained[0]))
		})

		It("Should return an error when no row is affected", func() {
			client := newTestClient()
			err := client.Init()
			Expect(err).NotTo(HaveOccurred())

			gen := Generation{
				Start:     1030,
				End:       2030,
				Version:   1,
				Tx:        []byte{0, 1, 2, 3},
				Status:    StatusProposed,
				Leader:    0,
				Followers: []int{1, 2},
			}

			err = client.UpsertGeneration(Token(0), &gen, &gen)
			Expect(err).To(MatchError("No generation was updated"))
		})
	})
})

func newTestClient() *client {
	return NewClient(&testConfig{}).(*client)
}

type testConfig struct{}

func (c *testConfig) LocalDbPath() string {
	dir, err := ioutil.TempDir("", "example")
	panicIfError(err)
	return filepath.Join(dir, "local.db")
}

func insertGeneration(c *client, gen Generation) {
	query := `
		INSERT INTO generations (start_token, end_token, version, tx, status, leader, followers)
		 VALUES (?, ?, ?, ?, ?, ?, ?)`
	_, err := c.db.Exec(
		query, gen.Start, gen.End, gen.Version, gen.Tx, gen.Status, gen.Leader, utils.ToCsv(gen.Followers))
	Expect(err).NotTo(HaveOccurred())
}
