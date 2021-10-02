package localdb

import (
	"io/ioutil"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
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

			result, err := client.GetGenerationsByToken(1000)
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(BeEmpty())
		})

		It("Should return the last 2 generations", func() {
			start := Token(1001)
			end := Token(2001)
			client := newTestClient()
			tx := uuid.New()

			// Insert test data
			for i := 1; i <= 10; i++ {
				insertGeneration(client, Generation{
					Start:     start,
					End:       end,
					Version:   i,
					Timestamp: utils.ToUnixMillis(time.Now()),
					Tx:        tx,
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
				Expect(result[i].Tx).To(Equal(tx))
				Expect(result[i].Timestamp).To(BeNumerically(">", 1624977183000))
				Expect(result[i].Status).To(Equal(StatusAccepted))
				Expect(result[i].Leader).To(Equal(2))
				Expect(result[i].Followers).To(Equal([]int{0, 1}))
			}
		})
	})

	Describe("UpsertGeneration()", func() {
		It("Should insert generation when existing is not provided", func() {
			client := newTestClient()

			gen := Generation{
				Start:     1010,
				End:       2010,
				Version:   1,
				Tx:        uuid.New(),
				Timestamp: utils.ToUnixMillis(time.Now()),
				Status:    StatusProposed,
				Leader:    0,
				Followers: []int{1, 2},
			}
			err := client.UpsertGeneration(nil, &gen)
			Expect(err).NotTo(HaveOccurred())

			obtained, err := client.GetGenerationsByToken(gen.Start)
			Expect(err).NotTo(HaveOccurred())
			Expect(gen).To(Equal(obtained[0]))
		})

		It("Should update when existing is provided", func() {
			client := newTestClient()

			gen := Generation{
				Start:     1021,
				End:       2021,
				Version:   2,
				Tx:        uuid.New(),
				Timestamp: utils.ToUnixMillis(time.Now()),
				Status:    StatusProposed,
				Leader:    0,
				Followers: []int{1, 2},
			}

			insertGeneration(client, gen)

			newGen := gen
			newGen.Leader = 1
			newGen.Followers = []int{2, 0}
			newGen.Tx = uuid.New()

			err := client.UpsertGeneration(&gen, &newGen)
			Expect(err).NotTo(HaveOccurred())

			obtained, err := client.GetGenerationsByToken(gen.Start)
			Expect(err).NotTo(HaveOccurred())
			Expect(newGen).To(Equal(obtained[0]))
		})

		It("Should return an error when no row is affected", func() {
			client := newTestClient()

			gen := Generation{
				Start:     1030,
				End:       2030,
				Version:   1,
				Tx:        uuid.New(),
				Status:    StatusProposed,
				Timestamp: utils.ToUnixMillis(time.Now()),
				Leader:    0,
				Followers: []int{1, 2},
			}

			err := client.UpsertGeneration(&gen, &gen)
			Expect(err).To(MatchError("No generation was updated"))
		})
	})

	Describe("SetAsAccepted()", func() {
		It("Should update the status to accepted when's a match", func() {
			client := newTestClient()

			gen := Generation{
				Start:     1040,
				End:       2040,
				Version:   2,
				Tx:        uuid.New(),
				Status:    StatusProposed,
				Timestamp: utils.ToUnixMillis(time.Now()),
				Leader:    2,
				Followers: []int{0, 1},
			}

			insertGeneration(client, gen)

			gen.Status = StatusAccepted

			err := client.SetGenerationAsAccepted(&gen)
			Expect(err).ToNot(HaveOccurred())

			obtained, err := client.GetGenerationsByToken(gen.Start)
			Expect(err).NotTo(HaveOccurred())
			Expect(gen).To(Equal(obtained[0]))
		})

		It("Should error when no row was affected", func() {
			client := newTestClient()
			gen := Generation{
				Start:     1050, // Does not exist
				End:       2050,
				Version:   2,
				Tx:        uuid.New(),
				Status:    StatusAccepted,
				Timestamp: utils.ToUnixMillis(time.Now()),
				Leader:    2,
				Followers: []int{0, 1},
			}

			err := client.SetGenerationAsAccepted(&gen)
			Expect(err).To(MatchError("No generation was updated"))
		})
	})
})

func newTestClient() *client {
	client := NewClient(&testConfig{}).(*client)
	err := client.Init()
	Expect(err).NotTo(HaveOccurred())
	return client
}

type testConfig struct{}

func (c *testConfig) LocalDbPath() string {
	dir, err := ioutil.TempDir("", "example")
	panicIfError(err)
	return filepath.Join(dir, "local.db")
}

func insertGeneration(c *client, gen Generation) {
	query := `
		INSERT INTO generations (start_token, end_token, version, timestamp, tx, status, leader, followers)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`
	_, err := c.db.Exec(
		query, gen.Start, gen.End, gen.Version, gen.Timestamp, gen.Tx,
		gen.Status, gen.Leader, utils.ToCsv(gen.Followers))
	Expect(err).NotTo(HaveOccurred())
}
