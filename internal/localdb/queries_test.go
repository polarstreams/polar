package localdb

import (
	"io/ioutil"
	"path/filepath"
	"testing"
	"time"

	. "github.com/barcostreams/barco/internal/types"
	"github.com/barcostreams/barco/internal/utils"
	"github.com/google/uuid"
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
					Version:   GenVersion(i),
					Timestamp: utils.ToUnixMillis(time.Now()),
					Tx:        tx,
					Status:    StatusAccepted,
					Leader:    2,
					Followers: []int{0, 1},
					Parents:   []GenParent{{Start: start, Version: GenVersion(i - 1)}},
				})
			}

			result, err := client.GetGenerationsByToken(Token(start))
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(HaveLen(2))
			Expect([]GenVersion{result[0].Version, result[1].Version}).To(Equal([]GenVersion{10, 9}))
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

	Describe("GenerationInfo()", func() {
		It("Should retrieve an empty generations when no info is found", func() {
			client := newTestClient()

			result, err := client.GenerationInfo(1100, 80000000)
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(BeNil())
		})

		It("Should return the last 2 generations", func() {
			start := Token(1101)
			end := Token(2101)
			client := newTestClient()
			tx := uuid.New()

			// Insert test data
			inserted := make([]Generation, 0)
			for i := 1; i <= 3; i++ {
				item := Generation{
					Start:     start,
					End:       end,
					Version:   GenVersion(i),
					Timestamp: utils.ToUnixMillis(time.Now()),
					Tx:        tx,
					Status:    StatusCommitted,
					Leader:    2,
					Followers: []int{0, 1},
					Parents:   []GenParent{{Start: start, Version: GenVersion(i - 1)}},
				}
				insertGeneration(client, item)
				inserted = append(inserted, item)
			}

			result, err := client.GenerationInfo(start, 2)
			Expect(err).NotTo(HaveOccurred())
			Expect(result).NotTo(BeNil())
			Expect(*result).To(Equal(inserted[1]))
		})
	})

	Describe("LatestGenerations()", func() {
		It("Should return latest generation per token", func() {
			client := newTestClient()
			defer client.Close()

			gen1_v3 := Generation{
				Start:     1,
				End:       2,
				Version:   3,
				Timestamp: utils.ToUnixMillis(time.Now()),
				Tx:        uuid.New(),
				Status:    StatusCommitted,
				Leader:    2,
				Followers: []int{0, 1},
				Parents:   []GenParent{{Start: 1, Version: 2}},
			}
			gen1_v4 := Generation{
				Start:     1,
				End:       2,
				Version:   4,
				Timestamp: utils.ToUnixMillis(time.Now()),
				Tx:        uuid.New(),
				Status:    StatusCommitted,
				Leader:    2,
				Followers: []int{0, 1},
				Parents:   []GenParent{{Start: 1, Version: 3}},
			}
			gen2_v1 := Generation{
				Start:     2,
				End:       3,
				Version:   1,
				Timestamp: utils.ToUnixMillis(time.Now()),
				Tx:        uuid.New(),
				Status:    StatusCommitted,
				Leader:    2,
				Followers: []int{0, 1},
				Parents:   []GenParent{{Start: 1, Version: 3}},
			}
			insertGeneration(client, gen1_v3)
			insertGeneration(client, gen1_v4)
			insertGeneration(client, gen2_v1)

			result, err := client.LatestGenerations()
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(HaveLen(2))
			Expect(result[0]).To(Equal(gen1_v4))
			Expect(result[1]).To(Equal(gen2_v1))
		})
	})

	Describe("GenerationsByParent()", func() {
		It("Should return the next generations", func() {
			client := newTestClient()
			defer client.Close()

			gen1_v3 := Generation{
				Start:     1,
				End:       2,
				Version:   3,
				Timestamp: utils.ToUnixMillis(time.Now()),
				Tx:        uuid.New(),
				Status:    StatusCommitted,
				Leader:    2,
				Followers: []int{0, 1},
				Parents:   []GenParent{{Start: 1, Version: 2}},
			}
			gen1_v4 := Generation{
				Start:     1,
				End:       2,
				Version:   4,
				Timestamp: utils.ToUnixMillis(time.Now()),
				Tx:        uuid.New(),
				Status:    StatusCommitted,
				Leader:    2,
				Followers: []int{0, 1},
				Parents:   []GenParent{{Start: 1, Version: 3}},
			}
			gen2_v1 := Generation{
				Start:     2,
				End:       3,
				Version:   1,
				Timestamp: utils.ToUnixMillis(time.Now()),
				Tx:        uuid.New(),
				Status:    StatusCommitted,
				Leader:    2,
				Followers: []int{0, 1},
				Parents:   []GenParent{{Start: 1, Version: 3}},
			}
			insertGeneration(client, gen1_v3)
			insertGeneration(client, gen1_v4)
			insertGeneration(client, gen2_v1)

			result, err := client.GenerationsByParent(&gen1_v3)
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal([]Generation{gen1_v4}))

			result, err = client.GenerationsByParent(&gen2_v1)
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(HaveLen(0))
		})
	})

	Describe("CommitGeneration()", func() {
		It("should insert a record in each table", func() {
			client := newTestClient()

			gen := Generation{
				Start:     2001,
				End:       3001,
				Version:   123,
				Timestamp: time.Now().UnixMicro(),
				Leader:    3,
				Followers: []int{1, 4},
				TxLeader:  3,
				Tx:        uuid.New(),
				Status:    StatusCommitted,
				Parents:   []GenParent{{Start: 2001, Version: GenVersion(122)}},
			}

			err := client.CommitGeneration(&gen, nil)
			Expect(err).NotTo(HaveOccurred())
			expectToMatchStored(client, gen)
			expectTransactionStored(client, gen)
		})

		It("should one record per each generation plus the tx in each table", func() {
			client := newTestClient()

			tx := uuid.New()
			gen1 := Generation{
				Start:     2001,
				End:       3001,
				Version:   123,
				Timestamp: time.Now().UnixMicro(),
				Leader:    0,
				Followers: []int{3, 1},
				TxLeader:  0,
				Tx:        tx,
				Status:    StatusCommitted,
				Parents:   []GenParent{{Start: 2001, Version: GenVersion(122)}},
			}

			gen2 := Generation{
				Start:     3001,
				End:       4001,
				Version:   1,
				Timestamp: time.Now().UnixMicro(),
				Leader:    3,
				Followers: []int{1, 4},
				TxLeader:  0,
				Tx:        tx,
				Status:    StatusCommitted,
				Parents:   []GenParent{{Start: 2001, Version: GenVersion(122)}},
			}

			err := client.CommitGeneration(&gen1, &gen2)
			Expect(err).NotTo(HaveOccurred())
			expectToMatchStored(client, gen1)
			expectToMatchStored(client, gen2)
			expectTransactionStored(client, gen1)
		})
	})

	Describe("SaveOffset()", func() {
		It("should insert a record in offsets table", func() {
			client := newTestClient()
			key := OffsetStoreKey{
				Group:      "group1",
				Topic:      "topic1",
				Token:      -123,
				RangeIndex: 7,
			}
			value := Offset{
				Offset:  1001,
				Version: 3,
				Source:  4,
			}
			kv := OffsetStoreKeyValue{
				Key:   key,
				Value: value,
			}
			err := client.SaveOffset(&kv)
			Expect(err).NotTo(HaveOccurred())

			// Verify stored
			query := `
				SELECT version, offset, source FROM offsets
				WHERE group_name = ? AND topic = ? AND token = ? AND range_index = ?`
			obtained := Offset{}
			err = client.db.
				QueryRow(query, key.Group, key.Topic, key.Token, key.RangeIndex).
				Scan(&obtained.Version, &obtained.Offset, &obtained.Source)
			Expect(err).NotTo(HaveOccurred())
			Expect(obtained).To(Equal(value))

			// Quick test that can be upserted multiple times
			Expect(client.SaveOffset(&kv)).NotTo(HaveOccurred())

			// Quick test Offsets() method
			offsets, err := client.Offsets()
			Expect(err).NotTo(HaveOccurred())
			Expect(offsets).To(ContainElement(kv))
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
		INSERT INTO generations
		(start_token, end_token, version, timestamp, tx, tx_leader, status, leader, followers, parents)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
	_, err := c.db.Exec(
		query, gen.Start, gen.End, gen.Version, gen.Timestamp, gen.Tx, gen.TxLeader,
		gen.Status, gen.Leader, utils.ToCsv(gen.Followers), parentsToString(gen.Parents))
	Expect(err).NotTo(HaveOccurred())
}

func expectTransactionStored(c *client, gen Generation) {
	query := `SELECT tx, origin, timestamp, status FROM transactions WHERE tx = ?`
	obtained := Generation{}
	err := c.db.QueryRow(query, gen.Tx).Scan(&obtained.Tx, &obtained.TxLeader, &obtained.Timestamp, &obtained.Status)
	Expect(err).NotTo(HaveOccurred())
	Expect(obtained.Tx).To(Equal(gen.Tx))
	Expect(obtained.TxLeader).To(Equal(gen.TxLeader))
	Expect(obtained.Status).To(Equal(gen.Status))
}

func expectToMatchStored(c *client, gen Generation) {
	result, err := c.GetGenerationsByToken(gen.Start)
	Expect(err).NotTo(HaveOccurred())
	Expect(result).To(HaveLen(1))
	Expect(result[0]).To(Equal(gen))
}
