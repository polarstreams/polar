package localdb

import (
	"io/ioutil"
	"path/filepath"
	"testing"

	"github.com/jorgebay/soda/internal/types"
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

			result, err := client.GetGenerationByToken(1000)
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(BeEmpty())
		})

		It("Should return the last 2 generations", func() {
			token := int64(1001)
			client := newTestClient()
			err := client.Init()
			Expect(err).NotTo(HaveOccurred())

			// Insert test data
			for i := 1; i <= 10; i++ {
				query := "INSERT INTO generations (token, version, tx, status, leader, followers) VALUES (?, ?, ?, ?, ?, ?)"
				_, err := client.db.Exec(query, token, i, []byte{0, 1}, 1, 2, "0,1")
				Expect(err).NotTo(HaveOccurred())
			}

			result, err := client.GetGenerationByToken(types.Token(token))
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(HaveLen(2))
			Expect([]int{result[0].Version, result[1].Version}).To(Equal([]int{10, 9}))
			for i := 0; i < 2; i++ {
				Expect(result[i].Tx).To(Equal([]byte{0, 1}))
				Expect(result[i].Status).To(Equal(1))
				Expect(result[i].Leader).To(Equal(2))
				Expect(result[i].Followers).To(Equal([]int{0, 1}))
			}
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
