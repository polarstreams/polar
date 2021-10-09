package discovery

import (
	"os"
	"testing"

	"github.com/jorgebay/soda/internal/types"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func Test(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Discoverer Suite")
}

var _ = Describe("discoverer", func() {
	AfterEach(func() {
		os.Setenv(envReplicas, "")
		os.Setenv(envBrokerNames, "")
		os.Setenv(envOrdinal, "")
	})

	Describe("Init()", func() {
		It("should parse fixed brokers", func() {
			os.Setenv(envOrdinal, "2")
			os.Setenv(envBrokerNames, "abc,def,ghi")
			d := &discoverer{}

			d.Init()

			Expect(d.brokers).To(Equal([]types.BrokerInfo{
				{
					IsSelf:   false,
					Ordinal:  0,
					HostName: "abc",
				}, {
					IsSelf:   false,
					Ordinal:  1,
					HostName: "def",
				}, {
					IsSelf:   true,
					Ordinal:  2,
					HostName: "ghi",
				},
			}))
		})

		It("should parse real brokers", func() {
			os.Setenv(envReplicas, "3")
			d := &discoverer{
				config: &configFake{
					ordinal:      1,
					baseHostName: "soda-",
				},
			}

			d.Init()

			Expect(d.brokers).To(Equal([]types.BrokerInfo{
				{
					IsSelf:   false,
					Ordinal:  0,
					HostName: "soda-0",
				}, {
					IsSelf:   true,
					Ordinal:  1,
					HostName: "soda-1",
				}, {
					IsSelf:   false,
					Ordinal:  2,
					HostName: "soda-2",
				},
			}))
		})

		It("should create the token ring", func() {
			os.Setenv(envReplicas, "3")
			d := &discoverer{
				config: &configFake{
					baseHostName: "soda-",
				},
			}

			d.Init()

			// 3 parts of around 6148914691236517205 length each (math.MaxUint64 / 3).
			// Rounded to avoid token movement on resizing.
			Expect(d.ring).To(Equal([]types.Token{-9223372036854775808, -3074457345618259968, 3074457345618255872}))
		})
	})

	Describe("Peers()", func() {
		It("should return all brokers except self", func() {
			os.Setenv(envReplicas, "3")
			d := &discoverer{
				config: &configFake{
					ordinal:      1,
					baseHostName: "soda-",
				},
			}

			d.Init()

			Expect(d.Peers()).To(Equal([]types.BrokerInfo{
				{
					IsSelf:   false,
					Ordinal:  0,
					HostName: "soda-0",
				}, {
					IsSelf:   false,
					Ordinal:  2,
					HostName: "soda-2",
				},
			}))
		})
	})
})

type configFake struct {
	ordinal      int
	baseHostName string
}

func (c *configFake) Ordinal() int {
	return c.ordinal
}

func (c *configFake) BaseHostName() string {
	return c.baseHostName
}
