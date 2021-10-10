package discovery

import (
	"os"
	"testing"

	"github.com/jorgebay/soda/internal/test/conf/mocks"
	. "github.com/jorgebay/soda/internal/types"
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

			Expect(d.topology.Brokers).To(Equal([]BrokerInfo{
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

			Expect(d.topology.Brokers).To(Equal([]BrokerInfo{
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

			Expect(d.Peers()).To(Equal([]BrokerInfo{
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

	Describe("brokersOrdered()", func() {
		It("should return the brokers in placement order for 3 broker cluster", func() {
			config := new(mocks.Config)
			config.On("BaseHostName").Return("barco-")
			config.On("Ordinal").Return(1)

			topology := brokersOrdered(3, config)
			Expect(topology.Brokers).To(Equal([]BrokerInfo{
				{IsSelf: false, Ordinal: 0, HostName: "barco-0"},
				{IsSelf: true, Ordinal: 1, HostName: "barco-1"},
				{IsSelf: false, Ordinal: 2, HostName: "barco-2"},
			}))
			Expect(topology.LocalIndex).To(Equal(BrokerIndex(1)))
		})

		It("should return the brokers in placement order for 6 broker cluster", func() {
			config := new(mocks.Config)
			config.On("BaseHostName").Return("broker-")
			config.On("Ordinal").Return(1)

			topology := brokersOrdered(6, config)
			Expect(topology.Brokers).To(Equal([]BrokerInfo{
				{IsSelf: false, Ordinal: 0, HostName: "broker-0"},
				{IsSelf: false, Ordinal: 3, HostName: "broker-3"},
				{IsSelf: true, Ordinal: 1, HostName: "broker-1"},
				{IsSelf: false, Ordinal: 4, HostName: "broker-4"},
				{IsSelf: false, Ordinal: 2, HostName: "broker-2"},
				{IsSelf: false, Ordinal: 5, HostName: "broker-5"},
			}))
			Expect(topology.LocalIndex).To(Equal(BrokerIndex(2)))

			config = new(mocks.Config)
			config.On("BaseHostName").Return("broker-")
			config.On("Ordinal").Return(2)
			topology = brokersOrdered(6, config)
			Expect(topology.LocalIndex).To(Equal(BrokerIndex(4)))
		})

		It("should return the brokers in placement order for 12 broker cluster", func() {
			config := new(mocks.Config)
			config.On("BaseHostName").Return("broker-")
			config.On("Ordinal").Return(4)

			topology := brokersOrdered(12, config)
			Expect(topology.Brokers).To(Equal([]BrokerInfo{
				{IsSelf: false, Ordinal: 0, HostName: "broker-0"},
				{IsSelf: false, Ordinal: 6, HostName: "broker-6"},
				{IsSelf: false, Ordinal: 3, HostName: "broker-3"},
				{IsSelf: false, Ordinal: 7, HostName: "broker-7"},
				{IsSelf: false, Ordinal: 1, HostName: "broker-1"},
				{IsSelf: false, Ordinal: 8, HostName: "broker-8"},
				{IsSelf: true, Ordinal: 4, HostName: "broker-4"},
				{IsSelf: false, Ordinal: 9, HostName: "broker-9"},
				{IsSelf: false, Ordinal: 2, HostName: "broker-2"},
				{IsSelf: false, Ordinal: 10, HostName: "broker-10"},
				{IsSelf: false, Ordinal: 5, HostName: "broker-5"},
				{IsSelf: false, Ordinal: 11, HostName: "broker-11"},
			}))
			Expect(topology.LocalIndex).To(Equal(BrokerIndex(6)))
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
