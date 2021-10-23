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
	RunSpecs(t, "Discovery Suite")
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

		It("should parse 3 real brokers", func() {
			os.Setenv(envReplicas, "3")
			d := &discoverer{
				config: &configFake{
					ordinal:      1,
					baseHostName: "soda-",
				},
			}

			d.Init()

			Expect(d.topology.Brokers).To(Equal([]BrokerInfo{
				{IsSelf: false, Ordinal: 0, HostName: "soda-0"},
				{IsSelf: true, Ordinal: 1, HostName: "soda-1"},
				{IsSelf: false, Ordinal: 2, HostName: "soda-2"},
			}))
			Expect(d.topology.LocalIndex).To(Equal(BrokerIndex(1)))
		})

		It("should parse 6 real brokers", func() {
			os.Setenv(envReplicas, "6")
			d := &discoverer{
				config: &configFake{
					ordinal:      2,
					baseHostName: "soda-",
				},
			}

			d.Init()

			Expect(d.topology.Brokers).To(Equal([]BrokerInfo{
				{IsSelf: false, Ordinal: 0, HostName: "soda-0"},
				{IsSelf: false, Ordinal: 3, HostName: "soda-3"},
				{IsSelf: false, Ordinal: 1, HostName: "soda-1"},
				{IsSelf: false, Ordinal: 4, HostName: "soda-4"},
				{IsSelf: true, Ordinal: 2, HostName: "soda-2"},
				{IsSelf: false, Ordinal: 5, HostName: "soda-5"},
			}))

			Expect(d.topology.LocalIndex).To(Equal(BrokerIndex(4)))
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

	Describe("createTopology()", func() {
		It("should return the brokers in placement order for 3 broker cluster", func() {
			config := new(mocks.Config)
			config.On("BaseHostName").Return("barco-")
			config.On("Ordinal").Return(1)

			topology := createTopology(3, config)
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

			topology := createTopology(6, config)
			Expect(topology.Brokers).To(Equal([]BrokerInfo{
				{IsSelf: false, Ordinal: 0, HostName: "broker-0"},
				{IsSelf: false, Ordinal: 3, HostName: "broker-3"},
				{IsSelf: true, Ordinal: 1, HostName: "broker-1"},
				{IsSelf: false, Ordinal: 4, HostName: "broker-4"},
				{IsSelf: false, Ordinal: 2, HostName: "broker-2"},
				{IsSelf: false, Ordinal: 5, HostName: "broker-5"},
			}))
			Expect(topology.LocalIndex).To(Equal(BrokerIndex(2)))
			Expect(topology.GetIndex(0)).To(Equal(BrokerIndex(0)))
			Expect(topology.GetIndex(3)).To(Equal(BrokerIndex(1)))
			Expect(topology.GetIndex(1)).To(Equal(BrokerIndex(2)))
			Expect(topology.GetIndex(4)).To(Equal(BrokerIndex(3)))
			Expect(topology.GetIndex(2)).To(Equal(BrokerIndex(4)))
			Expect(topology.GetIndex(5)).To(Equal(BrokerIndex(5)))

			config = new(mocks.Config)
			config.On("BaseHostName").Return("broker-")
			config.On("Ordinal").Return(2)
			topology = createTopology(6, config)
			Expect(topology.LocalIndex).To(Equal(BrokerIndex(4)))
		})

		It("should return the brokers in placement order for 12 broker cluster", func() {
			config := new(mocks.Config)
			config.On("BaseHostName").Return("broker-")
			config.On("Ordinal").Return(4)

			topology := createTopology(12, config)
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

	Describe("Leader()", func() {
		It("should default to the current token when not partition key is provided", func() {
			os.Setenv(envReplicas, "6")
			ordinal := 1
			d := NewDiscoverer(newConfigFake(ordinal), nil).(*discoverer)

			d.Init()

			existingMap := d.generations.Load().(genMap)
			existingMap[d.topology.MyToken()] = Generation{
				Start:     d.topology.MyToken(),
				End:       d.topology.GetToken(d.topology.LocalIndex + 1),
				Version:   1,
				Leader:    ordinal,
				Followers: []int{4, 2},
				Status:    StatusCommitted,
			}

			info := d.Leader("")
			Expect(info.Leader.Ordinal).To(Equal(ordinal))
			Expect(info.Token).To(Equal(d.topology.MyToken()))
			Expect(info.Followers[0].Ordinal).To(Equal(4))
			Expect(info.Followers[1].Ordinal).To(Equal(2))
		})

		It("should calculate the primary token and get the generation", func() {
			os.Setenv(envReplicas, "6")
			ordinal := 1
			d := NewDiscoverer(newConfigFake(ordinal), nil).(*discoverer)

			d.Init()

			existingMap := d.generations.Load().(genMap)
			existingMap[d.topology.GetToken(0)] = Generation{
				Start:     d.topology.GetToken(0),
				End:       d.topology.GetToken(1),
				Version:   1,
				Leader:    0,        // Ordinal of node at 0 is zero
				Followers: []int{3}, // Use a single follower as a signal that the generation was obtained
				Status:    StatusCommitted,
			}

			info := d.Leader("a") // token -8839064797231613815 , it should use the first broker
			Expect(info.Leader.Ordinal).To(Equal(0))
			Expect(info.Token).To(Equal(d.topology.GetToken(0)))
			Expect(info.Followers[0].Ordinal).To(Equal(3))
			// A single follower as a signal
			Expect(info.Followers).To(HaveLen(1))
		})

		It("should set it to the natural owner when there's no information", func() {
			os.Setenv(envReplicas, "6")
			d := NewDiscoverer(newConfigFake(1), nil).(*discoverer)

			d.Init()

			partitionKey := "hui" // token: "7851606034017063987" -> last range
			info := d.Leader(partitionKey)
			Expect(info.Leader.Ordinal).To(Equal(5))
			Expect(info.Token).To(Equal(d.topology.GetToken(BrokerIndex(5))))
			Expect(info.Followers[0].Ordinal).To(Equal(0))
			Expect(info.Followers[1].Ordinal).To(Equal(3))
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

func newConfigFake(ordinal int) *configFake {
	return &configFake{
		ordinal:      ordinal,
		baseHostName: "soda-",
	}
}
