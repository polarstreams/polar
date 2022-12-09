package interbroker

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	cMocks "github.com/polarstreams/polar/internal/test/conf/mocks"
	dMocks "github.com/polarstreams/polar/internal/test/discovery/mocks"
	. "github.com/polarstreams/polar/internal/types"
	"github.com/stretchr/testify/mock"
)

var _ = Describe("Gossiper", func() {
	Describe("ReadProducerOffset()", func() {
		It("Should call the server", func() {
			var mu sync.Mutex
			var requestUrl *url.URL
			ts := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				requestUrl = r.URL
				defer mu.Unlock()
				fmt.Fprintln(w, "123")
			}))
			ts.EnableHTTP2 = true
			ts.Start()
			defer ts.Close()

			port, err := strconv.Atoi(strings.Split(ts.URL, ":")[2])
			Expect(err).NotTo(HaveOccurred())

			const ordinal = 0
			topicId := TopicDataId{
				Name:       "abc",
				Token:      1,
				RangeIndex: 2,
				Version:    3,
			}

			config := new(cMocks.Config)
			config.On("GossipPort").Return(port)

			g := &gossiper{
				discoverer: newDiscovererForGossipClient(),
				config:     config,
			}
			setTestGossipClient(g, ordinal, ts)

			value, err := g.ReadProducerOffset(ordinal, &topicId)
			Expect(err).NotTo(HaveOccurred())
			Expect(value).To(Equal(int64(123)))
			mu.Lock()
			Expect(requestUrl).NotTo(BeNil())
			Expect(requestUrl.Path).To(Equal("/v1/producer/offset/abc/1/2/3"))
			mu.Unlock()
		})
	})

	Describe("SendConsumerCommit()", func() {
		It("should call the server", func() {
			var mu sync.Mutex
			var requestUrl *url.URL
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				requestUrl = r.URL
				defer mu.Unlock()
				fmt.Fprintln(w, "OK")
			}))
			defer ts.Close()

			const ordinal = 1
			port, _ := strconv.Atoi(strings.Split(ts.URL, ":")[2])
			config := new(cMocks.Config)
			config.On("GossipPort").Return(port)
			g := &gossiper{
				discoverer: newDiscovererForGossipClient(),
				config:     config,
			}
			setTestGossipClient(g, ordinal, ts)

			err := g.SendConsumerCommit(ordinal, "abc123")
			Expect(err).NotTo(HaveOccurred())
			mu.Lock()
			Expect(requestUrl.Path).To(Equal("/v1/consumer/commit/abc123"))
			mu.Unlock()
		})

		It("should succeed with any 2xx status", func() {
			// Status that are not success
			c := make(chan int, 100)
			c <- http.StatusNoContent
			c <- http.StatusAccepted

			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(<-c)
			}))
			defer ts.Close()

			const ordinal = 1
			port, _ := strconv.Atoi(strings.Split(ts.URL, ":")[2])
			config := new(cMocks.Config)
			config.On("GossipPort").Return(port)
			g := &gossiper{
				discoverer: newDiscovererForGossipClient(),
				config:     config,
			}
			setTestGossipClient(g, ordinal, ts)

			Expect(g.SendConsumerCommit(ordinal, "abc123")).NotTo(HaveOccurred())
			Expect(g.SendConsumerCommit(ordinal, "abc123")).NotTo(HaveOccurred())
		})

		It("should not ignore other error status", func() {
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusBadRequest)
			}))
			defer ts.Close()

			const ordinal = 1
			port, _ := strconv.Atoi(strings.Split(ts.URL, ":")[2])
			config := new(cMocks.Config)
			config.On("GossipPort").Return(port)
			g := &gossiper{
				discoverer: newDiscovererForGossipClient(),
				config:     config,
			}
			setTestGossipClient(g, ordinal, ts)

			err := g.SendConsumerCommit(ordinal, "abc123")
			Expect(err).To(Equal(NewHttpError(http.StatusBadRequest, "400 Bad Request")))
		})
	})
})

func newTestTopology(length int, ordinal int) *TopologyInfo {
	brokers := make([]BrokerInfo, length)
	for i := 0; i < length; i++ {
		brokers[i] = BrokerInfo{
			IsSelf:   i == ordinal,
			Ordinal:  i,
			HostName: fmt.Sprintf("127.0.0.%d", i+1),
		}
	}

	t := NewTopology(brokers, ordinal)
	return &t
}

func setTestGossipClient(g *gossiper, ordinal int, ts *httptest.Server) {
	clients := make(clientMap)
	clients[ordinal] = &clientInfo{
		gossipClient: ts.Client(),
		isConnected:  1,
	}
	g.connections.Store(clients)
}

func newDiscovererForGossipClient() *dMocks.Discoverer {
	discoverer := new(dMocks.Discoverer)
	discoverer.On("CurrentOrPastBroker", mock.Anything).Return(&BrokerInfo{HostName: "127.0.0.1"})
	return discoverer
}
