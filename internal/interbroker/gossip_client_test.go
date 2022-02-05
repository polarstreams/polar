package interbroker

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	cMocks "github.com/jorgebay/soda/internal/test/conf/mocks"
	dMocks "github.com/jorgebay/soda/internal/test/discovery/mocks"
	. "github.com/jorgebay/soda/internal/types"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Gossiper", func() {
	Describe("ReadProducerOffset", func() {
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

			const ordinal = 1
			topicId := TopicDataId{
				Name:       "abc",
				Token:      1,
				RangeIndex: 2,
				GenId:      3,
			}
			discoverer := new(dMocks.Discoverer)
			discoverer.On("Brokers").Return([]BrokerInfo{{HostName: "test-0"}, {HostName: "127.0.0.1"}})
			config := new(cMocks.Config)
			config.On("GossipPort").Return(port)

			g := gossiper{
				connections: atomic.Value{},
				discoverer:  discoverer,
				config:      config,
			}

			clients := make(clientMap)
			clients[ordinal] = &clientInfo{
				gossipClient: ts.Client(),
			}
			g.connections.Store(clients)

			value, err := g.ReadProducerOffset(ordinal, &topicId)
			Expect(err).NotTo(HaveOccurred())
			Expect(value).To(Equal(uint64(123)))
			mu.Lock()
			Expect(requestUrl).NotTo(BeNil())
			Expect(requestUrl.Path).To(Equal("/v1/producer/offset/abc/1/2/3"))
			mu.Unlock()
		})
	})
})
