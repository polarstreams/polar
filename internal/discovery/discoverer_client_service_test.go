package discovery

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/barcostreams/barco/internal/test/conf/mocks"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("discoverer", func() {
	Describe("Init()", func() {
		It("should start a discovery service for a 3-node cluster", func() {
			const port = 9020
			config := new(mocks.Config)
			config.On("BaseHostName").Return("barco1-")
			config.On("ServiceName").Return("svc")
			config.On("Ordinal").Return(1)
			config.On("DevMode").Return(false)
			config.On("ListenOnAllAddresses").Return(true)
			config.On("ClientDiscoveryPort").Return(port)
			config.On("ProducerPort").Return(8901)
			config.On("ConsumerPort").Return(8902)

			d := &discoverer{
				config:    config,
				k8sClient: &k8sClientFake{3},
				localDb:   newLocalDbWithNoRecords(),
			}

			defer d.Close()
			err := d.Init()
			Expect(err).NotTo(HaveOccurred())

			// Wait for a little bit
			time.Sleep(100 * time.Millisecond)

			client := &http.Client{}
			r, err := client.Get(fmt.Sprintf("http://127.0.0.1:%d/status", port))
			Expect(err).NotTo(HaveOccurred())
			Expect(r.StatusCode).To(Equal(http.StatusOK))
			r.Body.Close()

			r, err = client.Get(fmt.Sprintf("http://127.0.0.1:%d/v1/brokers", port))
			Expect(err).NotTo(HaveOccurred())
			Expect(r.StatusCode).To(Equal(http.StatusOK))
			defer r.Body.Close()
			var result topologyClientMessage
			err = json.NewDecoder(r.Body).Decode(&result)
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(topologyClientMessage{
				Length:       3,
				BrokerNames:  []string{"barco1-0.svc", "barco1-1.svc", "barco1-2.svc"},
				ProducerPort: 8901,
				ConsumerPort: 8902,
			}))
		})

		It("should start a discovery service for a 6-node cluster", func() {
			const port = 9021
			config := new(mocks.Config)
			config.On("BaseHostName").Return("barco2-")
			config.On("ServiceName").Return("svc")
			config.On("Ordinal").Return(1)
			config.On("DevMode").Return(false)
			config.On("ListenOnAllAddresses").Return(true)
			config.On("ClientDiscoveryPort").Return(port)
			config.On("ProducerPort").Return(8901)
			config.On("ConsumerPort").Return(8902)

			d := &discoverer{
				config:    config,
				k8sClient: &k8sClientFake{6},
				localDb:   newLocalDbWithNoRecords(),
			}

			defer d.Close()
			err := d.Init()
			Expect(err).NotTo(HaveOccurred())

			// Wait for a little bit
			time.Sleep(100 * time.Millisecond)

			client := &http.Client{}
			r, err := client.Get(fmt.Sprintf("http://127.0.0.1:%d/status", port))
			Expect(err).NotTo(HaveOccurred())
			Expect(r.StatusCode).To(Equal(http.StatusOK))
			r.Body.Close()

			r, err = client.Get(fmt.Sprintf("http://127.0.0.1:%d/v1/brokers", port))
			Expect(err).NotTo(HaveOccurred())
			Expect(r.StatusCode).To(Equal(http.StatusOK))
			defer r.Body.Close()
			var result topologyClientMessage
			err = json.NewDecoder(r.Body).Decode(&result)
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(topologyClientMessage{
				BaseName:     "barco2-",
				ServiceName:  "svc",
				Length:       6,
				ProducerPort: 8901,
				ConsumerPort: 8902,
			}))
		})
	})
})
