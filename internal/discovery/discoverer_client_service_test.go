package discovery

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/polarstreams/polar/internal/test/conf/mocks"
	"github.com/polarstreams/polar/internal/utils"
)

var _ = Describe("discoverer", func() {
	Describe("Init()", func() {
		It("should start a discovery service for a 3-node cluster", func() {
			const port = 9020
			config := new(mocks.Config)
			config.On("BaseHostName").Return("polar1-")
			config.On("ServiceName").Return("svc")
			config.On("PodNamespace").Return("streams")
			config.On("Ordinal").Return(1)
			config.On("DevMode").Return(false)
			config.On("ListenOnAllAddresses").Return(true)
			config.On("ClientDiscoveryPort").Return(port)
			config.On("ProducerPort").Return(8901)
			config.On("ProducerBinaryPort").Return(8904)
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
			body, _ := utils.ReadBodyClose(r)
			Expect(body).To(Equal(noGenerationsStatusMessage))

			r, err = client.Get(fmt.Sprintf("http://127.0.0.1:%d/v1/brokers", port))
			Expect(err).NotTo(HaveOccurred())
			Expect(r.StatusCode).To(Equal(http.StatusOK))
			defer r.Body.Close()
			var result topologyClientMessage
			err = json.NewDecoder(r.Body).Decode(&result)
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(topologyClientMessage{
				Length:             3,
				BrokerNames:        []string{"polar1-0.svc.streams", "polar1-1.svc.streams", "polar1-2.svc.streams"},
				ProducerPort:       8901,
				ProducerBinaryPort: 8904,
				ConsumerPort:       8902,
			}))
		})

		It("should start a discovery service for a 6-node cluster", func() {
			const port = 9021
			config := new(mocks.Config)
			config.On("BaseHostName").Return("polarsample-")
			config.On("ServiceName").Return("svc2")
			config.On("PodNamespace").Return("streams2")
			config.On("Ordinal").Return(1)
			config.On("DevMode").Return(false)
			config.On("ListenOnAllAddresses").Return(true)
			config.On("ClientDiscoveryPort").Return(port)
			config.On("ProducerPort").Return(8901)
			config.On("ProducerBinaryPort").Return(8904)
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
			body, _ := utils.ReadBodyClose(r)
			Expect(body).To(Equal(noGenerationsStatusMessage))

			r, err = client.Get(fmt.Sprintf("http://127.0.0.1:%d/v1/brokers", port))
			Expect(err).NotTo(HaveOccurred())
			Expect(r.StatusCode).To(Equal(http.StatusOK))
			defer r.Body.Close()
			var result topologyClientMessage
			err = json.NewDecoder(r.Body).Decode(&result)
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(topologyClientMessage{
				BaseName:           "polarsample-",
				ServiceName:        "svc2.streams2",
				Length:             6,
				ProducerPort:       8901,
				ProducerBinaryPort: 8904,
				ConsumerPort:       8902,
			}))
		})
	})
})
