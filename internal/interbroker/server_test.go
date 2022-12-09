package interbroker

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/polarstreams/polar/internal/conf"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func Test(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Interbroker Suite")
}

var _ = Describe("errorResponse", func() {
	It("should be marshal into bytes", func() {
		buffer := new(bytes.Buffer)
		for i := 0; i < 20; i++ {
			buffer.Reset()
			message := fmt.Sprintf("Hello world %d", i)
			r := &errorResponse{
				message:  message,
				streamId: 1,
			}

			err := r.Marshal(buffer)
			b := buffer.Bytes()
			Expect(err).NotTo(HaveOccurred())

			header := header{}
			err = binary.Read(buffer, conf.Endianness, &header)
			Expect(err).NotTo(HaveOccurred())

			Expect(header.StreamId).To(Equal(r.streamId))
			bodyLength := len([]byte(message))
			Expect(int(header.BodyLength)).To(Equal(bodyLength))

			Expect(len(b)).To(Equal(headerSize + bodyLength))
			messageBuffer := make([]byte, bodyLength)
			_, err = buffer.Read(messageBuffer)
			Expect(err).NotTo(HaveOccurred())
			Expect(string(messageBuffer)).To(Equal(message))
		}
	})
})
