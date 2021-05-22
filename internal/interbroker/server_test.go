package interbroker

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func Test(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Interbroker Suite")
}

var _ = Describe("errorResponse", func() {
	It("should be read into a slice", func() {
		buffer := make([]byte, 1024)
		for i := 0; i < 20; i++ {
			message := fmt.Sprintf("Hello world %d", i)
			r := &errorResponse{
				message:  message,
				streamId: 1,
			}

			n, err := r.Read(buffer)
			Expect(err).NotTo(HaveOccurred())
			Expect(n).To(BeNumerically(">", headerSize))

			header := header{}
			reader := bytes.NewReader(buffer)
			err = binary.Read(reader, binary.BigEndian, &header)
			Expect(err).NotTo(HaveOccurred())

			Expect(header.StreamId).To(Equal(r.streamId))
			bodyLength := len([]byte(message))
			Expect(int(header.BodyLength)).To(Equal(bodyLength))

			Expect(n).To(Equal(headerSize + bodyLength))
			Expect(string(buffer[headerSize:n])).To(Equal(message))
		}
	})
})
