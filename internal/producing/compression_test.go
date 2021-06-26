package producing_test

import (
	"bytes"
	"io"
	"strings"
	"testing"

	"github.com/klauspost/compress/zstd"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestSuite(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Producing Suite")
}

var _ = Describe("Compression", func() {
	It("should compress and decompress", func() {
		// Try to demostrate the technique used in a test
		// Reuse both the buffer and the writter across multiple iteration
		var b bytes.Buffer
		capacity := 0
		w, _ := zstd.NewWriter(&b, zstd.WithEncoderCRC(true))

		// shared group buffer
		for i := 0; i < 5; i++ {
			b.Reset()
			w.Reset(&b)
			// Placeholder for length
			b.Write([]byte{0, 0, 0, 0})
			text := "Lorem ipsum dolor sit amet consectetur adipiscing elit quisque, nisl tristique eros pellentesque cum vulputate aptent leo, gravida habitasse primis sed ligula senectus rutrum. Aliquam donec pharetra himenaeos dignissim aenean porttitor mauris litora tristique a lectus molestie id, ridiculus morbi praesent fames pretium nunc euismod ultrices netus fusce ligula ultricies. Placerat rutrum mollis ligula etiam suscipit risus himenaeos maecenas, netus gravida bibendum semper arcu hac ut curae, sagittis fames aenean vulputate luctus odio torquent."
			text = text + text + text + text + text + text + text + text + text + text + text + text + text + text + text + text
			written, err := w.Write([]byte(text))
			Expect(err).ToNot(HaveOccurred())
			w.Write([]byte("-FIN\n"))
			err = w.Close()
			Expect(err).ToNot(HaveOccurred())

			Expect(written).To(Equal(len(text)))

			// Skip length
			reader := bytes.NewReader(b.Bytes()[4:])
			r, err := zstd.NewReader(reader)
			Expect(err).ToNot(HaveOccurred())
			strBuilder := new(strings.Builder)
			_, err = io.Copy(strBuilder, r)
			Expect(err).ToNot(HaveOccurred())
			str := strBuilder.String()
			Expect(str).To(Equal(text + "-FIN\n"))

			if capacity == 0 {
				capacity = b.Cap()
			} else {
				// Assert the capacity is stable
				Expect(capacity).To(Equal(b.Cap()))
			}

			r.Close()
		}
	})
})
