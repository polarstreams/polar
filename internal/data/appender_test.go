package data

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/jorgebay/soda/internal/conf"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestAppender(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Appender Suite")
}

var _ = Describe("WriteFlags", func() {
	It("should write blocks with direct I/O", func() {
		blockSize := 512
		dir, err := ioutil.TempDir("", "test_writeflags*")
		Expect(err).NotTo(HaveOccurred())

		filename := filepath.Join(dir, "sample.txt")

		file, err := os.OpenFile(filename, conf.WriteFlags, filePermissions)
		Expect(err).NotTo(HaveOccurred())

		buf := make([]byte, blockSize)

		n, err := file.Write(buf)
		Expect(err).NotTo(HaveOccurred())
		Expect(n).To(Equal(blockSize))
		n, err = file.Write(buf)
		Expect(err).NotTo(HaveOccurred())
		Expect(n).To(Equal(blockSize))
		file.Close()

		buf = make([]byte, blockSize*4)
		file, _ = os.OpenFile(filename, conf.ReadFlags, filePermissions)
		n, err = file.Read(buf)
		Expect(err).NotTo(HaveOccurred())
		// It should retrieve what
		Expect(n).To(Equal(blockSize * 2))
	})
})
