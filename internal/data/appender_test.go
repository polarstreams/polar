package data

import (
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/jorgebay/soda/internal/conf"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

const blockSize = 512

func TestData(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Data Suite")
}

var _ = Describe("I/O Techniques", func() {
	It("should write blocks with direct I/O", func() {
		// Shows the basic technique for alignment
		dir, err := ioutil.TempDir("", "test_writeflags*")
		Expect(err).NotTo(HaveOccurred())

		filename := filepath.Join(dir, "sample.txt")

		file, err := os.OpenFile(filename, conf.WriteFlags, FilePermissions)
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
		file, _ = os.OpenFile(filename, conf.ReadFlags, FilePermissions)
		n, err = file.Read(buf)
		Expect(err).NotTo(HaveOccurred())
		// It should retrieve what was there
		Expect(n).To(Equal(blockSize * 2))
	})

	It("reading and writing concurrently ", func() {
		dir, err := ioutil.TempDir("", "test_read_write*")
		Expect(err).NotTo(HaveOccurred())

		filename := filepath.Join(dir, "sample.txt")

		writeFile, err := os.OpenFile(filename, conf.WriteFlags, FilePermissions)
		Expect(err).NotTo(HaveOccurred())

		buf := make([]byte, blockSize)

		n, err := writeFile.Write(buf)
		Expect(err).NotTo(HaveOccurred())
		Expect(n).To(Equal(blockSize))
		n, err = writeFile.Write(buf)
		Expect(err).NotTo(HaveOccurred())
		Expect(n).To(Equal(blockSize))
		writeFile.Close()

		buf = make([]byte, blockSize*4)
		readFile, err := os.OpenFile(filename, conf.ReadFlags, FilePermissions)
		Expect(err).NotTo(HaveOccurred())
		n, err = readFile.Read(buf)
		Expect(err).NotTo(HaveOccurred())
		// It should retrieve what was there
		Expect(n).To(Equal(blockSize * 2))

		// Read more than what's there
		n, err = readFile.Read(buf)
		Expect(n).To(BeZero())
		Expect(err).To(Equal(io.EOF))

		writeFile.Close()
		readFile.Close()
	})
})
