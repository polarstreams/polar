package data

import (
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/barcostreams/barco/internal/conf"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestData(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Data Suite")
}

const blockSize = 512

var _ = Describe("I/O Techniques", func() {
	It("should write blocks with direct I/O", func() {
		// Shows the basic technique for alignment
		dir, err := ioutil.TempDir("", "test_writeflags*")
		Expect(err).NotTo(HaveOccurred())

		filename := filepath.Join(dir, "sample.txt")

		file, err := os.OpenFile(filename, conf.SegmentFileWriteFlags, FilePermissions)
		Expect(err).NotTo(HaveOccurred())

		buf := makeAlignedBuffer(blockSize)

		n, err := file.Write(buf)
		Expect(err).NotTo(HaveOccurred())
		Expect(n).To(Equal(blockSize))
		n, err = file.Write(buf)
		Expect(err).NotTo(HaveOccurred())
		Expect(n).To(Equal(blockSize))
		file.Close()

		buf = makeAlignedBuffer(blockSize * 4)
		file, _ = os.OpenFile(filename, conf.SegmentFileReadFlags, FilePermissions)
		n, err = file.Read(buf)
		Expect(err).NotTo(HaveOccurred())
		// It should retrieve what was there
		Expect(n).To(Equal(blockSize * 2))
	})

	It("should read after storing something partially in the buffer", func() {
		buf := makeAlignedBuffer(alignmentSize * 4)
		// consider I read until position 3 and I want to do a new read
		const initialReadOffset = 3
		for i := 0; i < initialReadOffset; i++ {
			buf[i] = 0xf0
		}
		readBuf, offset := alignBuffer(buf[initialReadOffset:])
		Expect(offset).To(BeNumerically(">", 0))
		n := fakeAlignedFileRead(readBuf)

		// So now we move the initial bytes to the position before aligned offset
		copy(buf[offset:], buf[0:initialReadOffset])

		dataBuf := buf[offset : initialReadOffset+offset+n]
		Expect(dataBuf).To(HaveLen(initialReadOffset+n), "Initial read plus the fake read")
		Expect(dataBuf).NotTo(ContainElement(byte(0)))
		Expect(dataBuf[:initialReadOffset]).To(Equal(buf[:initialReadOffset]))
		Expect(dataBuf[initialReadOffset]).To(Equal(byte(0xff)))
		Expect(dataBuf[initialReadOffset+1]).To(Equal(byte(1)))
		Expect(dataBuf[initialReadOffset+2]).To(Equal(byte(2)))
	})

	It("reading and writing concurrently ", func() {
		dir, err := ioutil.TempDir("", "test_read_write*")
		Expect(err).NotTo(HaveOccurred())

		filename := filepath.Join(dir, "sample.txt")

		writeFile, err := os.OpenFile(filename, conf.SegmentFileWriteFlags, FilePermissions)
		Expect(err).NotTo(HaveOccurred())

		buf := makeAlignedBuffer(blockSize)

		n, err := writeFile.Write(buf)
		Expect(err).NotTo(HaveOccurred())
		Expect(n).To(Equal(blockSize))
		n, err = writeFile.Write(buf)
		Expect(err).NotTo(HaveOccurred())
		Expect(n).To(Equal(blockSize))
		writeFile.Close()

		buf = makeAlignedBuffer(blockSize * 4)
		readFile, err := os.OpenFile(filename, conf.SegmentFileReadFlags, FilePermissions)
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

// fills the half of the length of the buffer with non-zero values and return the amount of "read" bytes
func fakeAlignedFileRead(buf []byte) int {
	Expect(addressAlignment(buf)).To(BeZero())
	Expect(len(buf) % alignmentSize).To(BeZero())
	n := len(buf) / 2
	n -= n % alignmentSize
	for i := 0; i < n; i++ {
		buf[i] = byte(i)
		if buf[i] == 0 {
			buf[i] = 0xff
		}
	}
	return n
}
