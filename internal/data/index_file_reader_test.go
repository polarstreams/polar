package data

import (
	"fmt"
	"io/ioutil"

	"github.com/jorgebay/soda/internal/test/conf/mocks"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("tryReadIndexFile()", func() {
	It("Should give the file offset closest (lower bound) to the provided one", func() {

		dir, err := ioutil.TempDir("", "test_index")
		Expect(err).NotTo(HaveOccurred())
		writeIndexFile(dir, 50)
		filePrefix := fmt.Sprintf("%020d", 0)

		// Should find the message offset 50 with fileOffset 500
		fileOffset := tryReadIndexFile(dir, filePrefix, 57)
		Expect(fileOffset).To(Equal(int64(500)))

		// Equal
		fileOffset = tryReadIndexFile(dir, filePrefix, 100)
		Expect(fileOffset).To(Equal(int64(1000)))

		// Should find the last message offset 490 with fileOffset 4900
		fileOffset = tryReadIndexFile(dir, filePrefix, 60*10)
		Expect(fileOffset).To(Equal(int64(4900)))
	})
})

func writeIndexFile(basePath string, steps int) {
	config := new(mocks.Config)
	// Output everything
	config.On("IndexFilePeriodBytes").Return(1)

	w := &indexFileWriter{
		items:        make(chan indexFileItem),
		basePath:     basePath,
		config:       config,
		closed:       make(chan bool, 1),
		offsetWriter: newOffsetFileWriter(),
	}

	go w.writeLoop()

	for i := 0; i < steps; i++ {
		w.append(0, uint64(i*10), int64(i*100), 0)
	}

	close(w.items)

	<-w.closed
}
