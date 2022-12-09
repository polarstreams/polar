package data

import (
	"io/ioutil"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/polarstreams/polar/internal/test/conf/mocks"
	. "github.com/polarstreams/polar/internal/types"
	"github.com/stretchr/testify/mock"
)

var _ = Describe("offsetFileWriter and reader", func() {
	It("should read and write", func() {
		dir, err := ioutil.TempDir("", "offset_file_test")
		Expect(err).NotTo(HaveOccurred())

		config := new(mocks.Config)
		config.On("DatalogPath", mock.Anything).Return(dir)
		writer := newOffsetFileWriter()
		writer.create(dir)
		defer writer.close()
		writer.write(123)

		obtained, err := readProducerOffset(&TopicDataId{}, config)
		Expect(err).NotTo(HaveOccurred())
		Expect(obtained).To(Equal(int64(123)))

		writer.write(456)
		obtained, err = readProducerOffset(&TopicDataId{}, config)
		Expect(err).NotTo(HaveOccurred())
		Expect(obtained).To(Equal(int64(456)))
	})
})
