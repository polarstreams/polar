package data

import (
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/barcostreams/barco/internal/test/conf/mocks"
	. "github.com/barcostreams/barco/internal/types"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("ReadFileStructure()", func() {
	It("should return the filenames that might contain the offset", func() {
		dir, err := ioutil.TempDir("", "file_structure_test")
		Expect(err).NotTo(HaveOccurred())

		topic := &TopicDataId{}
		config := new(mocks.Config)
		config.On("DatalogPath", topic).Return(dir)
		Expect(ReadFileStructure(topic, 0, config)).To(BeEmpty())

		files := []string{"00000000000.dlog", "00000001000.dlog", "00000001005.dlog", "99000001000.dlog"}
		Expect(os.Create(filepath.Join(dir, files[0]))).NotTo(BeNil())
		Expect(ReadFileStructure(topic, 1000, config)).To(Equal(files[:1]))

		Expect(os.Create(filepath.Join(dir, "invalid.dlog"))).NotTo(BeNil())
		Expect(os.Create(filepath.Join(dir, files[1]))).NotTo(BeNil())
		Expect(os.Create(filepath.Join(dir, files[2]))).NotTo(BeNil())
		Expect(os.Create(filepath.Join(dir, files[3]))).NotTo(BeNil())

		Expect(ReadFileStructure(topic, 0, config)).To(Equal(files))
		Expect(ReadFileStructure(topic, 1, config)).To(Equal(files))
		Expect(ReadFileStructure(topic, 1000, config)).To(Equal(files[1:]))
		Expect(ReadFileStructure(topic, 1001, config)).To(Equal(files[1:]))
		Expect(ReadFileStructure(topic, 1002, config)).To(Equal(files[1:]))
		Expect(ReadFileStructure(topic, 2000, config)).To(Equal(files[2:]))
		Expect(ReadFileStructure(topic, 99000001000, config)).To(Equal(files[3:]))
		Expect(ReadFileStructure(topic, 99000001001, config)).To(Equal(files[3:]))
	})
})
