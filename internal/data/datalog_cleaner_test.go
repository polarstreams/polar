package data

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/polarstreams/polar/internal/test/conf/mocks"
)

var _ = Describe("datalog", func() {
	Describe("cleanUpDir()", func() {
		It("should remove files that are older than retention", func() {
			dir, err := ioutil.TempDir("", "clean_up_test")
			Expect(err).NotTo(HaveOccurred())
			createFilesToClean(dir)
			config := new(mocks.Config)
			config.On("DatalogSegmentsPath").Return(dir)
			d := datalog{config: config}
			read, removed := d.cleanUpDir(dir, 7*24*time.Hour)
			Expect(read).To(Equal(8))
			Expect(removed).To(Equal(2))
		})
	})
})

func createFilesToClean(dir string) {
	const permissions = 0755
	subDir := filepath.Join(dir, "sub_dir")
	err := os.Mkdir(subDir, permissions)
	Expect(err).NotTo(HaveOccurred())
	createEmptyFile(dir, "root_file1.dlog")
	createEmptyFile(dir, "root_file1.index")
	createEmptyFile(dir, "root_file2.dlog")
	createEmptyFile(dir, "root_file2.index")
	createEmptyFile(subDir, "sub_file1.dlog")
	createEmptyFile(subDir, "sub_file1.index")
	createEmptyFile(subDir, "sub_file2.dlog")

	modifiedTime := time.Now().AddDate(0, 0, -30)
	err = os.Chtimes(filepath.Join(dir, "root_file1.dlog"), modifiedTime, modifiedTime)
	Expect(err).NotTo(HaveOccurred())
	err = os.Chtimes(filepath.Join(subDir, "sub_file2.dlog"), modifiedTime, modifiedTime)
	Expect(err).NotTo(HaveOccurred())
}

func createEmptyFile(dir string, name string) {
	f, err := os.Create(filepath.Join(dir, name))
	Expect(err).NotTo(HaveOccurred())
	f.Close()
}
