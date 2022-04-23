package data

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"os"
	"path/filepath"

	"github.com/barcostreams/barco/internal/conf"
	"github.com/barcostreams/barco/internal/utils"
	"github.com/rs/zerolog/log"
)

// Writes the last known producer offset to a single file.
//
// IMPORTANT: Methods are not thread-safe
type offsetFileWriter struct {
	file   *os.File
	buf    []byte
	writer *bytes.Buffer
}

func newOffsetFileWriter() *offsetFileWriter {
	return &offsetFileWriter{}
}

func (w *offsetFileWriter) create(basePath string) {
	f, err := os.OpenFile(filepath.Join(basePath, conf.ProducerOffsetFileName), conf.ProducerOffsetFileWriteFlags, FilePermissions)
	utils.PanicIfErr(err, "Producer offset file could not be created")
	w.file = f
	w.buf = makeAlignedBuffer(alignmentSize)
	w.writer = bytes.NewBuffer(w.buf)
}

func (w *offsetFileWriter) write(value int64) {
	w.writer.Reset()
	binary.Write(w.writer, conf.Endianness, value)
	binary.Write(w.writer, conf.Endianness, crc32.ChecksumIEEE(w.writer.Bytes()))
	_, err := w.file.WriteAt(w.buf, 0)
	utils.PanicIfErr(err, "Producer offset file could not be written")
}

func (w *offsetFileWriter) close() {
	log.Err(w.file.Close()).Msgf("Producer file closed")
}
