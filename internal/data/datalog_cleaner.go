package data

import (
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/barcostreams/barco/internal/conf"
	"github.com/rs/zerolog/log"
)

func (d *datalog) cleanUp(retention time.Duration) {
	delay := time.Duration(RetentionCheckMs) * time.Millisecond
	for {
		time.Sleep(delay)
		log.Info().Msgf("Start looking for log files to clean up pass the retention time")

		start := time.Now()
		read, removed := d.cleanUpDir(d.config.DatalogSegmentsPath(), retention)
		diff := time.Since(start)
		log.Info().Msgf(
			"Log clean up took %dms to visit %d files and folders and removed %d segment files",
			diff.Milliseconds(), read, removed)
	}
}

func (d *datalog) cleanUpDir(dirPath string, retention time.Duration) (read int, removed int) {
	log.Debug().Msgf("Log clean up reading dir %s", dirPath)
	dir, err := os.Open(dirPath)
	if err != nil {
		log.Err(err).Msgf("Log clean up could not open the dir %s", dirPath)
		return
	}

	segmentFileExtension := "." + conf.SegmentFileExtension
	for {
		stats, err := dir.Readdir(1000)
		if err != nil && err != io.EOF {
			log.Err(err).Msgf("Log clean up could not read the dir %s", dirPath)
			return
		}
		if len(stats) == 0 {
			// Finished clean up
			log.Info().Msgf("Finishing cleaning up %s", dirPath)
			return
		}
		for _, file := range stats {
			read++
			if file.IsDir() {
				// Navigate the children of the dir
				subRead, subRemoved := d.cleanUpDir(filepath.Join(dirPath, file.Name()), retention)
				read += subRead
				removed += subRemoved
				continue
			}

			if filepath.Ext(file.Name()) == segmentFileExtension {
				removed += d.cleanUpFile(dirPath, file, retention)
			}
		}
	}
}

func (d *datalog) cleanUpFile(dirPath string, file fs.FileInfo, retention time.Duration) int {
	if time.Since(file.ModTime()) < retention {
		return 0
	}

	log.Debug().Msgf("Log clean up removing segment file %s/%s", dirPath, file.Name())

	// Remove the index file
	indexFile := strings.TrimSuffix(filepath.Base(file.Name()), conf.SegmentFileExtension) + conf.IndexFileExtension
	if err := os.RemoveAll(filepath.Join(dirPath, indexFile)); err != nil {
		log.Err(err).Msgf("Failed to remove index file %s on %s", dirPath, file.Name())
	}

	// Remove the actual segment
	if err := os.Remove(filepath.Join(dirPath, file.Name())); err != nil {
		log.Err(err).Msgf("Failed to remove segment file %s on %s", dirPath, file.Name())
		return 0
	}
	return 1
}
