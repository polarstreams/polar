package data

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/polarstreams/polar/internal/conf"
	. "github.com/polarstreams/polar/internal/types"
	"github.com/rs/zerolog/log"
)

func MergeDataStructure(fileNames []string, topic *TopicDataId, offset int64, config conf.DatalogConfig) error {
	log.Info().Msgf("Merging %d files for %s", len(fileNames), topic)
	localFileNames, err := ReadFileStructure(topic, offset, config)
	if err != nil {
		return err
	}
	localFileMap := make(map[string]bool, len(localFileNames))
	for _, name := range localFileNames {
		localFileMap[name] = true
	}

	basePath := config.DatalogPath(topic)
	total := 0
	for _, name := range fileNames {
		if localFileMap[name] {
			continue
		}

		emptyFile, err := os.OpenFile(filepath.Join(basePath, name), os.O_CREATE|os.O_WRONLY, FilePermissions)
		if err != nil {
			return err
		}
		total++
		_ = emptyFile.Close()
	}

	if total > 0 {
		log.Info().Msgf("%d files merged for %s", total, topic)
	} else if len(localFileNames) > 0 {
		log.Info().Msgf("All files already present for %s", topic)
	} else {
		return fmt.Errorf("No file was found for %s", topic)
	}

	return nil
}
