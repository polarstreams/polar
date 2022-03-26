package data

import (
	"fmt"
	"path/filepath"
	"sort"
	"strconv"

	"github.com/barcostreams/barco/internal/conf"
	. "github.com/barcostreams/barco/internal/types"
	"github.com/rs/zerolog/log"
)

// Reads segment file names that will contain the data starting from offset
func ReadFileStructure(topicId *TopicDataId, offset int64, config conf.DatalogConfig) ([]string, error) {
	basePath := config.DatalogPath(topicId)
	entries, err := filepath.Glob(fmt.Sprintf("%s/*.%s", basePath, conf.SegmentFileExtension))

	if err != nil {
		return nil, err
	}

	sort.Strings(entries)
	result := make([]string, 0, len(entries))
	lastValidCanContainIt := ""

	for _, entry := range entries {
		filename := filepath.Base(entry)

		// Remove extension & convert to int
		id := filename[0 : len(filename)-len(conf.SegmentFileExtension)-1]
		value, err := strconv.ParseInt(id, 10, 64)

		if err != nil {
			log.Err(err).Msgf("Filename %s could not be parsed", filename)
			continue
		}
		if value > offset {
			result = append(result, filename)
		} else {
			lastValidCanContainIt = filename
		}
	}

	if lastValidCanContainIt != "" {
		result = append([]string{lastValidCanContainIt}, result...)
	}

	return result, nil
}
