package data

import (
	"fmt"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/barcostreams/barco/internal/conf"
	. "github.com/barcostreams/barco/internal/types"
)

// Gets a sorted list of offsets representing the name of the segment files, where the offset is less than maxOffset
func SegmentFileList(topic *TopicDataId, config conf.DatalogConfig, maxOffset int64) ([]int64, error) {
	basePath := config.DatalogPath(topic)
	pattern := fmt.Sprintf("%s/*.%s", basePath, conf.SegmentFileExtension)

	entries, err := filepath.Glob(pattern)
	if err != nil {
		return nil, err
	}

	sort.Strings(entries)
	result := make([]int64, 0, len(entries))
	for _, entry := range entries {
		filePrefix := strings.Split(filepath.Base(entry), ".")[0]
		startOffset, err := strconv.ParseInt(filePrefix, 10, 64)
		if err != nil {
			continue
		}
		if startOffset > maxOffset {
			break
		}
		result = append(result, startOffset)
	}

	return result, nil
}
