package data

import (
	"github.com/jorgebay/soda/internal/conf"
	. "github.com/jorgebay/soda/internal/types"
)

type SegmentReader struct {
	Items  chan ReadItem
	topic  string
	token  Token
	offset Offset
}

// Returns a log file reader.
//
// It aggressively reads ahead and maintains local cache, so there should there
// should be a different reader per consumer group.
func NewSegmentReader(
	topic string,
	token Token,
	config conf.DatalogConfig,
) *SegmentReader {
	s := &SegmentReader{
		Items: make(chan ReadItem, 16),
		topic: topic,
		token: token,
	}
	go s.read()

	return s
}

func (s *SegmentReader) read() {
	// Determine file start position

	// for each read item
	//   check if should close

}
