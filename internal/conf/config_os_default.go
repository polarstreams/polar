//go:build windows || darwin
// +build windows darwin

package conf

import (
	"os"
)

// Fallback to O_SYNC on platforms not supported for production use
const SegmentFileWriteFlags = os.O_APPEND | os.O_CREATE | os.O_WRONLY | os.O_SYNC

const SegmentFileReadFlags = os.O_RDONLY

const IndexFileWriteFlags = SegmentFileWriteFlags

const ProducerOffsetFileWriteFlags = os.O_CREATE | os.O_WRONLY | os.O_SYNC
