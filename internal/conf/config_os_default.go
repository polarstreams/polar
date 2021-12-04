//go:build windows || darwin
// +build windows darwin

package conf

import (
	"os"
)

// Fallback to O_SYNC on platforms not supported for production use
const WriteFlags = os.O_APPEND | os.O_CREATE | os.O_WRONLY | os.O_SYNC

const ReadFlags = os.O_RDONLY

const IndexFileWriteFlags = WriteFlags
