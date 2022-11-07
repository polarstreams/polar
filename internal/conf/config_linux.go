package conf

import (
	"os"
	"syscall"
)

// Use DIRECT IO for linux
const readFileDirectFlags = os.O_RDONLY | syscall.O_DIRECT

const SegmentFileWriteFlags = os.O_APPEND | os.O_CREATE | os.O_WRONLY | syscall.O_DIRECT | syscall.O_DSYNC

const SegmentFileReadFlags = readFileDirectFlags

// Use page cache for index file as it's not critical and it won't abuse the cache space
const IndexFileWriteFlags = os.O_APPEND | os.O_CREATE | os.O_WRONLY

// Use page cache for max offset producer file as it won't abuse the cache space
const ProducerOffsetFileWriteFlags = os.O_CREATE | os.O_WRONLY

const ProducerOffsetFileReadFlags = os.O_RDONLY
