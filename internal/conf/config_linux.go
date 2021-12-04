package conf

import (
	"os"
	"syscall"
)

// Use DIRECT IO for linux
const WriteFlags = os.O_APPEND | os.O_CREATE | os.O_WRONLY | syscall.O_DIRECT | syscall.O_DSYNC

const ReadFlags = os.O_RDONLY | syscall.O_DIRECT

// Use page cache for index file as it's not critical and it won't abuse the cache space
const IndexFileWriteFlags = os.O_APPEND | os.O_CREATE | os.O_WRONLY
