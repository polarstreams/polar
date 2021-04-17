package conf

import (
	"os"
	"syscall"
)

// Use DIRECT IO for linux
const WriteFlags = os.O_APPEND | os.O_CREATE | os.O_WRONLY | syscall.O_DIRECT | syscall.O_DSYNC

const ReadFlags = os.O_RDONLY
