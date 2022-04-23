//go:build profiling
// +build profiling

package conf

import (
	"fmt"
	"os"
	"runtime/pprof"
	"time"

	"github.com/rs/zerolog/log"
)

// Enables profiling when the build tag is set
func StartProfiling() bool {
	fileName := fmt.Sprintf("barco-profile-%d.prof", time.Now().UnixMicro())
	f, err := os.Create(fileName)
	if err != nil {
		log.Panic().Err(err).Msgf("CPU profile file could not be created")
	}
	err = pprof.StartCPUProfile(f)
	if err != nil {
		log.Panic().Err(err).Msgf("CPU profile could not be started")
	}
	return true
}

func StopProfiling() {
	pprof.StopCPUProfile()
}
