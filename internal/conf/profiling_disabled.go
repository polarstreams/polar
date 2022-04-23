//go:build !profiling
// +build !profiling

package conf

// Enables profiling when the build tag is set
func StartProfiling() bool {
	return false
}

func StopProfiling() {

}
