// Package version provides version information for DataStream.
package version

import (
	"fmt"
	"runtime"
)

var (
	// Version is the semantic version of DataStream.
	// Set via ldflags at build time.
	Version = "unknown"

	// GitHash is the git commit hash.
	// Set via ldflags at build time.
	GitHash = "unknown"

	// BuildTime is the time when the binary was built.
	// Set via ldflags at build time.
	BuildTime = "unknown"

	// GoVersion is the Go version used to compile DataStream.
	GoVersion = runtime.Version()

	// Platform is the OS/Arch combination.
	Platform = fmt.Sprintf("%s/%s", runtime.GOOS, runtime.GOARCH)
)

// Info contains version information.
type Info struct {
	Version   string `json:"version"`
	GitHash   string `json:"git_hash"`
	BuildTime string `json:"build_time"`
	GoVersion string `json:"go_version"`
	Platform  string `json:"platform"`
}

// GetInfo returns the current version info.
func GetInfo() Info {
	return Info{
		Version:   Version,
		GitHash:   GitHash,
		BuildTime: BuildTime,
		GoVersion: GoVersion,
		Platform:  Platform,
	}
}

// String returns a formatted version string.
func (i Info) String() string {
	return fmt.Sprintf("DataStream %s (git: %s, built: %s, go: %s, platform: %s)",
		i.Version, i.GitHash, i.BuildTime, i.GoVersion, i.Platform)
}
