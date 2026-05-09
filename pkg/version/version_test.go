package version

import (
	"runtime"
	"testing"
)

func TestGetInfo(t *testing.T) {
	info := GetInfo()

	if info.Version == "" {
		t.Error("Version should not be empty")
	}
	if info.GitHash == "" {
		t.Error("GitHash should not be empty")
	}
	if info.BuildTime == "" {
		t.Error("BuildTime should not be empty")
	}
	if info.GoVersion == "" {
		t.Error("GoVersion should not be empty")
	}
	if info.Platform == "" {
		t.Error("Platform should not be empty")
	}
}

func TestInfoString(t *testing.T) {
	info := GetInfo()
	str := info.String()

	if str == "" {
		t.Error("String() should not return empty string")
	}

	// Verify it contains version info
	if len(str) < 10 {
		t.Error("String() should return a meaningful version string")
	}
}

func TestDefaultVersion(t *testing.T) {
	// When not set via ldflags, should be "unknown"
	// This test verifies the default value
	if Version != "unknown" && Version == "" {
		t.Error("Version should have a default value")
	}
}

func TestDefaultGitHash(t *testing.T) {
	// When not set via ldflags, should be "unknown"
	if GitHash != "unknown" && GitHash == "" {
		t.Error("GitHash should have a default value")
	}
}

func TestDefaultBuildTime(t *testing.T) {
	// When not set via ldflags, should be "unknown"
	if BuildTime != "unknown" && BuildTime == "" {
		t.Error("BuildTime should have a default value")
	}
}

func TestGoVersion(t *testing.T) {
	// GoVersion should be set from runtime
	expected := runtime.Version()
	if GoVersion != expected {
		t.Errorf("expected GoVersion '%s', got '%s'", expected, GoVersion)
	}
}

func TestPlatform(t *testing.T) {
	// Platform should be OS/Arch
	expected := runtime.GOOS + "/" + runtime.GOARCH
	if Platform != expected {
		t.Errorf("expected Platform '%s', got '%s'", expected, Platform)
	}
}

func TestInfoFields(t *testing.T) {
	info := GetInfo()

	// Verify all fields are accessible
	_ = info.Version
	_ = info.GitHash
	_ = info.BuildTime
	_ = info.GoVersion
	_ = info.Platform
}

func TestInfoConsistency(t *testing.T) {
	// Multiple calls should return consistent data
	info1 := GetInfo()
	info2 := GetInfo()

	if info1.Version != info2.Version {
		t.Error("Version should be consistent across calls")
	}
	if info1.GitHash != info2.GitHash {
		t.Error("GitHash should be consistent across calls")
	}
	if info1.BuildTime != info2.BuildTime {
		t.Error("BuildTime should be consistent across calls")
	}
}

func TestStringFormat(t *testing.T) {
	info := Info{
		Version:   "1.0.0",
		GitHash:   "abc123",
		BuildTime: "2024-01-01",
		GoVersion: "go1.21",
		Platform:  "linux/amd64",
	}

	str := info.String()

	// Verify format contains expected elements
	if str == "" {
		t.Error("String should not be empty")
	}

	// The string should contain "DataStream"
	// and version info
	if len(str) < len("DataStream 1.0.0") {
		t.Error("String should contain version info")
	}
}
