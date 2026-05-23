package cache

import (
	"fmt"
	"strconv"
	"strings"
	"syscall"
)

// CacheLevel represents the current cache utilization level.
type CacheLevel int

const (
	CacheLevelNormal          CacheLevel = iota // < 80%
	CacheLevelWarning                           // >= 80%
	CacheLevelPauseScheduling                   // >= 90%
	CacheLevelFull                              // >= 100%
)

// String returns the string representation of a CacheLevel.
func (l CacheLevel) String() string {
	switch l {
	case CacheLevelNormal:
		return "Normal"
	case CacheLevelWarning:
		return "Warning"
	case CacheLevelPauseScheduling:
		return "PauseScheduling"
	case CacheLevelFull:
		return "Full"
	default:
		return fmt.Sprintf("CacheLevel(%d)", l)
	}
}

// ParseCacheSize parses a cache size string and returns bytes.
// Supported formats:
//   - Fixed sizes: "50GB", "100MB", "1TB", "256KB" (case insensitive)
//   - Percentage of disk: "80%" (uses disk total at cacheDir)
func ParseCacheSize(value string, cacheDir string) (int64, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, fmt.Errorf("cache size cannot be empty")
	}

	// Handle percentage
	if strings.HasSuffix(value, "%") {
		return parseCacheSizePercent(value, cacheDir)
	}

	// Handle fixed size
	return parseCacheSizeFixed(value)
}

func parseCacheSizePercent(value string, cacheDir string) (int64, error) {
	numStr := strings.TrimSuffix(value, "%")
	pct, err := strconv.ParseFloat(numStr, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid percentage %q: %w", value, err)
	}
	if pct <= 0 || pct > 100 {
		return 0, fmt.Errorf("percentage must be between 1 and 100, got %v", pct)
	}

	totalBytes, err := diskTotal(cacheDir)
	if err != nil {
		return 0, fmt.Errorf("failed to get disk size for %q: %w", cacheDir, err)
	}

	result := int64(float64(totalBytes) * pct / 100.0)
	return result, nil
}

func parseCacheSizeFixed(value string) (int64, error) {
	upper := strings.ToUpper(value)

	var suffix string
	var multiplier int64

	switch {
	case strings.HasSuffix(upper, "TB"):
		suffix = "TB"
		multiplier = 1024 * 1024 * 1024 * 1024
	case strings.HasSuffix(upper, "GB"):
		suffix = "GB"
		multiplier = 1024 * 1024 * 1024
	case strings.HasSuffix(upper, "MB"):
		suffix = "MB"
		multiplier = 1024 * 1024
	case strings.HasSuffix(upper, "KB"):
		suffix = "KB"
		multiplier = 1024
	default:
		return 0, fmt.Errorf("invalid cache size %q: must end with TB, GB, MB, or KB", value)
	}

	numStr := strings.TrimSuffix(upper, suffix)
	num, err := strconv.ParseFloat(numStr, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid cache size %q: %w", value, err)
	}
	if num <= 0 {
		return 0, fmt.Errorf("cache size must be positive, got %v", num)
	}

	return int64(num * float64(multiplier)), nil
}

// diskTotal returns total disk bytes at path using syscall.Statfs.
func diskTotal(path string) (uint64, error) {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(path, &stat); err != nil {
		return 0, err
	}
	// Total bytes = total blocks * block size
	return stat.Blocks * uint64(stat.Bsize), nil
}

// CheckCacheLevel determines the cache level based on current usage vs max size.
func CheckCacheLevel(currentSize, maxSize int64) CacheLevel {
	if maxSize <= 0 {
		return CacheLevelFull
	}

	ratio := float64(currentSize) / float64(maxSize)

	switch {
	case ratio >= 1.0:
		return CacheLevelFull
	case ratio >= 0.9:
		return CacheLevelPauseScheduling
	case ratio >= 0.8:
		return CacheLevelWarning
	default:
		return CacheLevelNormal
	}
}
