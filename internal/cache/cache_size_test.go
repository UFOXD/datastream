package cache

import (
	"testing"
)

func TestParseCacheSize_FixedSizes(t *testing.T) {
	tests := []struct {
		input    string
		expected int64
	}{
		{"50GB", 50 * 1024 * 1024 * 1024},
		{"100MB", 100 * 1024 * 1024},
		{"1TB", 1 * 1024 * 1024 * 1024 * 1024},
		{"500gb", 500 * 1024 * 1024 * 1024}, // case insensitive
		{"256KB", 256 * 1024},
		{"10tb", 10 * 1024 * 1024 * 1024 * 1024},
		{"50mb", 50 * 1024 * 1024},
		{"100kb", 100 * 1024},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := ParseCacheSize(tt.input, "/tmp")
			if err != nil {
				t.Fatalf("ParseCacheSize(%q) unexpected error: %v", tt.input, err)
			}
			if got != tt.expected {
				t.Errorf("ParseCacheSize(%q) = %d, want %d", tt.input, got, tt.expected)
			}
		})
	}
}

func TestParseCacheSize_Percentage(t *testing.T) {
	got, err := ParseCacheSize("80%", "/tmp")
	if err != nil {
		t.Fatalf("ParseCacheSize(\"80%%\") unexpected error: %v", err)
	}
	if got <= 0 {
		t.Errorf("ParseCacheSize(\"80%%\") = %d, want > 0", got)
	}

	got50, err := ParseCacheSize("50%", "/tmp")
	if err != nil {
		t.Fatalf("ParseCacheSize(\"50%%\") unexpected error: %v", err)
	}
	if got50 >= got {
		t.Errorf("ParseCacheSize(\"50%%\") = %d should be less than ParseCacheSize(\"80%%\") = %d", got50, got)
	}
}

func TestParseCacheSize_Invalid(t *testing.T) {
	tests := []struct {
		input string
	}{
		{"abc"},
		{""},
		{"GB"},
		{"%"},
		{"0%"},
		{"-10GB"},
		{"101%"},
		{"0GB"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			_, err := ParseCacheSize(tt.input, "/tmp")
			if err == nil {
				t.Errorf("ParseCacheSize(%q) expected error, got nil", tt.input)
			}
		})
	}
}

func TestCheckCacheLevel(t *testing.T) {
	maxSize := int64(100)

	tests := []struct {
		name        string
		currentSize int64
		expected    CacheLevel
	}{
		{"0% → Normal", 0, CacheLevelNormal},
		{"50% → Normal", 50, CacheLevelNormal},
		{"79% → Normal", 79, CacheLevelNormal},
		{"80% → Warning", 80, CacheLevelWarning},
		{"82% → Warning", 82, CacheLevelWarning},
		{"89% → Warning", 89, CacheLevelWarning},
		{"90% → PauseScheduling", 90, CacheLevelPauseScheduling},
		{"92% → PauseScheduling", 92, CacheLevelPauseScheduling},
		{"99% → PauseScheduling", 99, CacheLevelPauseScheduling},
		{"100% → Full", 100, CacheLevelFull},
		{"120% → Full", 120, CacheLevelFull},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := CheckCacheLevel(tt.currentSize, maxSize)
			if got != tt.expected {
				t.Errorf("CheckCacheLevel(%d, %d) = %v, want %v", tt.currentSize, maxSize, got, tt.expected)
			}
		})
	}
}

func TestCacheLevel_String(t *testing.T) {
	tests := []struct {
		level    CacheLevel
		expected string
	}{
		{CacheLevelNormal, "Normal"},
		{CacheLevelWarning, "Warning"},
		{CacheLevelPauseScheduling, "PauseScheduling"},
		{CacheLevelFull, "Full"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			if got := tt.level.String(); got != tt.expected {
				t.Errorf("CacheLevel(%d).String() = %q, want %q", tt.level, got, tt.expected)
			}
		})
	}
}
