package utils

import "strings"

// IsBlank reports whether s is empty or contains only whitespace characters.
func IsBlank(s string) bool {
	return strings.TrimSpace(s) == ""
}

// DefaultString returns s when it is non-blank, otherwise fallback.
func DefaultString(s, fallback string) string {
	if IsBlank(s) {
		return fallback
	}
	return s
}

// TruncateString shortens s to at most max runes, appending an ellipsis when
// truncation occurs. A non-positive max returns the empty string.
func TruncateString(s string, max int) string {
	if max <= 0 {
		return ""
	}
	runes := []rune(s)
	if len(runes) <= max {
		return s
	}
	if max <= 3 {
		return string(runes[:max])
	}
	return string(runes[:max-3]) + "..."
}
