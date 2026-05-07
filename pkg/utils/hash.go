package utils

// FNV-1a hash constants (see https://en.wikipedia.org/wiki/Fowler-Noll-Vo_hash_function).
const (
	fnv32Offset uint32 = 2166136261
	fnv32Prime  uint32 = 16777619

	fnv64Offset uint64 = 14695981039346656037
	fnv64Prime  uint64 = 1099511628211
)

// FNV32 returns the 32-bit FNV-1a hash of s.
func FNV32(s string) uint32 {
	h := fnv32Offset
	for i := 0; i < len(s); i++ {
		h ^= uint32(s[i])
		h *= fnv32Prime
	}
	return h
}

// FNV64 returns the 64-bit FNV-1a hash of s.
func FNV64(s string) uint64 {
	h := fnv64Offset
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= fnv64Prime
	}
	return h
}
