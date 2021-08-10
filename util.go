package xsync

const (
	// used in paddings to prevent false sharing;
	// 64B are used instead of 128B as a compromise between
	// memory footprint and performance; 128B usage may give ~30%
	// improvement on NUMA machines
	cacheLineSize = 64
	fnv32Offset   = uint32(2166136261)
	fnv32Prime    = uint32(16777619)
)

// murmurhash3 64-bit finalizer
func hash64(x uintptr) uint32 {
	x = ((x >> 33) ^ x) * 0xff51afd7ed558ccd
	x = ((x >> 33) ^ x) * 0xc4ceb9fe1a85ec53
	x = (x >> 33) ^ x
	return uint32(x)
}

// FNV-1a 32-bit hash
func fnv32(key string) uint32 {
	hash := fnv32Offset
	keyLength := len(key)
	for i := 0; i < keyLength; i++ {
		hash ^= uint32(key[i])
		hash *= fnv32Prime
	}
	return hash
}
