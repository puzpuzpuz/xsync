package xsync

const (
	// used in paddings to prevent false sharing
	cacheLineSize = 64
)

// murmurhash3 64-bit finalizer
func hash64(x uintptr) uintptr {
	x = ((x >> 33) ^ x) * 0xff51afd7ed558ccd
	x = ((x >> 33) ^ x) * 0xc4ceb9fe1a85ec53
	x = (x >> 33) ^ x
	return x
}
