package xsync

import (
	"hash/maphash"
)

// test-only assert()-like flag
var assertionsEnabled = false

const (
	// used in paddings to prevent false sharing;
	// 64B are used instead of 128B as a compromise between
	// memory footprint and performance; 128B usage may give ~30%
	// improvement on NUMA machines
	cacheLineSize = 64
)

// murmurhash3 64-bit finalizer
func mixhash64ptr(v uintptr) uint64 {
	x := uint64(v)
	x = ((x >> 33) ^ x) * 0xff51afd7ed558ccd
	x = ((x >> 33) ^ x) * 0xc4ceb9fe1a85ec53
	x = (x >> 33) ^ x
	return x
}

// murmurhash3 64-bit finalizer
//
//lint:ignore U1000 used in MapOf
func mixhash64(v uint64) uint64 {
	v = ((v >> 33) ^ v) * 0xff51afd7ed558ccd
	v = ((v >> 33) ^ v) * 0xc4ceb9fe1a85ec53
	v = (v >> 33) ^ v
	return v
}

// hashString calculates a hash of s with the given seed.
func hashString(seed maphash.Seed, s string) uint64 {
	var h maphash.Hash
	h.SetSeed(seed)
	h.WriteString(s)
	return h.Sum64()
}
