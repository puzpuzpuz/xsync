package xsync

import (
	"hash/maphash"
	"runtime"
	"unsafe"
	_ "unsafe"
)

// test-only assert()-like flag
var assertionsEnabled = false

const (
	// cacheLineSize is used in paddings to prevent false sharing;
	// 64B are used instead of 128B as a compromise between
	// memory footprint and performance; 128B usage may give ~30%
	// improvement on NUMA machines.
	cacheLineSize = 64
)

// hashString calculates a hash of s with the given seed.
func hashString(seed maphash.Seed, s string) uint64 {
	var h maphash.Hash
	h.SetSeed(seed)
	h.WriteString(s)
	return h.Sum64()
}

// nextPowOf2 computes the next highest power of 2 of 32-bit v.
// Source: https://graphics.stanford.edu/~seander/bithacks.html#RoundUpPowerOf2
func nextPowOf2(v uint32) uint32 {
	v--
	v |= v >> 1
	v |= v >> 2
	v |= v >> 4
	v |= v >> 8
	v |= v >> 16
	v++
	return v
}

func parallelism() uint32 {
	maxProcs := uint32(runtime.GOMAXPROCS(0))
	numCores := uint32(runtime.NumCPU())
	if maxProcs < numCores {
		return maxProcs
	}
	return numCores
}

// hashUint64 calculates a hash of v with the given seed.
//
//lint:ignore U1000 used in MapOf
func hashUint64(seed maphash.Seed, v uint64) uint64 {
	nseed := *(*uint64)(unsafe.Pointer(&seed))
	return uint64(memhash64(unsafe.Pointer(&v), uintptr(nseed)))
}

//go:noescape
//go:linkname fastrand runtime.fastrand
func fastrand() uint32

//go:noescape
//go:linkname memhash64 runtime.memhash64
func memhash64(p unsafe.Pointer, h uintptr) uintptr
