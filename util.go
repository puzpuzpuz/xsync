package xsync

import (
	"reflect"
	"unsafe"
)

// test-only assert()-like flag
var assertionsEnabled = false

const (
	// used in paddings to prevent false sharing;
	// 64B are used instead of 128B as a compromise between
	// memory footprint and performance; 128B usage may give ~30%
	// improvement on NUMA machines
	cacheLineSize = 64
	// the seed value is of an absolutely arbitrary choice
	maphashSeed = 42
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

// exposes the built-in memhash function
func maphash64(s string) uint64 {
	if s == "" {
		return maphashSeed
	}
	strh := (*reflect.StringHeader)(unsafe.Pointer(&s))
	return uint64(memhash(unsafe.Pointer(strh.Data), maphashSeed, uintptr(strh.Len)))
}

//go:noescape
//go:linkname memhash runtime.memhash
func memhash(p unsafe.Pointer, h, s uintptr) uintptr
