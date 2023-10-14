package xsync

import (
	"encoding/binary"
	"fmt"
	"hash/maphash"
	"reflect"
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

// nextPowOf2 computes the next highest power of 2 of 32-bit v.
// Source: https://graphics.stanford.edu/~seander/bithacks.html#RoundUpPowerOf2
func nextPowOf2(v uint32) uint32 {
	if v == 0 {
		return 1
	}
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

// hashString calculates a hash of s with the given seed.
func hashString(seed maphash.Seed, s string) uint64 {
	seed64 := *(*uint64)(unsafe.Pointer(&seed))
	if s == "" {
		return seed64
	}
	strh := (*reflect.StringHeader)(unsafe.Pointer(&s))
	return uint64(memhash(unsafe.Pointer(strh.Data), uintptr(seed64), uintptr(strh.Len)))
}

func MakeHashFunc[T comparable]() func(T) uint64 {
	var zero T
	rt := reflect.TypeOf(&zero).Elem() // Elem() avoids panic when T is interface

	seed := maphash.MakeSeed()
	seed64 := *(*uint64)(unsafe.Pointer(&seed))

	switch rt.Kind() {
	// various integers and pointers
	case reflect.Int, reflect.Uint, reflect.Int8, reflect.Uint8, reflect.Int16, reflect.Uint16, reflect.Int32, reflect.Uint32, reflect.Int64, reflect.Uint64,
		reflect.Pointer, reflect.UnsafePointer, reflect.Chan:
		return func(v T) uint64 {

			return uint64(*(*uintptr)(unsafe.Pointer(&v)))
		}

	// strings use the same trick as in hashString()
	case reflect.String:
		return func(v T) uint64 {
			strh := (*reflect.StringHeader)(unsafe.Pointer(&v))
			return uint64(memhash(unsafe.Pointer(strh.Data), uintptr(seed64), uintptr(strh.Len)))
		}

	// other comparable fixed-size types
	case reflect.Struct, reflect.Array, reflect.Float32, reflect.Float64, reflect.Complex64, reflect.Complex128:
		seed := maphash.MakeSeed()
		seed64 := *(*uint64)(unsafe.Pointer(&seed))
		return func(v T) uint64 {
			valSize := unsafe.Sizeof(v)
			return uint64(memhash(unsafe.Pointer(&v), uintptr(seed64), valSize))
		}

	// Implementation for interfaces is slow and can throw a panic at runtime.
	// Technically we should never get here because this is a generic function used for
	// generic hash-maps.
	// Maybe it better to disable this case at all?
	case reflect.Interface:
		return func(t T) uint64 {
			var h maphash.Hash
			h.SetSeed(seed)
			err := binary.Write(&h, binary.LittleEndian, t)
			if err != nil {
				panic(err)
			}
			return h.Sum64()
		}

	// This should never happen as well, especially taking into account
	// that T is comparable.
	default:
		panic(fmt.Sprintf("unsupported type %v", rt))

	}
}

//go:noescape
//go:linkname memhash runtime.memhash
func memhash(p unsafe.Pointer, h, s uintptr) uintptr

//go:noescape
//go:linkname fastrand runtime.fastrand
func fastrand() uint32
