package xsync

import (
	"reflect"
	"unsafe"
)

const (
	// used in paddings to prevent false sharing;
	// 64B are used instead of 128B as a compromise between
	// memory footprint and performance; 128B usage may give ~30%
	// improvement on NUMA machines
	cacheLineSize = 64
)

// murmurhash3 64-bit finalizer
func hash64(x uintptr) uint32 {
	x = ((x >> 33) ^ x) * 0xff51afd7ed558ccd
	x = ((x >> 33) ^ x) * 0xc4ceb9fe1a85ec53
	x = (x >> 33) ^ x
	return uint32(x)
}

//go:noescape
//go:linkname memhash runtime.memhash
func memhash(p unsafe.Pointer, h, s uintptr) uintptr

func memhashMap(msg interface{}) uint64 {
	switch val := msg.(type) {
	case string:
		strhv := *(*reflect.StringHeader)(unsafe.Pointer(&val))

		return uint64(memhash(unsafe.Pointer(strhv.Data), 0, uintptr(strhv.Len)))
	case []byte:
		strhv := *(*reflect.SliceHeader)(unsafe.Pointer(&val))

		return uint64(memhash(unsafe.Pointer(strhv.Data), 0, uintptr(strhv.Len)))
	case int:
		return uint64(memhash(unsafe.Pointer(&val), 0, unsafe.Sizeof(val)))
	case uint:
		return uint64(memhash(unsafe.Pointer(&val), 0, unsafe.Sizeof(val)))
	case uintptr:
		return uint64(memhash(unsafe.Pointer(&val), 0, unsafe.Sizeof(val)))
	case int8:
		return uint64(memhash(unsafe.Pointer(&val), 0, unsafe.Sizeof(val)))
	case uint8:
		return uint64(memhash(unsafe.Pointer(&val), 0, unsafe.Sizeof(val)))
	case int16:
		return uint64(memhash(unsafe.Pointer(&val), 0, unsafe.Sizeof(val)))
	case uint16:
		return uint64(memhash(unsafe.Pointer(&val), 0, unsafe.Sizeof(val)))
	case int32:
		return uint64(memhash(unsafe.Pointer(&val), 0, unsafe.Sizeof(val)))
	case uint32:
		return uint64(memhash(unsafe.Pointer(&val), 0, unsafe.Sizeof(val)))
	case int64:
		return uint64(memhash(unsafe.Pointer(&val), 0, unsafe.Sizeof(val)))
	case uint64:
		return uint64(memhash(unsafe.Pointer(&val), 0, unsafe.Sizeof(val)))
	case float64:
		return uint64(memhash(unsafe.Pointer(&val), 0, unsafe.Sizeof(val)))
	case float32:
		return uint64(memhash(unsafe.Pointer(&val), 0, unsafe.Sizeof(val)))
	case bool:
		return uint64(memhash(unsafe.Pointer(&val), 0, unsafe.Sizeof(val)))
	case complex128:
		return uint64(memhash(unsafe.Pointer(&val), 0, unsafe.Sizeof(val)))
	case complex64:
		return uint64(memhash(unsafe.Pointer(&val), 0, unsafe.Sizeof(val)))
	}

	return uint64(memhash(unsafe.Pointer(&msg), 0, unsafe.Sizeof(msg)))
}
