//go:build go1.18
// +build go1.18

package xsync

import (
	"hash/maphash"
	"reflect"
	"unsafe"
)

// makeHashFunc creates a fast hash function for the given comparable type.
// The only limitation is that the type should not contain interfaces inside
// based on runtime.typehash.
func makeHashFunc[T comparable]() func(maphash.Seed, T) uint64 {
	var zero T

	isInterface := reflect.TypeOf(&zero).Elem().Kind() == reflect.Interface
	is64Bit := unsafe.Sizeof(uintptr(0)) == 8

	if isInterface {
		if is64Bit {
			return func(seed maphash.Seed, value T) uint64 {
				seed64 := *(*uint64)(unsafe.Pointer(&seed))
				iValue := any(value)
				i := (*iface)(unsafe.Pointer(&iValue))
				return uint64(runtime_typehash(i.typ, noescape(i.word), uintptr(seed64)))
			}
		} else {
			return func(seed maphash.Seed, value T) uint64 {
				seed64 := *(*uint64)(unsafe.Pointer(&seed))
				iValue := any(value)
				i := (*iface)(unsafe.Pointer(&iValue))

				lo := runtime_typehash(i.typ, noescape(i.word), uintptr(seed64))
				hi := runtime_typehash(i.typ, noescape(i.word), uintptr(seed64>>32))
				return uint64(hi)<<32 | uint64(lo)
			}
		}
	} else {
		var iZero any = zero
		i := (*iface)(unsafe.Pointer(&iZero))

		if is64Bit {
			return func(seed maphash.Seed, value T) uint64 {
				seed64 := *(*uint64)(unsafe.Pointer(&seed))
				return uint64(runtime_typehash(i.typ, noescape(unsafe.Pointer(&value)), uintptr(seed64)))
			}
		} else {
			return func(seed maphash.Seed, value T) uint64 {
				seed64 := *(*uint64)(unsafe.Pointer(&seed))

				lo := runtime_typehash(i.typ, noescape(unsafe.Pointer(&value)), uintptr(seed64))
				hi := runtime_typehash(i.typ, noescape(unsafe.Pointer(&value)), uintptr(seed64>>32))
				return uint64(hi)<<32 | uint64(lo)
			}
		}
	}
}

// DRY version of makeHashFunc
func makeHashFuncDRY[T comparable]() func(maphash.Seed, T) uint64 {
	var zero T

	if reflect.TypeOf(&zero).Elem().Kind() == reflect.Interface {
		return func(seed maphash.Seed, value T) uint64 {
			iValue := any(value)
			i := (*iface)(unsafe.Pointer(&iValue))
			return runtime_typehash64(i.typ, noescape(i.word), seed)
		}
	} else {
		var iZero any = zero
		i := (*iface)(unsafe.Pointer(&iZero))
		return func(seed maphash.Seed, value T) uint64 {
			return runtime_typehash64(i.typ, noescape(unsafe.Pointer(&value)), seed)
		}
	}
}

func makeHashFuncNative[T comparable]() func(maphash.Seed, T) uint64 {
	hasher := makeHashFuncNativeInternal(make(map[T]struct{}))

	is64Bit := unsafe.Sizeof(uintptr(0)) == 8

	if is64Bit {
		return func(seed maphash.Seed, value T) uint64 {
			seed64 := *(*uint64)(unsafe.Pointer(&seed))
			return uint64(hasher(noescape(unsafe.Pointer(&value)), uintptr(seed64)))
		}
	} else {
		return func(seed maphash.Seed, value T) uint64 {
			seed64 := *(*uint64)(unsafe.Pointer(&seed))
			lo := hasher(noescape(unsafe.Pointer(&value)), uintptr(seed64))
			hi := hasher(noescape(unsafe.Pointer(&value)), uintptr(seed64>>32))
			return uint64(hi)<<32 | uint64(lo)
		}
	}

}

type nativeHasher func(unsafe.Pointer, uintptr) uintptr

func makeHashFuncNativeInternal(mapValue any) nativeHasher {
	// go/src/runtime/type.go
	type tflag uint8
	type nameOff int32
	type typeOff int32

	// go/src/runtime/type.go
	type _type struct {
		size       uintptr
		ptrdata    uintptr
		hash       uint32
		tflag      tflag
		align      uint8
		fieldAlign uint8
		kind       uint8
		equal      func(unsafe.Pointer, unsafe.Pointer) bool
		gcdata     *byte
		str        nameOff
		ptrToThis  typeOff
	}

	// go/src/runtime/type.go
	type maptype struct {
		typ    _type
		key    *_type
		elem   *_type
		bucket *_type
		// function for hashing keys (ptr to key, seed) -> hash
		hasher     nativeHasher
		keysize    uint8
		elemsize   uint8
		bucketsize uint16
		flags      uint32
	}

	type mapiface struct {
		typ *maptype
		val uintptr
	}

	i := (*mapiface)(unsafe.Pointer(&mapValue))
	return i.typ.hasher
}

// how interface is represented in memory
type iface struct {
	typ  uintptr
	word unsafe.Pointer
}

// same as runtime_typehash, but always returns a uint64
// see: maphash.rthash function for details
func runtime_typehash64(t uintptr, p unsafe.Pointer, seed maphash.Seed) uint64 {
	seed64 := *(*uint64)(unsafe.Pointer(&seed))
	if unsafe.Sizeof(uintptr(0)) == 8 {
		return uint64(runtime_typehash(t, noescape(p), uintptr(seed64)))
	}

	lo := runtime_typehash(t, noescape(p), uintptr(seed64))
	hi := runtime_typehash(t, noescape(p), uintptr(seed64>>32))
	return uint64(hi)<<32 | uint64(lo)
}

//go:nosplit
//go:nocheckptr
func noescape(p unsafe.Pointer) unsafe.Pointer {
	x := uintptr(p)
	return unsafe.Pointer(x ^ 0)
}

//go:linkname runtime_typehash runtime.typehash
func runtime_typehash(t uintptr, p unsafe.Pointer, h uintptr) uintptr
