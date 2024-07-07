package xsync_test

//lint:file-ignore U1000 unused fields are necessary to access the hasher
//lint:file-ignore SA4000 hash code comparisons use identical expressions

import (
	"fmt"
	"hash/maphash"
	"testing"
	"unsafe"

	. "github.com/puzpuzpuz/xsync/v3"
)

func TestMakeHashFunc(t *testing.T) {
	type User struct {
		Name string
		City string
	}

	seed := MakeSeed()

	hashString := DefaultHasher[string]()
	hashUser := DefaultHasher[User]()

	hashUserMap := makeMapHasher[User]()

	// Not that much to test TBH.

	// check that hash is not always the same
	for i := 0; ; i++ {
		if hashString("foo", seed) != hashString("bar", seed) {
			break
		}
		if i >= 100 {
			t.Error("hashString is always the same")
			break
		}

		seed = MakeSeed() // try with a new seed
	}

	if hashString("foo", seed) != hashString("foo", seed) {
		t.Error("hashString is not deterministic")
	}

	if hashUser(User{Name: "Ivan", City: "Sofia"}, seed) != hashUser(User{Name: "Ivan", City: "Sofia"}, seed) {
		t.Error("hashUser is not deterministic")
	}

	// just for fun, compare with native hash function
	if hashUser(User{Name: "Ivan", City: "Sofia"}, seed) != hashUserMap(User{Name: "Ivan", City: "Sofia"}, seed) {
		t.Error("hashUser and hashUserNative return different values")
	}
}

func BenchmarkMapHashString(b *testing.B) {
	fn := func(seed maphash.Seed, s string) uint64 {
		var h maphash.Hash
		h.SetSeed(seed)
		h.WriteString(s)
		return h.Sum64()
	}
	seed := maphash.MakeSeed()
	for i := 0; i < b.N; i++ {
		_ = fn(seed, benchmarkKeyPrefix)
	}
	// about 13ns/op on x86-64
}

func BenchmarkHashString(b *testing.B) {
	seed := MakeSeed()
	for i := 0; i < b.N; i++ {
		_ = HashString(benchmarkKeyPrefix, seed)
	}
	// about 4ns/op on x86-64
}

func makeMapHasher[T comparable]() func(T, uint64) uint64 {
	hasher := makeMapHasherInternal(make(map[T]struct{}))

	is64Bit := unsafe.Sizeof(uintptr(0)) == 8

	if is64Bit {
		return func(value T, seed uint64) uint64 {
			seed64 := *(*uint64)(unsafe.Pointer(&seed))
			return uint64(hasher(runtime_noescape(unsafe.Pointer(&value)), uintptr(seed64)))
		}
	} else {
		return func(value T, seed uint64) uint64 {
			seed64 := *(*uint64)(unsafe.Pointer(&seed))
			lo := hasher(runtime_noescape(unsafe.Pointer(&value)), uintptr(seed64))
			hi := hasher(runtime_noescape(unsafe.Pointer(&value)), uintptr(seed64>>32))
			return uint64(hi)<<32 | uint64(lo)
		}
	}
}

//go:noescape
//go:linkname runtime_noescape runtime.noescape
func runtime_noescape(p unsafe.Pointer) unsafe.Pointer

type nativeHasher func(unsafe.Pointer, uintptr) uintptr

func makeMapHasherInternal(mapValue any) nativeHasher {
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

func BenchmarkMakeHashFunc(b *testing.B) {
	type Point struct {
		X, Y, Z int
	}

	type User struct {
		ID        int
		FirstName string
		LastName  string
		IsActive  bool
		City      string
	}

	type PadInside struct {
		A int
		B byte
		C int
	}

	type PadTrailing struct {
		A int
		B byte
	}

	doBenchmarkMakeHashFunc(b, int64(116))
	doBenchmarkMakeHashFunc(b, int32(116))
	doBenchmarkMakeHashFunc(b, 3.14)
	doBenchmarkMakeHashFunc(b, "test key test key test key test key test key test key test key test key test key ")
	doBenchmarkMakeHashFunc(b, Point{1, 2, 3})
	doBenchmarkMakeHashFunc(b, User{ID: 1, FirstName: "Ivan", LastName: "Ivanov", IsActive: true, City: "Sofia"})
	doBenchmarkMakeHashFunc(b, PadInside{})
	doBenchmarkMakeHashFunc(b, PadTrailing{})
	doBenchmarkMakeHashFunc(b, [1024]byte{})
	doBenchmarkMakeHashFunc(b, [128]Point{})
	doBenchmarkMakeHashFunc(b, [128]User{})
	doBenchmarkMakeHashFunc(b, [128]PadInside{})
	doBenchmarkMakeHashFunc(b, [128]PadTrailing{})
}

func doBenchmarkMakeHashFunc[T comparable](b *testing.B, val T) {
	hash := DefaultHasher[T]()
	hashNativeMap := makeMapHasher[T]()
	seed := MakeSeed()

	b.Run(fmt.Sprintf("%T normal", val), func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = hash(val, seed)
		}
	})

	b.Run(fmt.Sprintf("%T map native", val), func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = hashNativeMap(val, seed)
		}
	})
}
