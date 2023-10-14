package xsync_test

import (
	"fmt"
	"hash/maphash"
	"math/rand"
	"testing"
	"unsafe"

	. "github.com/puzpuzpuz/xsync/v2"
)

func TestNextPowOf2(t *testing.T) {
	if NextPowOf2(0) != 1 {
		t.Error("nextPowOf2 failed")
	}
	if NextPowOf2(1) != 1 {
		t.Error("nextPowOf2 failed")
	}
	if NextPowOf2(2) != 2 {
		t.Error("nextPowOf2 failed")
	}
	if NextPowOf2(3) != 4 {
		t.Error("nextPowOf2 failed")
	}
}

func doTestMakeHashFunc[T comparable](t *testing.T, val1, val2 T) {
	t.Helper()

	if val1 == val2 {
		t.Error("use two different values for the test")
	}

	hash := MakeHashFunc[T]()
	seed := maphash.MakeSeed()

	val1copy := val1
	if hash(seed, val1) != hash(seed, val1) || hash(seed, val1copy) != hash(seed, val1copy) {
		t.Error("two invocations of hash for the same value return different results")
	}

	// double check
	val2copy := val2
	if hash(seed, val2) != hash(seed, val2) || hash(seed, val2copy) != hash(seed, val2copy) {
		t.Error("two invocations of hash for the same value return different results")
	}

	hash2 := MakeHashFunc[T]()
	if hash(seed, val1) != hash2(seed, val1) {
		t.Error("two hash functions but behave identically for the same seed and value")
	}

	// Test that different values have different hashes.
	// That's not always the case, so we'll try different seeds,
	// to make probability of failure virtually zero
	for i := 0; ; i++ {
		if hash(seed, val1) != hash(seed, val2) {
			break
		}

		if i >= 20 {
			t.Error("Different values have the same hash")
			break
		}

		t.Log("Different values have the same hash, trying different seed")
		seed = maphash.MakeSeed()
	}
}

func TestMakeHashFunc(t *testing.T) {
	type Point struct {
		X, Y int
	}

	doTestMakeHashFunc(t, int32(116), int32(117))
	doTestMakeHashFunc(t, 3.1415, 2.7182)
	doTestMakeHashFunc(t, "foo", "bar")
	doTestMakeHashFunc(t, Point{1, 2}, Point{3, 4})
	doTestMakeHashFunc(t, [3]byte{'f', 'o', 'o'}, [3]byte{'b', 'a', 'r'})

	doTestMakeHashFunc(t, &Point{1, 2}, &Point{1, 2})
	doTestMakeHashFunc(t, nil, &Point{1, 2})
	doTestMakeHashFunc(t, unsafe.Pointer(&Point{1, 2}), unsafe.Pointer(&Point{1, 2}))
	doTestMakeHashFunc(t, make(chan string), make(chan string))

	// works only in go 1.20+
	//var a any = Point{1, 2}
	//var b any = Point{3, 4}
	//doTestMakeHashFunc(t, a, b)
}

// This test is here to catch potential problems
// with fastrand-related changes.
func TestFastrand(t *testing.T) {
	count := 100
	set := make(map[uint32]struct{}, count)

	for i := 0; i < count; i++ {
		num := Fastrand()
		set[num] = struct{}{}
	}

	if len(set) != count {
		t.Error("duplicated rand num")
	}
}

func BenchmarkFastrand(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = Fastrand()
	}
	// about 1.4 ns/op on x86-64
}

func BenchmarkRand(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = rand.Uint32()
	}
	// about 12 ns/op on x86-64
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
	seed := maphash.MakeSeed()
	for i := 0; i < b.N; i++ {
		_ = HashString(seed, benchmarkKeyPrefix)
	}
	// about 4ns/op on x86-64
}

func doBenchmarkMakeHashFunc[T comparable](b *testing.B, val T) {
	hash := MakeHashFunc[T]()
	seed := maphash.MakeSeed()

	b.Run(fmt.Sprintf("%T hash", val), func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = hash(seed, val)
		}
	})

	// Since hashing is a function call, let's wrap map read into a function call as well.
	// Seems this should give better comparison of the performance of the hash function itself.
	m := map[T]int{val: 10}
	readMap := func() int {
		return m[val]
	}

	b.Run(fmt.Sprintf("%T map read", val), func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = readMap()
		}
	})
}

func BenchmarkMakeHashFunc(b *testing.B) {
	type Point struct {
		X, Y, Z int
	}

	doBenchmarkMakeHashFunc(b, 116)
	doBenchmarkMakeHashFunc(b, "test key")
	doBenchmarkMakeHashFunc(b, Point{1, 2, 3})
	doBenchmarkMakeHashFunc(b, &Point{1, 2, 3})

}
