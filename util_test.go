package xsync_test

import (
	"hash/maphash"
	"math/rand"
	"testing"

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
