package xsync_test

//lint:file-ignore U1000 unused fields are necessary to access the hasher
//lint:file-ignore SA4000 hash code comparisons use identical expressions

import (
	"hash/maphash"
	"testing"

	. "github.com/puzpuzpuz/xsync/v3"
)

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
