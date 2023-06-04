package xsync

import (
	"math/rand"
	"testing"
)

func TestNextPowOf2(t *testing.T) {
	if nextPowOf2(0) != 1 {
		t.Error("nextPowOf2 failed")
	}
	if nextPowOf2(1) != 1 {
		t.Error("nextPowOf2 failed")
	}
	if nextPowOf2(2) != 2 {
		t.Error("nextPowOf2 failed")
	}
	if nextPowOf2(3) != 4 {
		t.Error("nextPowOf2 failed")
	}
}

func TestFastrand(t *testing.T) {
	count := 10000
	set := make(map[uint32]struct{}, count)

	for i := 0; i < count; i++ {
		num := fastrand()
		set[num] = struct{}{}
	}

	if len(set) != count {
		t.Error("duplicated rand num")
	}
}

func BenchmarkFastrand(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = fastrand()
	}
	// about 1.5 ns/op
}

func BenchmarkRand(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = rand.Uint32()
	}
	// about 12 ns/op
}
