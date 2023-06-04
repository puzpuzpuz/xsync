package xsync_test

import (
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
	count := 10000
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
