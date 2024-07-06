package xsync_test

import (
	"math/rand"
	"strconv"
	"testing"

	. "github.com/puzpuzpuz/xsync/v3"
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

func TestBroadcast(t *testing.T) {
	testCases := []struct {
		input    uint8
		expected uint64
	}{
		{
			input:    0,
			expected: 0,
		},
		{
			input:    1,
			expected: 0x0101010101010101,
		},
		{
			input:    2,
			expected: 0x0202020202020202,
		},
		{
			input:    42,
			expected: 0x2a2a2a2a2a2a2a2a,
		},
		{
			input:    127,
			expected: 0x7f7f7f7f7f7f7f7f,
		},
		{
			input:    255,
			expected: 0xffffffffffffffff,
		},
	}

	for _, tc := range testCases {
		t.Run(strconv.Itoa(int(tc.input)), func(t *testing.T) {
			if Broadcast(tc.input) != tc.expected {
				t.Errorf("unexpected result: %x", Broadcast(tc.input))
			}
		})
	}
}

func TestFirstMarkedByteIndex(t *testing.T) {
	testCases := []struct {
		input    uint64
		expected int
	}{
		{
			input:    0,
			expected: 8,
		},
		{
			input:    0x8080808080808080,
			expected: 0,
		},
		{
			input:    0x0000000000000080,
			expected: 0,
		},
		{
			input:    0x0000000000008000,
			expected: 1,
		},
		{
			input:    0x0000000000800000,
			expected: 2,
		},
		{
			input:    0x0000000080000000,
			expected: 3,
		},
		{
			input:    0x0000008000000000,
			expected: 4,
		},
		{
			input:    0x0000800000000000,
			expected: 5,
		},
		{
			input:    0x0080000000000000,
			expected: 6,
		},
		{
			input:    0x8000000000000000,
			expected: 7,
		},
	}

	for _, tc := range testCases {
		t.Run(strconv.Itoa(int(tc.input)), func(t *testing.T) {
			if FirstMarkedByteIndex(tc.input) != tc.expected {
				t.Errorf("unexpected result: %x", FirstMarkedByteIndex(tc.input))
			}
		})
	}
}

func TestMarkZeroBytes(t *testing.T) {
	testCases := []struct {
		input    uint64
		expected uint64
	}{
		{
			input:    0xffffffffffffffff,
			expected: 0,
		},
		{
			input:    0,
			expected: 0x8080808080808080,
		},
		{
			input:    1,
			expected: 0x8080808080808000,
		},
		{
			input:    1 << 9,
			expected: 0x8080808080800080,
		},
		{
			input:    1 << 17,
			expected: 0x8080808080008080,
		},
		{
			input:    1 << 25,
			expected: 0x8080808000808080,
		},
		{
			input:    1 << 33,
			expected: 0x8080800080808080,
		},
		{
			input:    1 << 41,
			expected: 0x8080008080808080,
		},
		{
			input:    1 << 49,
			expected: 0x8000808080808080,
		},
		{
			input:    1 << 57,
			expected: 0x0080808080808080,
		},
		// false positive
		{
			input:    0x0100,
			expected: 0x8080808080808080,
		},
	}

	for _, tc := range testCases {
		t.Run(strconv.Itoa(int(tc.input)), func(t *testing.T) {
			if MarkZeroBytes(tc.input) != tc.expected {
				t.Errorf("unexpected result: %x", MarkZeroBytes(tc.input))
			}
		})
	}
}

func TestSetByte(t *testing.T) {
	testCases := []struct {
		word     uint64
		b        uint8
		idx      int
		expected uint64
	}{
		{
			word:     0xffffffffffffffff,
			b:        0,
			idx:      0,
			expected: 0xffffffffffffff00,
		},
		{
			word:     0xffffffffffffffff,
			b:        1,
			idx:      1,
			expected: 0xffffffffffff01ff,
		},
		{
			word:     0xffffffffffffffff,
			b:        2,
			idx:      2,
			expected: 0xffffffffff02ffff,
		},
		{
			word:     0xffffffffffffffff,
			b:        3,
			idx:      3,
			expected: 0xffffffff03ffffff,
		},
		{
			word:     0xffffffffffffffff,
			b:        4,
			idx:      4,
			expected: 0xffffff04ffffffff,
		},
		{
			word:     0xffffffffffffffff,
			b:        5,
			idx:      5,
			expected: 0xffff05ffffffffff,
		},
		{
			word:     0xffffffffffffffff,
			b:        6,
			idx:      6,
			expected: 0xff06ffffffffffff,
		},
		{
			word:     0xffffffffffffffff,
			b:        7,
			idx:      7,
			expected: 0x07ffffffffffffff,
		},
		{
			word:     0,
			b:        0xff,
			idx:      7,
			expected: 0xff00000000000000,
		},
	}

	for _, tc := range testCases {
		t.Run(strconv.Itoa(int(tc.word)), func(t *testing.T) {
			if SetByte(tc.word, tc.b, tc.idx) != tc.expected {
				t.Errorf("unexpected result: %x", SetByte(tc.word, tc.b, tc.idx))
			}
		})
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
