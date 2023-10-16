//go:build go1.18
// +build go1.18

package xsync

import (
	"fmt"
	"hash/maphash"
	"reflect"
	"runtime"
	"testing"
	"time"
	"unsafe"
)

func TestMakeHashFunc(t *testing.T) {
	// makes a clone of the string that is not stored in the same memory location
	cloneString := func(str string) string {
		buf := make([]byte, 0, len(str))
		buf = append(buf, str...)
		return string(buf)
	}

	type Point struct {
		X, Y int
	}

	doTestMakeHashFuncNeq(t, int32(116), int32(117))
	doTestMakeHashFuncNeq(t, 3.1415, 2.7182)
	doTestMakeHashFuncNeq(t, "foo", "bar")
	doTestMakeHashFuncNeq(t, Point{1, 2}, Point{3, 4})
	doTestMakeHashFuncNeq(t, [3]byte{'f', 'o', 'o'}, [3]byte{'b', 'a', 'r'})

	doTestMakeHashFuncNeq(t, &Point{1, 2}, &Point{1, 2})
	doTestMakeHashFuncNeq(t, nil, &Point{1, 2})
	doTestMakeHashFuncNeq(t, unsafe.Pointer(&Point{1, 2}), unsafe.Pointer(&Point{1, 2}))
	doTestMakeHashFuncNeq(t, make(chan string), make(chan string))

	// ---- Test complex struct cases
	type Timestamps struct {
		CreatedAt, UpdatedAt int64
	}

	type User struct {
		ID       int
		Name     string
		IsActive bool
		Age      int

		InvitedBy *User
		Timestamps
	}

	inviter := User{}
	inviter.ID = 1
	inviter.Name = "John"
	inviter.IsActive = true
	inviter.Age = 30
	inviter.CreatedAt = 116
	inviter.UpdatedAt = 117

	user := User{}
	user.ID = 2
	user.Name = "Jane"
	user.IsActive = true
	user.Age = 25
	user.CreatedAt = 118
	user.UpdatedAt = 119
	user.InvitedBy = &inviter

	userCopy := user

	// explicitly check that hash of the copy is the same
	doTestMakeHashFuncEq(t, user, userCopy)

	// change numeric field
	userCopy.Age = 9000
	doTestMakeHashFuncNeq(t, user, userCopy)
	userCopy = user

	// change string field
	userCopy.Name = "9000"
	doTestMakeHashFuncNeq(t, user, userCopy)
	userCopy = user

	// change embedded struct field
	userCopy.CreatedAt = 9000
	doTestMakeHashFuncNeq(t, user, userCopy)
	userCopy = user

	// change string field pointer (but keep the same string value)
	// hash should remain the same
	userCopy.Name = cloneString(user.Name)
	doTestMakeHashFuncEq(t, user, userCopy)
	userCopy = user

	// change inviter address to it's shallow copy address
	// hash should change
	inviterCopy := inviter
	userCopy.InvitedBy = &inviterCopy
	doTestMakeHashFuncNeq(t, user, userCopy)
	userCopy = user

	// ---- Padded struct
	{
		type Padded struct {
			_     [3]byte
			value int
		}

		seed := maphash.MakeSeed()
		hasherPadded := makeHashFunc[Padded]()
		hasherInt := makeHashFunc[int]()

		if hasherPadded(seed, Padded{value: 25}) != hasherInt(seed, 25) {
			t.Error("hash of padded struct was affected by a padding")
		}

	}

	// ---- Just in case check that the same timestamp, but in different time zones, has different hash
	loc1 := time.FixedZone("UTC-8", -8*60*60)
	loc2 := time.FixedZone("UTC+3", 3*60*60)
	now := time.Now()

	doTestMakeHashFuncNeq(t, now.In(loc1), now.In(loc2))
}

func doTestMakeHashFuncNeq[T comparable](t *testing.T, val1, val2 T) {
	t.Helper()

	if val1 == val2 {
		t.Error("use doTestMakeHashFuncEq for equal values")
		return
	}

	hash := makeHashFunc[T]()
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

	hash2 := makeHashFunc[T]()
	if hash(seed, val1) != hash2(seed, val1) {
		t.Error("two hash functions should behave identically for the same seed and value")
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

func doTestMakeHashFuncEq[T comparable](t *testing.T, val1, val2 T) {
	t.Helper()

	if val1 != val2 {
		t.Error("use doTestMakeHashFuncNeq for non-equal values")
		return
	}

	hash := makeHashFunc[T]()
	seed := maphash.MakeSeed()

	if hash(seed, val1) != hash(seed, val2) {
		t.Error("two invocations of hash for the same value return different results")
	}
}

func TestHMemLayout(t *testing.T) {
	if a := runtime.GOARCH; a != "amd64" && a != "arm64" {
		t.Skipf("Only 64 bit architectures are covered by this test")
	}

	expect := func(val any, expected []hMemBlock) {
		t.Helper()
		actual := hGetMemLayout(reflect.TypeOf(val))

		if len(expected) != len(actual) {
			t.Errorf("expected %v, got %v", expected, actual)
			return
		}

		for i := range expected {
			if expected[i] != actual[i] {
				t.Errorf("expected %v, got %v", expected, actual)
				return
			}
		}
	}

	// ----- basic types

	expect("str", []hMemBlock{
		{0, 16, true},
	})

	expect(10, []hMemBlock{
		{0, 8, false},
	})

	expect(float32(3.14), []hMemBlock{
		{0, 4, false},
	})

	expect(byte(2), []hMemBlock{
		{0, 1, false},
	})

	expect(new(byte), []hMemBlock{
		{0, 8, false},
	})

	expect(make(chan int), []hMemBlock{
		{0, 8, false},
	})

	// ----- structs

	type EmptyStruct struct{}
	expect(EmptyStruct{}, []hMemBlock{})

	type Point struct {
		X, Y, Z int
	}

	expect(Point{}, []hMemBlock{
		{0, 24, false}, // X, Y, Z
	})

	type User struct {
		ID        int
		Name      string
		InvitedBy *User
		IsActive  bool
		Age       int
	}

	expect(User{}, []hMemBlock{
		{0, 8, false},  // ID
		{8, 16, true},  // Name
		{24, 9, false}, // InvitedBy + IsActive
		{40, 8, false}, // (padded) Age
	})

	type Strange struct {
		_   [2]byte
		_   byte
		p   int
		v   string
		_xx byte
	}

	expect(Strange{}, []hMemBlock{
		{8, 8, false},  // p
		{16, 16, true}, // v
		{32, 1, false}, // _xx
	})

	type StrangeUser struct {
		s    Strange
		User // embedded
	}

	expect(StrangeUser{}, []hMemBlock{
		{8, 8, false},  // (padded) s.p
		{16, 16, true}, // s.v
		{32, 1, false}, // s._xx
		{40, 8, false}, // (padded) User.ID
		{48, 16, true}, // User.Name
		{64, 9, false}, // User.InvitedBy + User.IsActive
		{80, 8, false}, // (padded) User.Age
	})

	// ----- arrays

	expect([10]byte{}, []hMemBlock{
		{0, 10, false},
	})

	expect([10]int{}, []hMemBlock{
		{0, 80, false},
	})

	expect([10]Point{}, []hMemBlock{
		{0, 240, false},
	})

	expect([3]string{}, []hMemBlock{
		{0, 16, true},
		{16, 16, true},
		{32, 16, true},
	})

	expect([2]User{}, []hMemBlock{
		{0, 8, false},   // [0]ID
		{8, 16, true},   // [0]Name
		{24, 9, false},  // [0]InvitedBy + [0]IsActive
		{40, 16, false}, // (padded) [0]Age + [1]ID
		{56, 16, true},  // [1]Name
		{72, 9, false},  // [1]InvitedBy + [1]IsActive
		{88, 8, false},  // (padded) [1]Age
	})

	type PadInside struct {
		A int
		B byte
		C int
	}

	expect([3]PadInside{}, []hMemBlock{
		{0, 9, false},   // [0]A + [0]B
		{16, 17, false}, // (padded) [0]C + [1]A + [1]B
		{40, 17, false}, // (padded) [1]C + [2]A + [2]B
		{64, 8, false},  // (padded) [2]C
	})

	type PadTrailing struct {
		A int
		B byte
	}

	expect([3]PadTrailing{}, []hMemBlock{
		{0, 9, false},  // [0]A + [0]B
		{16, 9, false}, // (padded) [1]A + [1]B
		{32, 9, false}, // (padded) [2]A + [2]B
	})
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

	type PadTrailing struct {
		A int
		B byte
	}

	doBenchmarkMakeHashFunc(b, 116)
	doBenchmarkMakeHashFunc(b, 3.14)
	doBenchmarkMakeHashFunc(b, "test key")
	doBenchmarkMakeHashFunc(b, Point{1, 2, 3})
	doBenchmarkMakeHashFunc(b, &Point{1, 2, 3})
	doBenchmarkMakeHashFunc(b, User{ID: 1, FirstName: "John", LastName: "Smith", IsActive: true, City: "New York"})
	doBenchmarkMakeHashFunc(b, [1024]byte{})
	doBenchmarkMakeHashFunc(b, [128]Point{})
	doBenchmarkMakeHashFunc(b, [128]PadTrailing{})
}

func doBenchmarkMakeHashFunc[T comparable](b *testing.B, val T) {
	hash := makeHashFunc[T]()
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

	//nativeHash := maphash2.NewHasher[T]()
	//b.Run(fmt.Sprintf("%T native", val), func(b *testing.B) {
	//	b.ReportAllocs()
	//	for i := 0; i < b.N; i++ {
	//		_ = nativeHash.Hash(val)
	//	}
	//})
}
