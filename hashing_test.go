//go:build go1.18
// +build go1.18

package xsync

import (
	"fmt"
	"hash/maphash"
	"testing"
)

func TestMakeHashFunc(t *testing.T) {
	type User struct {
		Name string
		City string
	}

	seed := maphash.MakeSeed()

	hashString := makeHashFunc[string]()
	hashUser := makeHashFunc[User]()
	hashAny := makeHashFunc[any]() // this declaration requires go 1.20+, though makeHashFunc itself can work with go.18+

	hashUserNative := makeHashFuncNative[User]()

	// Not that much to test TBH.

	// check that hash is not always the same
	for i := 0; ; i++ {
		if hashString(seed, "foo") != hashString(seed, "bar") {
			break
		}
		if i >= 100 {
			t.Error("hashString is always the same")
			break
		}

		seed = maphash.MakeSeed() // try with a new seed
	}

	// do the same for hash any
	for i := 0; ; i++ {
		if hashAny(seed, "foo") != hashAny(seed, "bar") {
			break
		}
		if i >= 100 {
			t.Error("hashAny is always the same")
			break
		}

		seed = maphash.MakeSeed() // try with a new seed
	}

	if hashString(seed, "foo") != hashString(seed, "foo") {
		t.Error("hashString is not deterministic")
	}

	if hashUser(seed, User{Name: "John", City: "New York"}) != hashUser(seed, User{Name: "John", City: "New York"}) {
		t.Error("hashUser is not deterministic")
	}

	if hashAny(seed, User{Name: "John", City: "New York"}) != hashAny(seed, User{Name: "John", City: "New York"}) {
		t.Error("hashAny is not deterministic")
	}

	// just for fun, compare with native hash function
	if hashUser(seed, User{Name: "John", City: "New York"}) != hashUserNative(seed, User{Name: "John", City: "New York"}) {
		t.Error("hashUser and hashUserNative return different values")
	}

}

func expectEqualHashes[T comparable](t *testing.T, val1, val2 T) {
	t.Helper()

	if val1 != val2 {
		t.Error("use expectDifferentHashes for non-equal values")
		return
	}

	hash := makeHashFunc[T]()
	seed := maphash.MakeSeed()

	if hash(seed, val1) != hash(seed, val2) {
		t.Error("two invocations of hash for the same value return different results")
	}
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
	doBenchmarkMakeHashFunc(b, User{ID: 1, FirstName: "John", LastName: "Smith", IsActive: true, City: "New York"})
	doBenchmarkMakeHashFunc(b, PadInside{})
	doBenchmarkMakeHashFunc(b, PadTrailing{})
	doBenchmarkMakeHashFunc(b, [1024]byte{})
	doBenchmarkMakeHashFunc(b, [128]Point{})
	doBenchmarkMakeHashFunc(b, [128]User{})
	doBenchmarkMakeHashFunc(b, [128]PadInside{})
	doBenchmarkMakeHashFunc(b, [128]PadTrailing{})
}

func doBenchmarkMakeHashFunc[T comparable](b *testing.B, val T) {
	hash := makeHashFunc[T]()
	hashDry := makeHashFuncDRY[T]()
	hashNative := makeHashFuncNative[T]()
	seed := maphash.MakeSeed()

	b.Run(fmt.Sprintf("%T normal", val), func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = hash(seed, val)
		}
	})

	b.Run(fmt.Sprintf("%T dry", val), func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = hashDry(seed, val)
		}
	})

	b.Run(fmt.Sprintf("%T native", val), func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = hashNative(seed, val)
		}
	})
}
