package xsync_test

import (
	"math"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	. "github.com/puzpuzpuz/xsync/v3"
)

func TestCowCowMapOf_CowBucketOfStructSize(t *testing.T) {
	size := unsafe.Sizeof(CowBucketOfPadded{})
	if size != 64 {
		t.Fatalf("size of 64B (one cache line) is expected, got: %d", size)
	}
}

func TestCowMapOf_MissingEntry(t *testing.T) {
	m := NewCowMapOf[string, string]()
	v, ok := m.Load("foo")
	if ok {
		t.Fatalf("value was not expected: %v", v)
	}
	if deleted, loaded := m.LoadAndDelete("foo"); loaded {
		t.Fatalf("value was not expected %v", deleted)
	}
	if actual, loaded := m.LoadOrStore("foo", "bar"); loaded {
		t.Fatalf("value was not expected %v", actual)
	}
}

func TestCowMapOf_EmptyStringKey(t *testing.T) {
	m := NewCowMapOf[string, string]()
	m.Store("", "foobar")
	v, ok := m.Load("")
	if !ok {
		t.Fatal("value was expected")
	}
	if v != "foobar" {
		t.Fatalf("value does not match: %v", v)
	}
}

func TestCowMapOfStore_NilValue(t *testing.T) {
	m := NewCowMapOf[string, *struct{}]()
	m.Store("foo", nil)
	v, ok := m.Load("foo")
	if !ok {
		t.Fatal("nil value was expected")
	}
	if v != nil {
		t.Fatalf("value was not nil: %v", v)
	}
}

func TestCowMapOfLoadOrStore_NilValue(t *testing.T) {
	m := NewCowMapOf[string, *struct{}]()
	m.LoadOrStore("foo", nil)
	v, loaded := m.LoadOrStore("foo", nil)
	if !loaded {
		t.Fatal("nil value was expected")
	}
	if v != nil {
		t.Fatalf("value was not nil: %v", v)
	}
}

func TestCowMapOfLoadOrStore_NonNilValue(t *testing.T) {
	type foo struct{}
	m := NewCowMapOf[string, *foo]()
	newv := &foo{}
	v, loaded := m.LoadOrStore("foo", newv)
	if loaded {
		t.Fatal("no value was expected")
	}
	if v != newv {
		t.Fatalf("value does not match: %v", v)
	}
	newv2 := &foo{}
	v, loaded = m.LoadOrStore("foo", newv2)
	if !loaded {
		t.Fatal("value was expected")
	}
	if v != newv {
		t.Fatalf("value does not match: %v", v)
	}
}

func TestCowMapOfLoadAndStore_NilValue(t *testing.T) {
	m := NewCowMapOf[string, *struct{}]()
	m.LoadAndStore("foo", nil)
	v, loaded := m.LoadAndStore("foo", nil)
	if !loaded {
		t.Fatal("nil value was expected")
	}
	if v != nil {
		t.Fatalf("value was not nil: %v", v)
	}
	v, loaded = m.Load("foo")
	if !loaded {
		t.Fatal("nil value was expected")
	}
	if v != nil {
		t.Fatalf("value was not nil: %v", v)
	}
}

func TestCowMapOfLoadAndStore_NonNilValue(t *testing.T) {
	m := NewCowMapOf[string, int]()
	v1 := 1
	v, loaded := m.LoadAndStore("foo", v1)
	if loaded {
		t.Fatal("no value was expected")
	}
	if v != v1 {
		t.Fatalf("value does not match: %v", v)
	}
	v2 := 2
	v, loaded = m.LoadAndStore("foo", v2)
	if !loaded {
		t.Fatal("value was expected")
	}
	if v != v1 {
		t.Fatalf("value does not match: %v", v)
	}
	v, loaded = m.Load("foo")
	if !loaded {
		t.Fatal("value was expected")
	}
	if v != v2 {
		t.Fatalf("value does not match: %v", v)
	}
}

func TestCowMapOfRange(t *testing.T) {
	const numEntries = 1000
	m := NewCowMapOf[string, int]()
	for i := 0; i < numEntries; i++ {
		m.Store(strconv.Itoa(i), i)
	}
	iters := 0
	met := make(map[string]int)
	m.Range(func(key string, value int) bool {
		if key != strconv.Itoa(value) {
			t.Fatalf("got unexpected key/value for iteration %d: %v/%v", iters, key, value)
			return false
		}
		met[key] += 1
		iters++
		return true
	})
	if iters != numEntries {
		t.Fatalf("got unexpected number of iterations: %d", iters)
	}
	for i := 0; i < numEntries; i++ {
		if c := met[strconv.Itoa(i)]; c != 1 {
			t.Fatalf("range did not iterate correctly over %d: %d", i, c)
		}
	}
}

func TestCowMapOfRange_FalseReturned(t *testing.T) {
	m := NewCowMapOf[string, int]()
	for i := 0; i < 100; i++ {
		m.Store(strconv.Itoa(i), i)
	}
	iters := 0
	m.Range(func(key string, value int) bool {
		iters++
		return iters != 13
	})
	if iters != 13 {
		t.Fatalf("got unexpected number of iterations: %d", iters)
	}
}

func TestCowMapOfRange_NestedDelete(t *testing.T) {
	const numEntries = 256
	m := NewCowMapOf[string, int]()
	for i := 0; i < numEntries; i++ {
		m.Store(strconv.Itoa(i), i)
	}
	m.Range(func(key string, value int) bool {
		m.Delete(key)
		return true
	})
	for i := 0; i < numEntries; i++ {
		if _, ok := m.Load(strconv.Itoa(i)); ok {
			t.Fatalf("value found for %d", i)
		}
	}
}

func TestCowMapOfStringStore(t *testing.T) {
	const numEntries = 128
	m := NewCowMapOf[string, int]()
	for i := 0; i < numEntries; i++ {
		m.Store(strconv.Itoa(i), i)
	}
	for i := 0; i < numEntries; i++ {
		v, ok := m.Load(strconv.Itoa(i))
		if !ok {
			t.Fatalf("value not found for %d", i)
		}
		if v != i {
			t.Fatalf("values do not match for %d: %v", i, v)
		}
	}
}

func TestCowMapOfIntStore(t *testing.T) {
	const numEntries = 128
	m := NewCowMapOf[int, int]()
	for i := 0; i < numEntries; i++ {
		m.Store(i, i)
	}
	for i := 0; i < numEntries; i++ {
		v, ok := m.Load(i)
		if !ok {
			t.Fatalf("value not found for %d", i)
		}
		if v != i {
			t.Fatalf("values do not match for %d: %v", i, v)
		}
	}
}

func TestCowMapOfStore_StructKeys_IntValues(t *testing.T) {
	const numEntries = 128
	m := NewCowMapOf[point, int]()
	for i := 0; i < numEntries; i++ {
		m.Store(point{int32(i), -int32(i)}, i)
	}
	for i := 0; i < numEntries; i++ {
		v, ok := m.Load(point{int32(i), -int32(i)})
		if !ok {
			t.Fatalf("value not found for %d", i)
		}
		if v != i {
			t.Fatalf("values do not match for %d: %v", i, v)
		}
	}
}

func TestCowMapOfStore_StructKeys_StructValues(t *testing.T) {
	const numEntries = 128
	m := NewCowMapOf[point, point]()
	for i := 0; i < numEntries; i++ {
		m.Store(point{int32(i), -int32(i)}, point{-int32(i), int32(i)})
	}
	for i := 0; i < numEntries; i++ {
		v, ok := m.Load(point{int32(i), -int32(i)})
		if !ok {
			t.Fatalf("value not found for %d", i)
		}
		if v.x != -int32(i) {
			t.Fatalf("x value does not match for %d: %v", i, v)
		}
		if v.y != int32(i) {
			t.Fatalf("y value does not match for %d: %v", i, v)
		}
	}
}

func TestCowMapOfWithHasher(t *testing.T) {
	const numEntries = 10000
	m := NewCowMapOfWithHasher[int, int](murmur3Finalizer)
	for i := 0; i < numEntries; i++ {
		m.Store(i, i)
	}
	for i := 0; i < numEntries; i++ {
		v, ok := m.Load(i)
		if !ok {
			t.Fatalf("value not found for %d", i)
		}
		if v != i {
			t.Fatalf("values do not match for %d: %v", i, v)
		}
	}
}

func TestCowMapOfWithHasher_HashCodeCollisions(t *testing.T) {
	const numEntries = 1000
	m := NewCowMapOfWithHasher[int, int](func(i int, _ uint64) uint64 {
		// We intentionally use an awful hash function here to make sure
		// that the map copes with key collisions.
		return 42
	}, WithPresize(numEntries))
	for i := 0; i < numEntries; i++ {
		m.Store(i, i)
	}
	for i := 0; i < numEntries; i++ {
		v, ok := m.Load(i)
		if !ok {
			t.Fatalf("value not found for %d", i)
		}
		if v != i {
			t.Fatalf("values do not match for %d: %v", i, v)
		}
	}
}

func TestCowMapOfLoadOrStore(t *testing.T) {
	const numEntries = 1000
	m := NewCowMapOf[string, int]()
	for i := 0; i < numEntries; i++ {
		m.Store(strconv.Itoa(i), i)
	}
	for i := 0; i < numEntries; i++ {
		if _, loaded := m.LoadOrStore(strconv.Itoa(i), i); !loaded {
			t.Fatalf("value not found for %d", i)
		}
	}
}

func TestCowMapOfLoadOrCompute(t *testing.T) {
	const numEntries = 1000
	m := NewCowMapOf[string, int]()
	for i := 0; i < numEntries; i++ {
		v, loaded := m.LoadOrCompute(strconv.Itoa(i), func() int {
			return i
		})
		if loaded {
			t.Fatalf("value not computed for %d", i)
		}
		if v != i {
			t.Fatalf("values do not match for %d: %v", i, v)
		}
	}
	for i := 0; i < numEntries; i++ {
		v, loaded := m.LoadOrCompute(strconv.Itoa(i), func() int {
			return i
		})
		if !loaded {
			t.Fatalf("value not loaded for %d", i)
		}
		if v != i {
			t.Fatalf("values do not match for %d: %v", i, v)
		}
	}
}

func TestCowMapOfLoadOrCompute_FunctionCalledOnce(t *testing.T) {
	m := NewCowMapOf[int, int]()
	for i := 0; i < 100; {
		m.LoadOrCompute(i, func() (v int) {
			v, i = i, i+1
			return v
		})
	}
	m.Range(func(k, v int) bool {
		if k != v {
			t.Fatalf("%dth key is not equal to value %d", k, v)
		}
		return true
	})
}

func TestCowMapOfLoadOrTryCompute(t *testing.T) {
	const numEntries = 1000
	m := NewCowMapOf[string, int]()
	for i := 0; i < numEntries; i++ {
		v, loaded := m.LoadOrTryCompute(strconv.Itoa(i), func() (newValue int, cancel bool) {
			return i, true
		})
		if loaded {
			t.Fatalf("value not computed for %d", i)
		}
		if v != 0 {
			t.Fatalf("values do not match for %d: %v", i, v)
		}
	}
	if m.Size() != 0 {
		t.Fatalf("zero map size expected: %d", m.Size())
	}
	for i := 0; i < numEntries; i++ {
		v, loaded := m.LoadOrTryCompute(strconv.Itoa(i), func() (newValue int, cancel bool) {
			return i, false
		})
		if loaded {
			t.Fatalf("value not computed for %d", i)
		}
		if v != i {
			t.Fatalf("values do not match for %d: %v", i, v)
		}
	}
	for i := 0; i < numEntries; i++ {
		v, loaded := m.LoadOrTryCompute(strconv.Itoa(i), func() (newValue int, cancel bool) {
			return i, false
		})
		if !loaded {
			t.Fatalf("value not loaded for %d", i)
		}
		if v != i {
			t.Fatalf("values do not match for %d: %v", i, v)
		}
	}
}

func TestCowMapOfLoadOrTryCompute_FunctionCalledOnce(t *testing.T) {
	m := NewCowMapOf[int, int]()
	for i := 0; i < 100; {
		m.LoadOrTryCompute(i, func() (newValue int, cancel bool) {
			newValue, i = i, i+1
			return newValue, false
		})
	}
	m.Range(func(k, v int) bool {
		if k != v {
			t.Fatalf("%dth key is not equal to value %d", k, v)
		}
		return true
	})
}

func TestCowMapOfCompute(t *testing.T) {
	m := NewCowMapOf[string, int]()
	// Store a new value.
	v, ok := m.Compute("foobar", func(oldValue int, loaded bool) (newValue int, delete bool) {
		if oldValue != 0 {
			t.Fatalf("oldValue should be 0 when computing a new value: %d", oldValue)
		}
		if loaded {
			t.Fatal("loaded should be false when computing a new value")
		}
		newValue = 42
		delete = false
		return
	})
	if v != 42 {
		t.Fatalf("v should be 42 when computing a new value: %d", v)
	}
	if !ok {
		t.Fatal("ok should be true when computing a new value")
	}
	// Update an existing value.
	v, ok = m.Compute("foobar", func(oldValue int, loaded bool) (newValue int, delete bool) {
		if oldValue != 42 {
			t.Fatalf("oldValue should be 42 when updating the value: %d", oldValue)
		}
		if !loaded {
			t.Fatal("loaded should be true when updating the value")
		}
		newValue = oldValue + 42
		delete = false
		return
	})
	if v != 84 {
		t.Fatalf("v should be 84 when updating the value: %d", v)
	}
	if !ok {
		t.Fatal("ok should be true when updating the value")
	}
	// Delete an existing value.
	v, ok = m.Compute("foobar", func(oldValue int, loaded bool) (newValue int, delete bool) {
		if oldValue != 84 {
			t.Fatalf("oldValue should be 84 when deleting the value: %d", oldValue)
		}
		if !loaded {
			t.Fatal("loaded should be true when deleting the value")
		}
		delete = true
		return
	})
	if v != 84 {
		t.Fatalf("v should be 84 when deleting the value: %d", v)
	}
	if ok {
		t.Fatal("ok should be false when deleting the value")
	}
	// Try to delete a non-existing value. Notice different key.
	v, ok = m.Compute("barbaz", func(oldValue int, loaded bool) (newValue int, delete bool) {
		if oldValue != 0 {
			t.Fatalf("oldValue should be 0 when trying to delete a non-existing value: %d", oldValue)
		}
		if loaded {
			t.Fatal("loaded should be false when trying to delete a non-existing value")
		}
		// We're returning a non-zero value, but the map should ignore it.
		newValue = 42
		delete = true
		return
	})
	if v != 0 {
		t.Fatalf("v should be 0 when trying to delete a non-existing value: %d", v)
	}
	if ok {
		t.Fatal("ok should be false when trying to delete a non-existing value")
	}
}

func TestCowMapOfStringStoreThenDelete(t *testing.T) {
	const numEntries = 1000
	m := NewCowMapOf[string, int]()
	for i := 0; i < numEntries; i++ {
		m.Store(strconv.Itoa(i), i)
	}
	for i := 0; i < numEntries; i++ {
		m.Delete(strconv.Itoa(i))
		if _, ok := m.Load(strconv.Itoa(i)); ok {
			t.Fatalf("value was not expected for %d", i)
		}
	}
}

func TestCowMapOfIntStoreThenDelete(t *testing.T) {
	const numEntries = 1000
	m := NewCowMapOf[int32, int32]()
	for i := 0; i < numEntries; i++ {
		m.Store(int32(i), int32(i))
	}
	for i := 0; i < numEntries; i++ {
		m.Delete(int32(i))
		if _, ok := m.Load(int32(i)); ok {
			t.Fatalf("value was not expected for %d", i)
		}
	}
}

func TestCowMapOfStructStoreThenDelete(t *testing.T) {
	const numEntries = 1000
	m := NewCowMapOf[point, string]()
	for i := 0; i < numEntries; i++ {
		m.Store(point{int32(i), 42}, strconv.Itoa(i))
	}
	for i := 0; i < numEntries; i++ {
		m.Delete(point{int32(i), 42})
		if _, ok := m.Load(point{int32(i), 42}); ok {
			t.Fatalf("value was not expected for %d", i)
		}
	}
}

func TestCowMapOfStringStoreThenLoadAndDelete(t *testing.T) {
	const numEntries = 1000
	m := NewCowMapOf[string, int]()
	for i := 0; i < numEntries; i++ {
		m.Store(strconv.Itoa(i), i)
	}
	for i := 0; i < numEntries; i++ {
		if v, loaded := m.LoadAndDelete(strconv.Itoa(i)); !loaded || v != i {
			t.Fatalf("value was not found or different for %d: %v", i, v)
		}
		if _, ok := m.Load(strconv.Itoa(i)); ok {
			t.Fatalf("value was not expected for %d", i)
		}
	}
}

func TestCowMapOfIntStoreThenLoadAndDelete(t *testing.T) {
	const numEntries = 1000
	m := NewCowMapOf[int, int]()
	for i := 0; i < numEntries; i++ {
		m.Store(i, i)
	}
	for i := 0; i < numEntries; i++ {
		if _, loaded := m.LoadAndDelete(i); !loaded {
			t.Fatalf("value was not found for %d", i)
		}
		if _, ok := m.Load(i); ok {
			t.Fatalf("value was not expected for %d", i)
		}
	}
}

func TestCowMapOfStructStoreThenLoadAndDelete(t *testing.T) {
	const numEntries = 1000
	m := NewCowMapOf[point, int]()
	for i := 0; i < numEntries; i++ {
		m.Store(point{42, int32(i)}, i)
	}
	for i := 0; i < numEntries; i++ {
		if _, loaded := m.LoadAndDelete(point{42, int32(i)}); !loaded {
			t.Fatalf("value was not found for %d", i)
		}
		if _, ok := m.Load(point{42, int32(i)}); ok {
			t.Fatalf("value was not expected for %d", i)
		}
	}
}

func TestCowMapOfStoreThenParallelDelete_DoesNotShrinkBelowMinTableLen(t *testing.T) {
	const numEntries = 1000
	m := NewCowMapOf[int, int]()
	for i := 0; i < numEntries; i++ {
		m.Store(i, i)
	}

	cdone := make(chan bool)
	go func() {
		for i := 0; i < numEntries; i++ {
			m.Delete(i)
		}
		cdone <- true
	}()
	go func() {
		for i := 0; i < numEntries; i++ {
			m.Delete(i)
		}
		cdone <- true
	}()

	// Wait for the goroutines to finish.
	<-cdone
	<-cdone

	stats := m.Stats()
	if stats.RootBuckets != DefaultMinMapTableLen {
		t.Fatalf("table length was different from the minimum: %d", stats.RootBuckets)
	}
}

func sizeBasedOnCowTypedRange(m *CowMapOf[string, int]) int {
	size := 0
	m.Range(func(key string, value int) bool {
		size++
		return true
	})
	return size
}

func TestCowMapOfSize(t *testing.T) {
	const numEntries = 1000
	m := NewCowMapOf[string, int]()
	size := m.Size()
	if size != 0 {
		t.Fatalf("zero size expected: %d", size)
	}
	expectedSize := 0
	for i := 0; i < numEntries; i++ {
		m.Store(strconv.Itoa(i), i)
		expectedSize++
		size := m.Size()
		if size != expectedSize {
			t.Fatalf("size of %d was expected, got: %d", expectedSize, size)
		}
		rsize := sizeBasedOnCowTypedRange(m)
		if size != rsize {
			t.Fatalf("size does not match number of entries in Range: %v, %v", size, rsize)
		}
	}
	for i := 0; i < numEntries; i++ {
		m.Delete(strconv.Itoa(i))
		expectedSize--
		size := m.Size()
		if size != expectedSize {
			t.Fatalf("size of %d was expected, got: %d", expectedSize, size)
		}
		rsize := sizeBasedOnCowTypedRange(m)
		if size != rsize {
			t.Fatalf("size does not match number of entries in Range: %v, %v", size, rsize)
		}
	}
}

func TestCowMapOfClear(t *testing.T) {
	const numEntries = 1000
	m := NewCowMapOf[string, int]()
	for i := 0; i < numEntries; i++ {
		m.Store(strconv.Itoa(i), i)
	}
	size := m.Size()
	if size != numEntries {
		t.Fatalf("size of %d was expected, got: %d", numEntries, size)
	}
	m.Clear()
	size = m.Size()
	if size != 0 {
		t.Fatalf("zero size was expected, got: %d", size)
	}
	rsize := sizeBasedOnCowTypedRange(m)
	if rsize != 0 {
		t.Fatalf("zero number of entries in Range was expected, got: %d", rsize)
	}
}

func assertCowMapOfCapacity[K comparable, V any](t *testing.T, m *CowMapOf[K, V], expectedCap int) {
	stats := m.Stats()
	if stats.Capacity != expectedCap {
		t.Fatalf("capacity was different from %d: %d", expectedCap, stats.Capacity)
	}
}

func TestNewCowMapOfWithPresize(t *testing.T) {
	assertCowMapOfCapacity(t, NewCowMapOf[string, string](), DefaultMinCowMapOfTableCap)
	assertCowMapOfCapacity(t, NewCowMapOf[string, string](WithPresize(0)), DefaultMinCowMapOfTableCap)
	assertCowMapOfCapacity(t, NewCowMapOf[string, string](WithPresize(-100)), DefaultMinCowMapOfTableCap)
	assertCowMapOfCapacity(t, NewCowMapOf[string, string](WithPresize(500)), 1024)
	assertCowMapOfCapacity(t, NewCowMapOf[int, int](WithPresize(1_000_000)), 2097152)
	assertCowMapOfCapacity(t, NewCowMapOf[point, point](WithPresize(100)), 128)
}

func TestNewCowMapOfPresized_DoesNotShrinkBelowMinTableLen(t *testing.T) {
	const minTableLen = 1024
	const numEntries = int(minTableLen * EntriesPerCowMapOfBucket * MapLoadFactor)
	m := NewCowMapOf[int, int](WithPresize(numEntries))
	for i := 0; i < 2*numEntries; i++ {
		m.Store(i, i)
	}

	stats := m.Stats()
	if stats.RootBuckets <= minTableLen {
		t.Fatalf("table did not grow: %d", stats.RootBuckets)
	}

	for i := 0; i < 2*numEntries; i++ {
		m.Delete(i)
	}

	stats = m.Stats()
	if stats.RootBuckets != minTableLen {
		t.Fatalf("table length was different from the minimum: %d", stats.RootBuckets)
	}
}

func TestNewCowMapOfGrowOnly_OnlyShrinksOnClear(t *testing.T) {
	const minTableLen = 128
	const numEntries = minTableLen * EntriesPerCowMapOfBucket
	m := NewCowMapOf[int, int](WithPresize(numEntries), WithGrowOnly())

	stats := m.Stats()
	initialTableLen := stats.RootBuckets

	for i := 0; i < 2*numEntries; i++ {
		m.Store(i, i)
	}
	stats = m.Stats()
	maxTableLen := stats.RootBuckets
	if maxTableLen <= minTableLen {
		t.Fatalf("table did not grow: %d", maxTableLen)
	}

	for i := 0; i < numEntries; i++ {
		m.Delete(i)
	}
	stats = m.Stats()
	if stats.RootBuckets != maxTableLen {
		t.Fatalf("table length was different from the expected: %d", stats.RootBuckets)
	}

	m.Clear()
	stats = m.Stats()
	if stats.RootBuckets != initialTableLen {
		t.Fatalf("table length was different from the initial: %d", stats.RootBuckets)
	}
}

func TestCowMapOfResize(t *testing.T) {
	const numEntries = 100_000
	m := NewCowMapOf[string, int]()

	for i := 0; i < numEntries; i++ {
		m.Store(strconv.Itoa(i), i)
	}
	stats := m.Stats()
	if stats.Size != numEntries {
		t.Fatalf("size was too small: %d", stats.Size)
	}
	expectedCapacity := int(math.RoundToEven(MapLoadFactor+1)) * stats.RootBuckets * EntriesPerCowMapOfBucket
	if stats.Capacity > expectedCapacity {
		t.Fatalf("capacity was too large: %d, expected: %d", stats.Capacity, expectedCapacity)
	}
	if stats.RootBuckets <= DefaultMinMapTableLen {
		t.Fatalf("table was too small: %d", stats.RootBuckets)
	}
	if stats.TotalGrowths == 0 {
		t.Fatalf("non-zero total growths expected: %d", stats.TotalGrowths)
	}
	if stats.TotalShrinks > 0 {
		t.Fatalf("zero total shrinks expected: %d", stats.TotalShrinks)
	}
	// This is useful when debugging table resize and occupancy.
	// Use -v flag to see the output.
	t.Log(stats.ToString())

	for i := 0; i < numEntries; i++ {
		m.Delete(strconv.Itoa(i))
	}
	stats = m.Stats()
	if stats.Size > 0 {
		t.Fatalf("zero size was expected: %d", stats.Size)
	}
	expectedCapacity = stats.RootBuckets * EntriesPerCowMapOfBucket
	if stats.Capacity != expectedCapacity {
		t.Fatalf("capacity was too large: %d, expected: %d", stats.Capacity, expectedCapacity)
	}
	if stats.RootBuckets != DefaultMinMapTableLen {
		t.Fatalf("table was too large: %d", stats.RootBuckets)
	}
	if stats.TotalShrinks == 0 {
		t.Fatalf("non-zero total shrinks expected: %d", stats.TotalShrinks)
	}
	t.Log(stats.ToString())
}

func TestCowMapOfResize_CounterLenLimit(t *testing.T) {
	const numEntries = 1_000_000
	m := NewCowMapOf[string, string]()

	for i := 0; i < numEntries; i++ {
		m.Store("foo"+strconv.Itoa(i), "bar"+strconv.Itoa(i))
	}
	stats := m.Stats()
	if stats.Size != numEntries {
		t.Fatalf("size was too small: %d", stats.Size)
	}
	if stats.CounterLen != MaxMapCounterLen {
		t.Fatalf("number of counter stripes was too large: %d, expected: %d",
			stats.CounterLen, MaxMapCounterLen)
	}
}

func parallelSeqCowTypedResizer(m *CowMapOf[int, int], numEntries int, positive bool, cdone chan bool) {
	for i := 0; i < numEntries; i++ {
		if positive {
			m.Store(i, i)
		} else {
			m.Store(-i, -i)
		}
	}
	cdone <- true
}

func TestCowMapOfParallelResize_GrowOnly(t *testing.T) {
	const numEntries = 100_000
	m := NewCowMapOf[int, int]()
	cdone := make(chan bool)
	go parallelSeqCowTypedResizer(m, numEntries, true, cdone)
	go parallelSeqCowTypedResizer(m, numEntries, false, cdone)
	// Wait for the goroutines to finish.
	<-cdone
	<-cdone
	// Verify map contents.
	for i := -numEntries + 1; i < numEntries; i++ {
		v, ok := m.Load(i)
		if !ok {
			t.Fatalf("value not found for %d", i)
		}
		if v != i {
			t.Fatalf("values do not match for %d: %v", i, v)
		}
	}
	if s := m.Size(); s != 2*numEntries-1 {
		t.Fatalf("unexpected size: %v", s)
	}
}

func parallelRandCowTypedResizer(t *testing.T, m *CowMapOf[string, int], numIters, numEntries int, cdone chan bool) {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < numIters; i++ {
		coin := r.Int63n(2)
		for j := 0; j < numEntries; j++ {
			if coin == 1 {
				m.Store(strconv.Itoa(j), j)
			} else {
				m.Delete(strconv.Itoa(j))
			}
		}
	}
	cdone <- true
}

func TestCowMapOfParallelResize(t *testing.T) {
	const numIters = 1_000
	const numEntries = 2 * EntriesPerCowMapOfBucket * DefaultMinMapTableLen
	m := NewCowMapOf[string, int]()
	cdone := make(chan bool)
	go parallelRandCowTypedResizer(t, m, numIters, numEntries, cdone)
	go parallelRandCowTypedResizer(t, m, numIters, numEntries, cdone)
	// Wait for the goroutines to finish.
	<-cdone
	<-cdone
	// Verify map contents.
	for i := 0; i < numEntries; i++ {
		v, ok := m.Load(strconv.Itoa(i))
		if !ok {
			// The entry may be deleted and that's ok.
			continue
		}
		if v != i {
			t.Fatalf("values do not match for %d: %v", i, v)
		}
	}
	s := m.Size()
	if s > numEntries {
		t.Fatalf("unexpected size: %v", s)
	}
	rs := sizeBasedOnCowTypedRange(m)
	if s != rs {
		t.Fatalf("size does not match number of entries in Range: %v, %v", s, rs)
	}
}

func parallelRandCowTypedClearer(t *testing.T, m *CowMapOf[string, int], numIters, numEntries int, cdone chan bool) {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < numIters; i++ {
		coin := r.Int63n(2)
		for j := 0; j < numEntries; j++ {
			if coin == 1 {
				m.Store(strconv.Itoa(j), j)
			} else {
				m.Clear()
			}
		}
	}
	cdone <- true
}

func TestCowMapOfParallelClear(t *testing.T) {
	const numIters = 100
	const numEntries = 1_000
	m := NewCowMapOf[string, int]()
	cdone := make(chan bool)
	go parallelRandCowTypedClearer(t, m, numIters, numEntries, cdone)
	go parallelRandCowTypedClearer(t, m, numIters, numEntries, cdone)
	// Wait for the goroutines to finish.
	<-cdone
	<-cdone
	// Verify map size.
	s := m.Size()
	if s > numEntries {
		t.Fatalf("unexpected size: %v", s)
	}
	rs := sizeBasedOnCowTypedRange(m)
	if s != rs {
		t.Fatalf("size does not match number of entries in Range: %v, %v", s, rs)
	}
}

func parallelSeqCowTypedStorer(t *testing.T, m *CowMapOf[string, int], storeEach, numIters, numEntries int, cdone chan bool) {
	for i := 0; i < numIters; i++ {
		for j := 0; j < numEntries; j++ {
			if storeEach == 0 || j%storeEach == 0 {
				m.Store(strconv.Itoa(j), j)
				// Due to atomic snapshots we must see a "<j>"/j pair.
				v, ok := m.Load(strconv.Itoa(j))
				if !ok {
					t.Errorf("value was not found for %d", j)
					break
				}
				if v != j {
					t.Errorf("value was not expected for %d: %d", j, v)
					break
				}
			}
		}
	}
	cdone <- true
}

func TestCowMapOfParallelStores(t *testing.T) {
	const numStorers = 4
	const numIters = 10_000
	const numEntries = 100
	m := NewCowMapOf[string, int]()
	cdone := make(chan bool)
	for i := 0; i < numStorers; i++ {
		go parallelSeqCowTypedStorer(t, m, i, numIters, numEntries, cdone)
	}
	// Wait for the goroutines to finish.
	for i := 0; i < numStorers; i++ {
		<-cdone
	}
	// Verify map contents.
	for i := 0; i < numEntries; i++ {
		v, ok := m.Load(strconv.Itoa(i))
		if !ok {
			t.Fatalf("value not found for %d", i)
		}
		if v != i {
			t.Fatalf("values do not match for %d: %v", i, v)
		}
	}
}

func parallelRandCowTypedCopyStorer(t *testing.T, m *CowMapOf[string, int], numIters, numEntries int, cdone chan bool) {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < numIters; i++ {
		j := r.Intn(numEntries)
		if v, loaded := m.LoadOrStore(strconv.Itoa(j), j); loaded {
			if v != j {
				t.Errorf("value was not expected for %d: %d", j, v)
			}
		}
	}
	cdone <- true
}

func parallelRandCowTypedCopyDeleter(t *testing.T, m *CowMapOf[string, int], numIters, numEntries int, cdone chan bool) {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < numIters; i++ {
		j := r.Intn(numEntries)
		if v, loaded := m.LoadAndDelete(strconv.Itoa(j)); loaded {
			if v != j {
				t.Errorf("value was not expected for %d: %d", j, v)
			}
		}
	}
	cdone <- true
}

func parallelCowTypedCopyLoader(t *testing.T, m *CowMapOf[string, int], numIters, numEntries int, cdone chan bool) {
	for i := 0; i < numIters; i++ {
		for j := 0; j < numEntries; j++ {
			// Due to atomic snapshots we must either see no entry, or a "<j>"/j pair.
			if v, ok := m.Load(strconv.Itoa(j)); ok {
				if v != j {
					t.Errorf("value was not expected for %d: %d", j, v)
				}
			}
		}
	}
	cdone <- true
}

func TestCowMapOfAtomicSnapshot(t *testing.T) {
	const numIters = 100_000
	const numEntries = 100
	m := NewCowMapOf[string, int]()
	cdone := make(chan bool)
	// Update or delete random entry in parallel with loads.
	go parallelRandCowTypedCopyStorer(t, m, numIters, numEntries, cdone)
	go parallelRandCowTypedCopyDeleter(t, m, numIters, numEntries, cdone)
	go parallelCowTypedCopyLoader(t, m, numIters, numEntries, cdone)
	// Wait for the goroutines to finish.
	for i := 0; i < 3; i++ {
		<-cdone
	}
}

func TestCowMapOfParallelStoresAndDeletes(t *testing.T) {
	const numWorkers = 2
	const numIters = 100_000
	const numEntries = 1000
	m := NewCowMapOf[string, int]()
	cdone := make(chan bool)
	// Update random entry in parallel with deletes.
	for i := 0; i < numWorkers; i++ {
		go parallelRandCowTypedStorer(t, m, numIters, numEntries, cdone)
		go parallelRandCowTypedDeleter(t, m, numIters, numEntries, cdone)
	}
	// Wait for the goroutines to finish.
	for i := 0; i < 2*numWorkers; i++ {
		<-cdone
	}
}

func parallelCowTypedComputer(m *CowMapOf[uint64, uint64], numIters, numEntries int, cdone chan bool) {
	for i := 0; i < numIters; i++ {
		for j := 0; j < numEntries; j++ {
			m.Compute(uint64(j), func(oldValue uint64, loaded bool) (newValue uint64, delete bool) {
				return oldValue + 1, false
			})
		}
	}
	cdone <- true
}

func TestCowMapOfParallelComputes(t *testing.T) {
	const numWorkers = 4 // Also stands for numEntries.
	const numIters = 10_000
	m := NewCowMapOf[uint64, uint64]()
	cdone := make(chan bool)
	for i := 0; i < numWorkers; i++ {
		go parallelCowTypedComputer(m, numIters, numWorkers, cdone)
	}
	// Wait for the goroutines to finish.
	for i := 0; i < numWorkers; i++ {
		<-cdone
	}
	// Verify map contents.
	for i := 0; i < numWorkers; i++ {
		v, ok := m.Load(uint64(i))
		if !ok {
			t.Fatalf("value not found for %d", i)
		}
		if v != numWorkers*numIters {
			t.Fatalf("values do not match for %d: %v", i, v)
		}
	}
}

func parallelCowTypedRangeStorer(m *CowMapOf[int, int], numEntries int, stopFlag *int64, cdone chan bool) {
	for {
		for i := 0; i < numEntries; i++ {
			m.Store(i, i)
		}
		if atomic.LoadInt64(stopFlag) != 0 {
			break
		}
	}
	cdone <- true
}

func parallelCowTypedRangeDeleter(m *CowMapOf[int, int], numEntries int, stopFlag *int64, cdone chan bool) {
	for {
		for i := 0; i < numEntries; i++ {
			m.Delete(i)
		}
		if atomic.LoadInt64(stopFlag) != 0 {
			break
		}
	}
	cdone <- true
}

func TestCowMapOfParallelRange(t *testing.T) {
	const numEntries = 10_000
	m := NewCowMapOf[int, int](WithPresize(numEntries))
	for i := 0; i < numEntries; i++ {
		m.Store(i, i)
	}
	// Start goroutines that would be storing and deleting items in parallel.
	cdone := make(chan bool)
	stopFlag := int64(0)
	go parallelCowTypedRangeStorer(m, numEntries, &stopFlag, cdone)
	go parallelCowTypedRangeDeleter(m, numEntries, &stopFlag, cdone)
	// Iterate the map and verify that no duplicate keys were met.
	met := make(map[int]int)
	m.Range(func(key int, value int) bool {
		if key != value {
			t.Fatalf("got unexpected value for key %d: %d", key, value)
			return false
		}
		met[key] += 1
		return true
	})
	if len(met) == 0 {
		t.Fatal("no entries were met when iterating")
	}
	for k, c := range met {
		if c != 1 {
			t.Fatalf("met key %d multiple times: %d", k, c)
		}
	}
	// Make sure that both goroutines finish.
	atomic.StoreInt64(&stopFlag, 1)
	<-cdone
	<-cdone
}

func parallelCowTypedShrinker(t *testing.T, m *CowMapOf[uint64, *point], numIters, numEntries int, stopFlag *int64, cdone chan bool) {
	for i := 0; i < numIters; i++ {
		for j := 0; j < numEntries; j++ {
			if p, loaded := m.LoadOrStore(uint64(j), &point{int32(j), int32(j)}); loaded {
				t.Errorf("value was present for %d: %v", j, p)
			}
		}
		for j := 0; j < numEntries; j++ {
			m.Delete(uint64(j))
		}
	}
	atomic.StoreInt64(stopFlag, 1)
	cdone <- true
}

func parallelCowTypedUpdater(t *testing.T, m *CowMapOf[uint64, *point], idx int, stopFlag *int64, cdone chan bool) {
	for atomic.LoadInt64(stopFlag) != 1 {
		sleepUs := int(Fastrand() % 10)
		if p, loaded := m.LoadOrStore(uint64(idx), &point{int32(idx), int32(idx)}); loaded {
			t.Errorf("value was present for %d: %v", idx, p)
		}
		time.Sleep(time.Duration(sleepUs) * time.Microsecond)
		if _, ok := m.Load(uint64(idx)); !ok {
			t.Errorf("value was not found for %d", idx)
		}
		m.Delete(uint64(idx))
	}
	cdone <- true
}

func TestCowMapOfDoesNotLoseEntriesOnResize(t *testing.T) {
	const numIters = 10_000
	const numEntries = 128
	m := NewCowMapOf[uint64, *point]()
	cdone := make(chan bool)
	stopFlag := int64(0)
	go parallelCowTypedShrinker(t, m, numIters, numEntries, &stopFlag, cdone)
	go parallelCowTypedUpdater(t, m, numEntries, &stopFlag, cdone)
	// Wait for the goroutines to finish.
	<-cdone
	<-cdone
	// Verify map contents.
	if s := m.Size(); s != 0 {
		t.Fatalf("map is not empty: %d", s)
	}
}

func TestCowMapOfStats(t *testing.T) {
	m := NewCowMapOf[int, int]()

	stats := m.Stats()
	if stats.RootBuckets != DefaultMinMapTableLen {
		t.Fatalf("unexpected number of root buckets: %d", stats.RootBuckets)
	}
	if stats.TotalBuckets != stats.RootBuckets {
		t.Fatalf("unexpected number of total buckets: %d", stats.TotalBuckets)
	}
	if stats.EmptyBuckets != stats.RootBuckets {
		t.Fatalf("unexpected number of empty buckets: %d", stats.EmptyBuckets)
	}
	if stats.Capacity != EntriesPerCowMapOfBucket*DefaultMinMapTableLen {
		t.Fatalf("unexpected capacity: %d", stats.Capacity)
	}
	if stats.Size != 0 {
		t.Fatalf("unexpected size: %d", stats.Size)
	}
	if stats.Counter != 0 {
		t.Fatalf("unexpected counter: %d", stats.Counter)
	}
	if stats.CounterLen != 8 {
		t.Fatalf("unexpected counter length: %d", stats.CounterLen)
	}

	for i := 0; i < 160; i++ {
		m.Store(i, i)
	}

	for i := 0; i < 10; i++ {
		m.Copy()
		m.Store(i, i)
	}

	stats = m.Stats()
	if stats.RootBuckets != 2*DefaultMinMapTableLen {
		t.Fatalf("unexpected number of root buckets: %d", stats.RootBuckets)
	}
	if stats.TotalBuckets < stats.RootBuckets {
		t.Fatalf("unexpected number of total buckets: %d", stats.TotalBuckets)
	}
	if stats.EmptyBuckets >= stats.RootBuckets {
		t.Fatalf("unexpected number of empty buckets: %d", stats.EmptyBuckets)
	}
	if stats.Capacity < 2*EntriesPerCowMapOfBucket*DefaultMinMapTableLen {
		t.Fatalf("unexpected capacity: %d", stats.Capacity)
	}
	if stats.Size != 160 {
		t.Fatalf("unexpected size: %d", stats.Size)
	}
	if stats.Counter != 160 {
		t.Fatalf("unexpected counter: %d", stats.Counter)
	}
	if stats.CounterLen != 8 {
		t.Fatalf("unexpected counter length: %d", stats.CounterLen)
	}
	if stats.TotalCopies != 10 {
		t.Fatalf("unexpected copies count: %d", stats.TotalCopies)
	}
}

func TestToPlainCowMapOf_NilPointer(t *testing.T) {
	pm := ToPlainCowMapOf[int, int](nil)
	if len(pm) != 0 {
		t.Fatalf("got unexpected size of nil map copy: %d", len(pm))
	}
}

func TestToPlainCowMapOf(t *testing.T) {
	const numEntries = 1000
	m := NewCowMapOf[int, int]()
	for i := 0; i < numEntries; i++ {
		m.Store(i, i)
	}
	pm := ToPlainCowMapOf[int, int](m)
	if len(pm) != numEntries {
		t.Fatalf("got unexpected size of nil map copy: %d", len(pm))
	}
	for i := 0; i < numEntries; i++ {
		if v := pm[i]; v != i {
			t.Fatalf("unexpected value for key %d: %d", i, v)
		}
	}
}

func BenchmarkCowMapOf_NoWarmUp(b *testing.B) {
	for _, bc := range benchmarkCases {
		if bc.readPercentage == 100 {
			// This benchmark doesn't make sense without a warm-up.
			continue
		}
		b.Run(bc.name, func(b *testing.B) {
			m := NewCowMapOf[string, int]()
			benchmarkMapOfStringKeys(b, func(k string) (int, bool) {
				return m.Load(k)
			}, func(k string, v int) {
				m.Store(k, v)
			}, func(k string) {
				m.Delete(k)
			}, bc.readPercentage)
		})
	}
}

func BenchmarkCowMapOf_WarmUp(b *testing.B) {
	for _, bc := range benchmarkCases {
		b.Run(bc.name, func(b *testing.B) {
			m := NewCowMapOf[string, int](WithPresize(benchmarkNumEntries))
			for i := 0; i < benchmarkNumEntries; i++ {
				m.Store(benchmarkKeyPrefix+strconv.Itoa(i), i)
			}
			b.ResetTimer()
			benchmarkMapOfStringKeys(b, func(k string) (int, bool) {
				return m.Load(k)
			}, func(k string, v int) {
				m.Store(k, v)
			}, func(k string) {
				m.Delete(k)
			}, bc.readPercentage)
		})
	}
}

func BenchmarkCowMapOfInt_NoWarmUp(b *testing.B) {
	for _, bc := range benchmarkCases {
		if bc.readPercentage == 100 {
			// This benchmark doesn't make sense without a warm-up.
			continue
		}
		b.Run(bc.name, func(b *testing.B) {
			m := NewCowMapOf[int, int]()
			benchmarkMapOfIntKeys(b, func(k int) (int, bool) {
				return m.Load(k)
			}, func(k int, v int) {
				m.Store(k, v)
			}, func(k int) {
				m.Delete(k)
			}, bc.readPercentage)
		})
	}
}

func BenchmarkCowMapOfInt_WarmUp(b *testing.B) {
	for _, bc := range benchmarkCases {
		b.Run(bc.name, func(b *testing.B) {
			m := NewCowMapOf[int, int](WithPresize(benchmarkNumEntries))
			for i := 0; i < benchmarkNumEntries; i++ {
				m.Store(i, i)
			}
			b.ResetTimer()
			benchmarkMapOfIntKeys(b, func(k int) (int, bool) {
				return m.Load(k)
			}, func(k int, v int) {
				m.Store(k, v)
			}, func(k int) {
				m.Delete(k)
			}, bc.readPercentage)
		})
	}
}

func BenchmarkCowMapOfInt_Murmur3Finalizer_WarmUp(b *testing.B) {
	for _, bc := range benchmarkCases {
		b.Run(bc.name, func(b *testing.B) {
			m := NewCowMapOfWithHasher[int, int](murmur3Finalizer, WithPresize(benchmarkNumEntries))
			for i := 0; i < benchmarkNumEntries; i++ {
				m.Store(i, i)
			}
			b.ResetTimer()
			benchmarkMapOfIntKeys(b, func(k int) (int, bool) {
				return m.Load(k)
			}, func(k int, v int) {
				m.Store(k, v)
			}, func(k int) {
				m.Delete(k)
			}, bc.readPercentage)
		})
	}
}

func BenchmarkCowMapOfRange(b *testing.B) {
	m := NewCowMapOf[string, int](WithPresize(benchmarkNumEntries))
	for i := 0; i < benchmarkNumEntries; i++ {
		m.Store(benchmarkKeys[i], i)
	}
	b.ResetTimer()
	runParallel(b, func(pb *testing.PB) {
		foo := 0
		for pb.Next() {
			m.Range(func(key string, value int) bool {
				foo++
				return true
			})
			_ = foo
		}
	})
}

func parallelRandCowTypedStorer(t *testing.T, m *CowMapOf[string, int], numIters, numEntries int, cdone chan bool) {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < numIters; i++ {
		j := r.Intn(numEntries)
		m.Copy()
		if v, loaded := m.LoadOrStore(strconv.Itoa(j), j); loaded {
			if v != j {
				t.Errorf("value was not expected for %d: %d", j, v)
			}
		}
	}
	cdone <- true
}

func parallelRandCowTypedDeleter(t *testing.T, m *CowMapOf[string, int], numIters, numEntries int, cdone chan bool) {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < numIters; i++ {
		j := r.Intn(numEntries)
		m.Copy()
		if v, loaded := m.LoadAndDelete(strconv.Itoa(j)); loaded {
			if v != j {
				t.Errorf("value was not expected for %d: %d", j, v)
			}
		}
	}
	cdone <- true
}

func parallelCowTypedLoader(t *testing.T, m *CowMapOf[string, int], numIters, numEntries int, cdone chan bool) {
	for i := 0; i < numIters; i++ {
		for j := 0; j < numEntries; j++ {
			m.Copy()
			// Due to atomic snapshots we must either see no entry, or a "<j>"/j pair.
			if v, ok := m.Load(strconv.Itoa(j)); ok {
				if v != j {
					t.Errorf("value was not expected for %d: %d", j, v)
				}
			}
		}
	}
	cdone <- true
}

func TestCowMapOfCopyAtomicSnapshot(t *testing.T) {
	const numIters = 100_000
	const numEntries = 100
	m := NewCowMapOf[string, int]()
	cdone := make(chan bool)
	// Update or delete random entry in parallel with loads.
	go parallelRandCowTypedStorer(t, m, numIters, numEntries, cdone)
	go parallelRandCowTypedDeleter(t, m, numIters, numEntries, cdone)
	go parallelCowTypedLoader(t, m, numIters, numEntries, cdone)
	// Wait for the goroutines to finish.
	for i := 0; i < 3; i++ {
		<-cdone
	}
}

func parallelCowTypedCopier(t *testing.T, m *CowMapOf[int, int], numIters, numEntries int, stopFlag *int64, cdone chan bool) {
	for i := 0; i < numIters; i++ {
		for j := 0; j < numEntries; j++ {
			if p, loaded := m.LoadOrStore(j, j); loaded {
				t.Errorf("value was present for %d: %v", j, p)
			}
		}
		m.Copy()
		for j := 0; j < numEntries; j++ {
			if _, deleted := m.LoadAndDelete(j); !deleted {
				t.Errorf("value was not found for %d", j)
			}
		}
	}
	atomic.StoreInt64(stopFlag, 1)
	cdone <- true
}

func parallelCowTypedCopyUpdater(t *testing.T, m *CowMapOf[int, int], idx int, stopFlag *int64, cdone chan bool) {
	for atomic.LoadInt64(stopFlag) != 1 {
		sleepUs := int(Fastrand() % 10)
		if p, loaded := m.LoadOrStore(idx, idx); loaded {
			t.Errorf("value was present for %d: %v", idx, p)
		}
		time.Sleep(time.Duration(sleepUs) * time.Microsecond)
		if _, ok := m.LoadAndDelete(idx); !ok {
			t.Errorf("value was not found for %d", idx)
		}
	}
	cdone <- true
}

func TestCowMapOfDoesNotLoseEntriesOnCopy(t *testing.T) {
	const numIters = 10_000
	const numEntries = 128
	m := NewCowMapOf[int, int]()
	cdone := make(chan bool)
	stopFlag := int64(0)
	go parallelCowTypedCopier(t, m, numIters, numEntries, &stopFlag, cdone)
	go parallelCowTypedCopyUpdater(t, m, numEntries, &stopFlag, cdone)
	// Wait for the goroutines to finish.
	<-cdone
	<-cdone
	// Verify map contents.
	if s := m.Size(); s != 0 {
		t.Fatalf("map is not empty: %d", s)
	}
}

func TestCowMapOfCopy(t *testing.T) {
	const numEntries = 10_000
	t.Run("fixed", func(t *testing.T) {
		var copyGen func(m *CowMapOf[int, int], start int, wg *sync.WaitGroup, maps *[]*CowMapOf[int, int], lock *sync.Mutex)
		copyGen = func(m *CowMapOf[int, int], start int, wg *sync.WaitGroup, maps *[]*CowMapOf[int, int], lock *sync.Mutex) {
			lock.Lock()
			*maps = append(*maps, m)
			lock.Unlock()
			for i := start; i < numEntries; i++ {
				m.Store(i, i)
				if i%(numEntries/5) == 0 {
					newStart := i + 1
					wg.Add(1)
					go copyGen(m.Copy(), newStart, wg, maps, lock)
				}
			}
			wg.Done()
		}
		m := NewCowMapOf[int, int]()
		var maps []*CowMapOf[int, int]
		var wg sync.WaitGroup
		wg.Add(1)
		go copyGen(m, 0, &wg, &maps, &sync.Mutex{})
		wg.Wait()
		all := make(map[int]int)
		half := make(map[int]int)
		otherHalf := make(map[int]int)
		for i := 0; i < numEntries; i++ {
			all[i] = i
			if i < numEntries/2 {
				half[i] = i
			} else {
				otherHalf[i] = i
			}
		}
		// Check maps equality
		for i, m := range maps {
			if !mapsEqual(all, ToPlainCowMapOf(m)) {
				t.Errorf("map %v mismatch", i)
			}
		}
		// Remove half from first half
		toRemove := half
		for i := 0; i < len(maps)/2; i++ {
			m := maps[i]
			wg.Add(1)
			go func() {
				for key := range toRemove {
					m.Delete(key)
				}
				wg.Done()
			}()
		}
		wg.Wait()
		// Check all values again
		for i, m := range maps {
			var want map[int]int
			if i < len(maps)/2 {
				want = otherHalf
			} else {
				want = all
			}
			if got := ToPlainCowMapOf(m); !mapsEqual(want, got) {
				t.Errorf("map %v mismatch, want %v got %v", i, len(want), len(got))
			}
		}
	})
	t.Run("continuous", func(t *testing.T) {
		var testFunc func(t *testing.T, N int, m1 *CowMapOf[int, int], e11 map[int]int, deep bool)
		testFunc = func(t *testing.T, N int, m1 *CowMapOf[int, int], e11 map[int]int, deep bool) {
			e12 := ToPlainCowMapOf(m1)
			if !mapsEqual(e11, e12) {
				t.Fatal("e11 != e12")
			}

			// Make a copy and compare the values
			m2 := m1.Copy()
			e21 := ToPlainCowMapOf(m2)
			if !mapsEqual(e21, e12) {
				t.Fatal("e21 != e12")
			}

			// Delete every other key
			e22 := make(map[int]int)
			var i int
			for k, v := range e21 {
				if i&1 == 0 {
					e22[k] = v
				} else {
					prev, deleted := m2.LoadAndDelete(k)
					if !deleted {
						t.Fatalf("expect %v to present before delete", k)
					}
					if prev != v {
						t.Fatalf("previous %v mismatch, got %d, want %d", k, prev, v)
					}
				}
				i++
			}
			if m2.Size() != N/2 {
				t.Fatalf("size mismatch, got %d, want %d", m2.Size(), N/2)
			}
			e23 := ToPlainCowMapOf(m2)
			if !mapsEqual(e23, e22) {
				t.Fatal("e23 != e22")
			}
			if !deep {
				var wg sync.WaitGroup
				wg.Add(2)
				go func() {
					defer wg.Done()
					testFunc(t, N/2, m2, e23, true)
				}()
				go func() {
					defer wg.Done()
					testFunc(t, N/2, m2, e23, true)
				}()
				wg.Wait()
			}
			e24 := ToPlainCowMapOf(m2)
			if !mapsEqual(e24, e23) {
				t.Fatal("e24 != e23")
			}
		}

		// Create the initial map
		m1 := NewCowMapOf[int, int]()
		for i := 0; i < numEntries; i++ {
			m1.Store(i, i)
		}
		e11 := ToPlainCowMapOf(m1)
		dur := time.Second * 2
		var wg sync.WaitGroup
		for i := 0; i < 16; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				start := time.Now()
				for time.Since(start) < dur {
					testFunc(t, numEntries, m1, e11, false)
				}
			}()
		}
		wg.Wait()
		e12 := ToPlainCowMapOf(m1)
		if !mapsEqual(e11, e12) {
			t.Fatal("map malformed during test")
		}
	})
}

func mapsEqual[K comparable, V comparable](m1, m2 map[K]V) bool {
	if len(m1) != len(m2) {
		return false
	}
	for k1, v1 := range m1 {
		v2, ok := m2[k1]
		if !ok {
			return false
		}
		if v1 != v2 {
			return false
		}
	}
	return true
}

func BenchmarkCowMapOfCopy(b *testing.B) {
	const numEntries = 1_000
	bench := func(copyBefore bool, f func(int, *CowMapOf[int, int]) *CowMapOf[int, int]) func(b *testing.B) {
		return func(b *testing.B) {
			m := NewCowMapOf[int, int]()
			for i := 0; i < numEntries; i++ {
				m.Store(i, i)
			}
			b.ResetTimer()
			b.ReportAllocs()
			if copyBefore {
				m = m.Copy()
			}
			for i := 0; i < b.N; i++ {
				m = f(i, m)
			}
		}
	}
	b.Run("copy", bench(false, func(i int, m *CowMapOf[int, int]) *CowMapOf[int, int] {
		return m.Copy()
	}))
	b.Run("once", func(b *testing.B) {
		b.Run("delete store", bench(true, func(i int, m *CowMapOf[int, int]) *CowMapOf[int, int] {
			v := i % numEntries
			m.Delete(v)
			m.Store(v, v)
			return m
		}))
		b.Run("load", bench(true, func(i int, m *CowMapOf[int, int]) *CowMapOf[int, int] {
			v := i % numEntries
			m.Load(v)
			return m
		}))
	})
	b.Run("each time", func(b *testing.B) {
		b.Run("delete store", bench(true, func(i int, m *CowMapOf[int, int]) *CowMapOf[int, int] {
			m = m.Copy()
			v := i % numEntries
			m.Delete(v)
			m.Store(v, v)
			return m
		}))
		b.Run("load", bench(true, func(i int, m *CowMapOf[int, int]) *CowMapOf[int, int] {
			m = m.Copy()
			v := i % numEntries
			m.Load(v)
			return m
		}))
	})
}
