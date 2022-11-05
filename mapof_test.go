//go:build go1.18
// +build go1.18

package xsync_test

import (
	"encoding/binary"
	"hash/maphash"
	"math"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	. "github.com/puzpuzpuz/xsync/v2"
)

type point struct {
	x int32
	y int32
}

func pointHash(seed maphash.Seed, p point) uint64 {
	var h maphash.Hash
	h.SetSeed(seed)
	binary.Write(&h, binary.LittleEndian, p.x)
	hash := h.Sum64()
	h.Reset()
	binary.Write(&h, binary.LittleEndian, p.y)
	return 31*hash + h.Sum64()
}

func TestMap_BucketOfStructSize(t *testing.T) {
	size := unsafe.Sizeof(BucketOfPadded{})
	if size != 64 {
		t.Fatalf("size of 64B (one cache line) is expected, got: %d", size)
	}
}

func TestMapOf_MissingEntry(t *testing.T) {
	m := NewMapOf[string]()
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

func TestMapOf_EmptyStringKey(t *testing.T) {
	m := NewMapOf[string]()
	m.Store("", "foobar")
	v, ok := m.Load("")
	if !ok {
		t.Fatal("value was expected")
	}
	if v != "foobar" {
		t.Fatalf("value does not match: %v", v)
	}
}

func TestMapOfStore_NilValue(t *testing.T) {
	m := NewMapOf[*struct{}]()
	m.Store("foo", nil)
	v, ok := m.Load("foo")
	if !ok {
		t.Fatal("nil value was expected")
	}
	if v != nil {
		t.Fatalf("value was not nil: %v", v)
	}
}

func TestMapOfLoadOrStore_NilValue(t *testing.T) {
	m := NewMapOf[*struct{}]()
	m.LoadOrStore("foo", nil)
	v, loaded := m.LoadOrStore("foo", nil)
	if !loaded {
		t.Fatal("nil value was expected")
	}
	if v != nil {
		t.Fatalf("value was not nil: %v", v)
	}
}

func TestMapOfLoadOrStore_NonNilValue(t *testing.T) {
	type foo struct{}
	m := NewMapOf[*foo]()
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

func TestMapOfLoadAndStore_NilValue(t *testing.T) {
	m := NewMapOf[*struct{}]()
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

func TestMapOfLoadAndStore_NonNilValue(t *testing.T) {
	m := NewMapOf[int]()
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

func TestMapOfRange(t *testing.T) {
	const numEntries = 1000
	m := NewMapOf[int]()
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

func TestMapOfRange_FalseReturned(t *testing.T) {
	m := NewMapOf[int]()
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

func TestMapOfRange_NestedDelete(t *testing.T) {
	const numEntries = 256
	m := NewMapOf[int]()
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

func TestMapOfStore(t *testing.T) {
	const numEntries = 128
	m := NewMapOf[int]()
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

func TestIntegerMapOfStore(t *testing.T) {
	const numEntries = 128
	m := NewIntegerMapOf[int, int]()
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

func TestTypedMapOfStore_StructKeys_IntValues(t *testing.T) {
	const numEntries = 128
	m := NewTypedMapOf[point, int](pointHash)
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

func TestTypedMapOfStore_StructKeys_StructValues(t *testing.T) {
	const numEntries = 128
	m := NewTypedMapOf[point, point](pointHash)
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

func TestTypedMapOfStore_HashCodeCollisions(t *testing.T) {
	const numEntries = 1000
	m := NewTypedMapOf[int, int](func(_ maphash.Seed, i int) uint64 {
		// We intentionally use an awful hash function here to make sure
		// that the map copes with key collisions.
		return 42
	})
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

func TestMapOfLoadOrStore(t *testing.T) {
	const numEntries = 1000
	m := NewMapOf[int]()
	for i := 0; i < numEntries; i++ {
		m.Store(strconv.Itoa(i), i)
	}
	for i := 0; i < numEntries; i++ {
		if _, loaded := m.LoadOrStore(strconv.Itoa(i), i); !loaded {
			t.Fatalf("value not found for %d", i)
		}
	}
}

func TestMapOfLoadOrCompute(t *testing.T) {
	const numEntries = 1000
	m := NewMapOf[int]()
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

func TestMapOfLoadOrCompute_FunctionCalledOnce(t *testing.T) {
	m := NewIntegerMapOf[int, int]()
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

func TestMapOfCompute(t *testing.T) {
	m := NewMapOf[int]()
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

func TestMapOfStoreThenDelete(t *testing.T) {
	const numEntries = 1000
	m := NewMapOf[int]()
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

func TestIntegerMapOfStoreThenDelete(t *testing.T) {
	const numEntries = 1000
	m := NewIntegerMapOf[int32, int32]()
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

func TestTypedMapOfStoreThenDelete(t *testing.T) {
	const numEntries = 1000
	m := NewTypedMapOf[point, string](pointHash)
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

func TestMapOfStoreThenLoadAndDelete(t *testing.T) {
	const numEntries = 1000
	m := NewMapOf[int]()
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

func TestIntegerMapOfStoreThenLoadAndDelete(t *testing.T) {
	const numEntries = 1000
	m := NewIntegerMapOf[int, int]()
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

func TestTypedMapOfStoreThenLoadAndDelete(t *testing.T) {
	const numEntries = 1000
	m := NewTypedMapOf[point, int](pointHash)
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

func sizeBasedOnTypedRange(m *MapOf[string, int]) int {
	size := 0
	m.Range(func(key string, value int) bool {
		size++
		return true
	})
	return size
}

func TestMapOfSize(t *testing.T) {
	const numEntries = 1000
	m := NewMapOf[int]()
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
		rsize := sizeBasedOnTypedRange(m)
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
		rsize := sizeBasedOnTypedRange(m)
		if size != rsize {
			t.Fatalf("size does not match number of entries in Range: %v, %v", size, rsize)
		}
	}
}

func TestMapOfClear(t *testing.T) {
	const numEntries = 1000
	m := NewMapOf[int]()
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
	rsize := sizeBasedOnTypedRange(m)
	if rsize != 0 {
		t.Fatalf("zero number of entries in Range was expected, got: %d", rsize)
	}
}

func assertMapOfCapacity[K comparable, V any](t *testing.T, m *MapOf[K, V], expectedCap int) {
	stats := CollectMapOfStats(m)
	if stats.Capacity != expectedCap {
		t.Fatalf("capacity was different from %d: %d", expectedCap, stats.Capacity)
	}
}

func TestNewMapOfPresized(t *testing.T) {
	assertMapOfCapacity(t, NewMapOf[string](), MinMapTableCap)
	assertMapOfCapacity(t, NewMapOfPresized[string](0), MinMapTableCap)
	assertMapOfCapacity(t, NewMapOfPresized[string](-100), MinMapTableCap)
	assertMapOfCapacity(t, NewMapOfPresized[string](500), 768)

	assertMapOfCapacity(t, NewIntegerMapOf[int, int](), MinMapTableCap)
	assertMapOfCapacity(t, NewIntegerMapOfPresized[int, int](0), MinMapTableCap)
	assertMapOfCapacity(t, NewIntegerMapOfPresized[int, int](-1), MinMapTableCap)
	assertMapOfCapacity(t, NewIntegerMapOfPresized[int, int](1_000_000), 1_572_864)

	assertMapOfCapacity(t, NewTypedMapOf[point, point](pointHash), MinMapTableCap)
	assertMapOfCapacity(t, NewTypedMapOfPresized[point, point](pointHash, 0), MinMapTableCap)
	assertMapOfCapacity(t, NewTypedMapOfPresized[point, point](pointHash, -42), MinMapTableCap)
	assertMapOfCapacity(t, NewTypedMapOfPresized[point, point](pointHash, 100), 192)
}

func TestMapOfResize(t *testing.T) {
	const numEntries = 100_000
	m := NewMapOf[int]()

	for i := 0; i < numEntries; i++ {
		m.Store(strconv.Itoa(i), i)
	}
	stats := CollectMapOfStats(m)
	if stats.Size != numEntries {
		t.Fatalf("size was too small: %d", stats.Size)
	}
	expectedCapacity := int(math.RoundToEven(MapLoadFactor+1)) * stats.RootBuckets * EntriesPerMapBucket
	if stats.Capacity > expectedCapacity {
		t.Fatalf("capacity was too large: %d, expected: %d", stats.Capacity, expectedCapacity)
	}
	if stats.RootBuckets <= MinMapTableLen {
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
	stats = CollectMapOfStats(m)
	if stats.Size > 0 {
		t.Fatalf("zero size was expected: %d", stats.Size)
	}
	expectedCapacity = stats.RootBuckets * EntriesPerMapBucket
	if stats.Capacity != expectedCapacity {
		t.Fatalf("capacity was too large: %d, expected: %d", stats.Capacity, expectedCapacity)
	}
	if stats.RootBuckets != MinMapTableLen {
		t.Fatalf("table was too large: %d", stats.RootBuckets)
	}
	if stats.TotalShrinks == 0 {
		t.Fatalf("non-zero total shrinks expected: %d", stats.TotalShrinks)
	}
	t.Log(stats.ToString())
}

func TestMapOfResize_CounterLenLimit(t *testing.T) {
	const numEntries = 1_000_000
	m := NewMapOf[string]()

	for i := 0; i < numEntries; i++ {
		m.Store("foo"+strconv.Itoa(i), "bar"+strconv.Itoa(i))
	}
	stats := CollectMapOfStats(m)
	if stats.Size != numEntries {
		t.Fatalf("size was too small: %d", stats.Size)
	}
	if stats.CounterLen != MaxMapCounterLen {
		t.Fatalf("number of counter stripes was too large: %d, expected: %d",
			stats.CounterLen, MaxMapCounterLen)
	}
}

func parallelSeqTypedResizer(t *testing.T, m *MapOf[int, int], numEntries int, positive bool, cdone chan bool) {
	for i := 0; i < numEntries; i++ {
		if positive {
			m.Store(i, i)
		} else {
			m.Store(-i, -i)
		}
	}
	cdone <- true
}

func TestMapOfParallelResize_GrowOnly(t *testing.T) {
	const numEntries = 100_000
	m := NewIntegerMapOf[int, int]()
	cdone := make(chan bool)
	go parallelSeqTypedResizer(t, m, numEntries, true, cdone)
	go parallelSeqTypedResizer(t, m, numEntries, false, cdone)
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

func parallelRandTypedResizer(t *testing.T, m *MapOf[string, int], numIters, numEntries int, cdone chan bool) {
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

func TestMapOfParallelResize(t *testing.T) {
	const numIters = 1_000
	const numEntries = 2 * EntriesPerMapBucket * MinMapTableLen
	m := NewMapOf[int]()
	cdone := make(chan bool)
	go parallelRandTypedResizer(t, m, numIters, numEntries, cdone)
	go parallelRandTypedResizer(t, m, numIters, numEntries, cdone)
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
	rs := sizeBasedOnTypedRange(m)
	if s != rs {
		t.Fatalf("size does not match number of entries in Range: %v, %v", s, rs)
	}
}

func parallelRandTypedClearer(t *testing.T, m *MapOf[string, int], numIters, numEntries int, cdone chan bool) {
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

func TestMapOfParallelClear(t *testing.T) {
	const numIters = 100
	const numEntries = 1_000
	m := NewMapOf[int]()
	cdone := make(chan bool)
	go parallelRandTypedClearer(t, m, numIters, numEntries, cdone)
	go parallelRandTypedClearer(t, m, numIters, numEntries, cdone)
	// Wait for the goroutines to finish.
	<-cdone
	<-cdone
	// Verify map size.
	s := m.Size()
	if s > numEntries {
		t.Fatalf("unexpected size: %v", s)
	}
	rs := sizeBasedOnTypedRange(m)
	if s != rs {
		t.Fatalf("size does not match number of entries in Range: %v, %v", s, rs)
	}
}

func parallelSeqTypedStorer(t *testing.T, m *MapOf[string, int], storeEach, numIters, numEntries int, cdone chan bool) {
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

func TestMapOfParallelStores(t *testing.T) {
	const numStorers = 4
	const numIters = 10_000
	const numEntries = 100
	m := NewMapOf[int]()
	cdone := make(chan bool)
	for i := 0; i < numStorers; i++ {
		go parallelSeqTypedStorer(t, m, i, numIters, numEntries, cdone)
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

func parallelRandTypedStorer(t *testing.T, m *MapOf[string, int], numIters, numEntries int, cdone chan bool) {
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

func parallelRandTypedDeleter(t *testing.T, m *MapOf[string, int], numIters, numEntries int, cdone chan bool) {
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

func parallelTypedLoader(t *testing.T, m *MapOf[string, int], numIters, numEntries int, cdone chan bool) {
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

func TestMapOfAtomicSnapshot(t *testing.T) {
	const numIters = 100_000
	const numEntries = 100
	m := NewMapOf[int]()
	cdone := make(chan bool)
	// Update or delete random entry in parallel with loads.
	go parallelRandTypedStorer(t, m, numIters, numEntries, cdone)
	go parallelRandTypedDeleter(t, m, numIters, numEntries, cdone)
	go parallelTypedLoader(t, m, numIters, numEntries, cdone)
	// Wait for the goroutines to finish.
	for i := 0; i < 3; i++ {
		<-cdone
	}
}

func TestMapOfParallelStoresAndDeletes(t *testing.T) {
	const numWorkers = 2
	const numIters = 100_000
	const numEntries = 1000
	m := NewMapOf[int]()
	cdone := make(chan bool)
	// Update random entry in parallel with deletes.
	for i := 0; i < numWorkers; i++ {
		go parallelRandTypedStorer(t, m, numIters, numEntries, cdone)
		go parallelRandTypedDeleter(t, m, numIters, numEntries, cdone)
	}
	// Wait for the goroutines to finish.
	for i := 0; i < 2*numWorkers; i++ {
		<-cdone
	}
}

func parallelTypedComputer(t *testing.T, m *MapOf[uint64, uint64], numIters, numEntries int, cdone chan bool) {
	for i := 0; i < numIters; i++ {
		for j := 0; j < numEntries; j++ {
			m.Compute(uint64(j), func(oldValue uint64, loaded bool) (newValue uint64, delete bool) {
				return oldValue + 1, false
			})
		}
	}
	cdone <- true
}

func TestMapOfParallelComputes(t *testing.T) {
	const numWorkers = 4 // Also stands for numEntries.
	const numIters = 10_000
	m := NewIntegerMapOf[uint64, uint64]()
	cdone := make(chan bool)
	for i := 0; i < numWorkers; i++ {
		go parallelTypedComputer(t, m, numIters, numWorkers, cdone)
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

func parallelTypedRangeStorer(t *testing.T, m *MapOf[int, int], numEntries int, stopFlag *int64, cdone chan bool) {
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

func parallelTypedRangeDeleter(t *testing.T, m *MapOf[int, int], numEntries int, stopFlag *int64, cdone chan bool) {
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

func TestMapOfParallelRange(t *testing.T) {
	const numEntries = 10_000
	m := NewIntegerMapOfPresized[int, int](numEntries)
	for i := 0; i < numEntries; i++ {
		m.Store(i, i)
	}
	// Start goroutines that would be storing and deleting items in parallel.
	cdone := make(chan bool)
	stopFlag := int64(0)
	go parallelTypedRangeStorer(t, m, numEntries, &stopFlag, cdone)
	go parallelTypedRangeDeleter(t, m, numEntries, &stopFlag, cdone)
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

func BenchmarkMapOf_NoWarmUp(b *testing.B) {
	for _, bc := range benchmarkCases {
		if bc.readPercentage == 100 {
			// This benchmark doesn't make sense without a warm-up.
			continue
		}
		b.Run(bc.name, func(b *testing.B) {
			m := NewMapOf[int]()
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

func BenchmarkMapOf_WarmUp(b *testing.B) {
	for _, bc := range benchmarkCases {
		b.Run(bc.name, func(b *testing.B) {
			m := NewMapOfPresized[int](benchmarkNumEntries)
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

func benchmarkMapOfStringKeys(
	b *testing.B,
	loadFn func(k string) (int, bool),
	storeFn func(k string, v int),
	deleteFn func(k string),
	readPercentage int,
) {
	runParallel(b, func(pb *testing.PB) {
		// convert percent to permille to support 99% case
		storeThreshold := 10 * readPercentage
		deleteThreshold := 10*readPercentage + ((1000 - 10*readPercentage) / 2)
		for pb.Next() {
			op := int(Fastrand() % 1000)
			i := int(Fastrand() % benchmarkNumEntries)
			if op >= deleteThreshold {
				deleteFn(benchmarkKeys[i])
			} else if op >= storeThreshold {
				storeFn(benchmarkKeys[i], i)
			} else {
				loadFn(benchmarkKeys[i])
			}
		}
	})
}

func BenchmarkIntegerMapOf_NoWarmUp(b *testing.B) {
	for _, bc := range benchmarkCases {
		if bc.readPercentage == 100 {
			// This benchmark doesn't make sense without a warm-up.
			continue
		}
		b.Run(bc.name, func(b *testing.B) {
			m := NewIntegerMapOf[int, int]()
			benchmarkMapOfIntegerKeys(b, func(k int) (int, bool) {
				return m.Load(k)
			}, func(k int, v int) {
				m.Store(k, v)
			}, func(k int) {
				m.Delete(k)
			}, bc.readPercentage)
		})
	}
}

func BenchmarkIntegerMapOf_WarmUp(b *testing.B) {
	for _, bc := range benchmarkCases {
		b.Run(bc.name, func(b *testing.B) {
			m := NewIntegerMapOfPresized[int, int](benchmarkNumEntries)
			for i := 0; i < benchmarkNumEntries; i++ {
				m.Store(i, i)
			}
			b.ResetTimer()
			benchmarkMapOfIntegerKeys(b, func(k int) (int, bool) {
				return m.Load(k)
			}, func(k int, v int) {
				m.Store(k, v)
			}, func(k int) {
				m.Delete(k)
			}, bc.readPercentage)
		})
	}
}

func BenchmarkIntegerMapStandard_NoWarmUp(b *testing.B) {
	for _, bc := range benchmarkCases {
		if bc.readPercentage == 100 {
			// This benchmark doesn't make sense without a warm-up.
			continue
		}
		b.Run(bc.name, func(b *testing.B) {
			var m sync.Map
			benchmarkMapOfIntegerKeys(b, func(k int) (value int, ok bool) {
				v, ok := m.Load(k)
				if ok {
					return v.(int), ok
				} else {
					return 0, false
				}
			}, func(k int, v int) {
				m.Store(k, v)
			}, func(k int) {
				m.Delete(k)
			}, bc.readPercentage)
		})
	}
}

// This is a nice scenario for sync.Map since a lot of updates
// will hit the readOnly part of the map.
func BenchmarkIntegerMapStandard_WarmUp(b *testing.B) {
	for _, bc := range benchmarkCases {
		b.Run(bc.name, func(b *testing.B) {
			var m sync.Map
			for i := 0; i < benchmarkNumEntries; i++ {
				m.Store(i, i)
			}
			b.ResetTimer()
			benchmarkMapOfIntegerKeys(b, func(k int) (value int, ok bool) {
				v, ok := m.Load(k)
				if ok {
					return v.(int), ok
				} else {
					return 0, false
				}
			}, func(k int, v int) {
				m.Store(k, v)
			}, func(k int) {
				m.Delete(k)
			}, bc.readPercentage)
		})
	}
}

func benchmarkMapOfIntegerKeys(
	b *testing.B,
	loadFn func(k int) (int, bool),
	storeFn func(k int, v int),
	deleteFn func(k int),
	readPercentage int,
) {
	runParallel(b, func(pb *testing.PB) {
		// convert percent to permille to support 99% case
		storeThreshold := 10 * readPercentage
		deleteThreshold := 10*readPercentage + ((1000 - 10*readPercentage) / 2)
		for pb.Next() {
			op := int(Fastrand() % 1000)
			i := int(Fastrand() % benchmarkNumEntries)
			if op >= deleteThreshold {
				deleteFn(i)
			} else if op >= storeThreshold {
				storeFn(i, i)
			} else {
				loadFn(i)
			}
		}
	})
}

func BenchmarkMapOfRange(b *testing.B) {
	m := NewMapOfPresized[int](benchmarkNumEntries)
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
