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
	"testing"
	"time"

	. "github.com/puzpuzpuz/xsync/v2"
)

func TestMapOf_UniqueValuePointers_Int(t *testing.T) {
	EnableAssertions()
	m := NewMapOf[int]()
	v := 42
	m.Store("foo", v)
	m.Store("foo", v)
	DisableAssertions()
}

func TestMapOf_UniqueValuePointers_Struct(t *testing.T) {
	type foo struct{}
	EnableAssertions()
	m := NewMapOf[foo]()
	v := foo{}
	m.Store("foo", v)
	m.Store("foo", v)
	DisableAssertions()
}

func TestMapOf_UniqueValuePointers_Pointer(t *testing.T) {
	type foo struct{}
	EnableAssertions()
	m := NewMapOf[*foo]()
	v := &foo{}
	m.Store("foo", v)
	m.Store("foo", v)
	DisableAssertions()
}

func TestMapOf_UniqueValuePointers_Slice(t *testing.T) {
	EnableAssertions()
	m := NewMapOf[[]int]()
	v := make([]int, 13)
	m.Store("foo", v)
	m.Store("foo", v)
	DisableAssertions()
}

func TestMapOf_UniqueValuePointers_String(t *testing.T) {
	EnableAssertions()
	m := NewMapOf[string]()
	v := "bar"
	m.Store("foo", v)
	m.Store("foo", v)
	DisableAssertions()
}

func TestMapOf_UniqueValuePointers_Nil(t *testing.T) {
	EnableAssertions()
	m := NewMapOf[*struct{}]()
	m.Store("foo", nil)
	m.Store("foo", nil)
	DisableAssertions()
}

func TestMapOf_MissingEntry(t *testing.T) {
	m := NewMapOf[string]()
	v, ok := m.Load("foo")
	if ok {
		t.Errorf("value was not expected: %v", v)
	}
	if deleted, loaded := m.LoadAndDelete("foo"); loaded {
		t.Errorf("value was not expected %v", deleted)
	}
	if actual, loaded := m.LoadOrStore("foo", "bar"); loaded {
		t.Errorf("value was not expected %v", actual)
	}
}

func TestMapOf_EmptyStringKey(t *testing.T) {
	m := NewMapOf[string]()
	m.Store("", "foobar")
	v, ok := m.Load("")
	if !ok {
		t.Error("value was expected")
	}
	if v != "foobar" {
		t.Errorf("value does not match: %v", v)
	}
}

func TestMapOfStore_NilValue(t *testing.T) {
	m := NewMapOf[*struct{}]()
	m.Store("foo", nil)
	v, ok := m.Load("foo")
	if !ok {
		t.Error("nil value was expected")
	}
	if v != nil {
		t.Errorf("value was not nil: %v", v)
	}
}

func TestMapOfLoadOrStore_NilValue(t *testing.T) {
	m := NewMapOf[*struct{}]()
	m.LoadOrStore("foo", nil)
	v, loaded := m.LoadOrStore("foo", nil)
	if !loaded {
		t.Error("nil value was expected")
	}
	if v != nil {
		t.Errorf("value was not nil: %v", v)
	}
}

func TestMapOfLoadOrStore_NonNilValue(t *testing.T) {
	type foo struct{}
	m := NewMapOf[*foo]()
	newv := &foo{}
	v, loaded := m.LoadOrStore("foo", newv)
	if loaded {
		t.Error("no value was expected")
	}
	if v != newv {
		t.Errorf("value does not match: %v", v)
	}
	newv2 := &foo{}
	v, loaded = m.LoadOrStore("foo", newv2)
	if !loaded {
		t.Error("value was expected")
	}
	if v != newv {
		t.Errorf("value does not match: %v", v)
	}
}

func TestMapOfLoadAndStore_NilValue(t *testing.T) {
	m := NewMapOf[*struct{}]()
	m.LoadAndStore("foo", nil)
	v, loaded := m.LoadAndStore("foo", nil)
	if !loaded {
		t.Error("nil value was expected")
	}
	if v != nil {
		t.Errorf("value was not nil: %v", v)
	}
	v, loaded = m.Load("foo")
	if !loaded {
		t.Error("nil value was expected")
	}
	if v != nil {
		t.Errorf("value was not nil: %v", v)
	}
}

func TestMapOfLoadAndStore_NonNilValue(t *testing.T) {
	m := NewMapOf[int]()
	v1 := 1
	v, loaded := m.LoadAndStore("foo", v1)
	if loaded {
		t.Error("no value was expected")
	}
	if v != v1 {
		t.Errorf("value does not match: %v", v)
	}
	v2 := 2
	v, loaded = m.LoadAndStore("foo", v2)
	if !loaded {
		t.Error("value was expected")
	}
	if v != v1 {
		t.Errorf("value does not match: %v", v)
	}
	v, loaded = m.Load("foo")
	if !loaded {
		t.Error("value was expected")
	}
	if v != v2 {
		t.Errorf("value does not match: %v", v)
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
			t.Errorf("got unexpected key/value for iteration %d: %v/%v", iters, key, value)
			return false
		}
		met[key] += 1
		iters++
		return true
	})
	if iters != numEntries {
		t.Errorf("got unexpected number of iterations: %d", iters)
	}
	for i := 0; i < numEntries; i++ {
		if c := met[strconv.Itoa(i)]; c != 1 {
			t.Errorf("range did not iterate correctly over %d: %d", i, c)
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
		t.Errorf("got unexpected number of iterations: %d", iters)
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
			t.Errorf("value found for %d", i)
		}
	}
}

func TestMapOfSerialStore(t *testing.T) {
	const numEntries = 128
	m := NewMapOf[int]()
	for i := 0; i < numEntries; i++ {
		m.Store(strconv.Itoa(i), i)
	}
	for i := 0; i < numEntries; i++ {
		v, ok := m.Load(strconv.Itoa(i))
		if !ok {
			t.Errorf("value not found for %d", i)
		}
		if v != i {
			t.Errorf("values do not match for %d: %v", i, v)
		}
	}
}

func TestIntegerMapOfSerialStore(t *testing.T) {
	const numEntries = 128
	m := NewIntegerMapOf[int, int]()
	for i := 0; i < numEntries; i++ {
		m.Store(i, i)
	}
	for i := 0; i < numEntries; i++ {
		v, ok := m.Load(i)
		if !ok {
			t.Errorf("value not found for %d", i)
		}
		if v != i {
			t.Errorf("values do not match for %d: %v", i, v)
		}
	}
}

func TestTypedMapOfSerialStore_StructKeys_IntValues(t *testing.T) {
	type foo struct {
		x int
		y int
	}
	const numEntries = 128
	m := NewTypedMapOf[foo, int](func(seed maphash.Seed, f foo) uint64 {
		var h maphash.Hash
		h.SetSeed(seed)
		binary.Write(&h, binary.LittleEndian, f)
		return h.Sum64()
	})
	for i := 0; i < numEntries; i++ {
		m.Store(foo{i, -i}, i)
	}
	for i := 0; i < numEntries; i++ {
		v, ok := m.Load(foo{i, -i})
		if !ok {
			t.Errorf("value not found for %d", i)
		}
		if v != i {
			t.Errorf("values do not match for %d: %v", i, v)
		}
	}
}

func TestTypedMapOfSerialStore_StructKeys_StructValues(t *testing.T) {
	type foo struct {
		x int
		y int
	}
	const numEntries = 128
	m := NewTypedMapOf[foo, foo](func(seed maphash.Seed, f foo) uint64 {
		var h maphash.Hash
		h.SetSeed(seed)
		binary.Write(&h, binary.LittleEndian, f)
		return h.Sum64()
	})
	for i := 0; i < numEntries; i++ {
		m.Store(foo{i, -i}, foo{-i, i})
	}
	for i := 0; i < numEntries; i++ {
		v, ok := m.Load(foo{i, -i})
		if !ok {
			t.Errorf("value not found for %d", i)
		}
		if v.x != -i {
			t.Errorf("x value does not match for %d: %v", i, v)
		}
		if v.y != i {
			t.Errorf("y value does not match for %d: %v", i, v)
		}
	}
}

func TestTypedMapOfSerialStore_HashCodeCollisions(t *testing.T) {
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
			t.Errorf("value not found for %d", i)
		}
		if v != i {
			t.Errorf("values do not match for %d: %v", i, v)
		}
	}
}

func TestMapOfSerialLoadOrStore(t *testing.T) {
	const numEntries = 1000
	m := NewMapOf[int]()
	for i := 0; i < numEntries; i++ {
		m.Store(strconv.Itoa(i), i)
	}
	for i := 0; i < numEntries; i++ {
		if _, loaded := m.LoadOrStore(strconv.Itoa(i), i); !loaded {
			t.Errorf("value not found for %d", i)
		}
	}
}

func TestMapOfSerialLoadOrCompute(t *testing.T) {
	const numEntries = 1000
	m := NewMapOf[int]()
	for i := 0; i < numEntries; i++ {
		v, loaded := m.LoadOrCompute(strconv.Itoa(i), func() int {
			return i
		})
		if loaded {
			t.Errorf("value not computed for %d", i)
		}
		if v != i {
			t.Errorf("values do not match for %d: %v", i, v)
		}
	}
	for i := 0; i < numEntries; i++ {
		v, loaded := m.LoadOrCompute(strconv.Itoa(i), func() int {
			return i
		})
		if !loaded {
			t.Errorf("value not loaded for %d", i)
		}
		if v != i {
			t.Errorf("values do not match for %d: %v", i, v)
		}
	}
}

func TestMapOfSerialStoreThenDelete(t *testing.T) {
	const numEntries = 1000
	m := NewMapOf[int]()
	for i := 0; i < numEntries; i++ {
		m.Store(strconv.Itoa(i), i)
	}
	for i := 0; i < numEntries; i++ {
		m.Delete(strconv.Itoa(i))
		if _, ok := m.Load(strconv.Itoa(i)); ok {
			t.Errorf("value was not expected for %d", i)
		}
	}
}

func TestIntegerMapOfSerialStoreThenDelete(t *testing.T) {
	const numEntries = 1000
	m := NewIntegerMapOf[int32, int32]()
	for i := 0; i < numEntries; i++ {
		m.Store(int32(i), int32(i))
	}
	for i := 0; i < numEntries; i++ {
		m.Delete(int32(i))
		if _, ok := m.Load(int32(i)); ok {
			t.Errorf("value was not expected for %d", i)
		}
	}
}

func TestTypedMapOfSerialStoreThenDelete(t *testing.T) {
	type foo struct {
		x int
		y int
	}
	const numEntries = 1000
	m := NewTypedMapOf[foo, string](func(seed maphash.Seed, f foo) uint64 {
		var h maphash.Hash
		h.SetSeed(seed)
		binary.Write(&h, binary.LittleEndian, f)
		return h.Sum64()
	})
	for i := 0; i < numEntries; i++ {
		m.Store(foo{i, 42}, strconv.Itoa(i))
	}
	for i := 0; i < numEntries; i++ {
		m.Delete(foo{i, 42})
		if _, ok := m.Load(foo{i, 42}); ok {
			t.Errorf("value was not expected for %d", i)
		}
	}
}

func TestMapOfSerialStoreThenLoadAndDelete(t *testing.T) {
	const numEntries = 1000
	m := NewMapOf[int]()
	for i := 0; i < numEntries; i++ {
		m.Store(strconv.Itoa(i), i)
	}
	for i := 0; i < numEntries; i++ {
		if _, loaded := m.LoadAndDelete(strconv.Itoa(i)); !loaded {
			t.Errorf("value was not found for %d", i)
		}
		if _, ok := m.Load(strconv.Itoa(i)); ok {
			t.Errorf("value was not expected for %d", i)
		}
	}
}

func TestIntegerMapOfSerialStoreThenLoadAndDelete(t *testing.T) {
	const numEntries = 1000
	m := NewIntegerMapOf[int, int]()
	for i := 0; i < numEntries; i++ {
		m.Store(i, i)
	}
	for i := 0; i < numEntries; i++ {
		if _, loaded := m.LoadAndDelete(i); !loaded {
			t.Errorf("value was not found for %d", i)
		}
		if _, ok := m.Load(i); ok {
			t.Errorf("value was not expected for %d", i)
		}
	}
}

func TestTypedMapOfSerialStoreThenLoadAndDelete(t *testing.T) {
	type foo struct {
		x int
		y int
	}
	const numEntries = 1000
	m := NewTypedMapOf[foo, int](func(seed maphash.Seed, f foo) uint64 {
		var h maphash.Hash
		h.SetSeed(seed)
		binary.Write(&h, binary.LittleEndian, f)
		return h.Sum64()
	})
	for i := 0; i < numEntries; i++ {
		m.Store(foo{42, i}, i)
	}
	for i := 0; i < numEntries; i++ {
		if _, loaded := m.LoadAndDelete(foo{42, i}); !loaded {
			t.Errorf("value was not found for %d", i)
		}
		if _, ok := m.Load(foo{42, i}); ok {
			t.Errorf("value was not expected for %d", i)
		}
	}
}

func TestMapOfSize(t *testing.T) {
	const numEntries = 1000
	m := NewMapOf[int]()
	size := m.Size()
	if size != 0 {
		t.Errorf("zero size expected: %d", size)
	}
	expectedSize := 0
	for i := 0; i < numEntries; i++ {
		m.Store(strconv.Itoa(i), i)
		expectedSize++
		size := m.Size()
		if size != expectedSize {
			t.Errorf("size of %d was expected, got: %d", expectedSize, size)
		}
	}
	for i := 0; i < numEntries; i++ {
		m.Delete(strconv.Itoa(i))
		expectedSize--
		size := m.Size()
		if size != expectedSize {
			t.Errorf("size of %d was expected, got: %d", expectedSize, size)
		}
	}
}

func TestMapOfResize(t *testing.T) {
	const numEntries = 100_000
	m := NewMapOf[int]()

	for i := 0; i < numEntries; i++ {
		m.Store(strconv.Itoa(i), i)
	}
	stats := CollectMapOfStats(m)
	if stats.Size != numEntries {
		t.Errorf("size was too small: %d", stats.Size)
	}
	expectedCapacity := int(math.RoundToEven(MapLoadFactor+1)) * stats.TableLen * EntriesPerMapBucket
	if stats.Capacity > expectedCapacity {
		t.Errorf("capacity was too large: %d, expected: %d", stats.Capacity, expectedCapacity)
	}
	if stats.TableLen <= MinMapTableLen {
		t.Errorf("table was too small: %d", stats.TableLen)
	}
	if stats.TotalGrowths == 0 {
		t.Errorf("non-zero total growths expected: %d", stats.TotalGrowths)
	}
	if stats.TotalShrinks > 0 {
		t.Errorf("zero total shrinks expected: %d", stats.TotalShrinks)
	}
	// This is useful when debugging table resize and occupancy.
	stats.Print()

	for i := 0; i < numEntries; i++ {
		m.Delete(strconv.Itoa(i))
	}
	stats = CollectMapOfStats(m)
	if stats.Size > 0 {
		t.Errorf("zero size was expected: %d", stats.Size)
	}
	expectedCapacity = stats.TableLen * EntriesPerMapBucket
	if stats.Capacity != expectedCapacity {
		t.Errorf("capacity was too large: %d, expected: %d", stats.Capacity, expectedCapacity)
	}
	if stats.TableLen != MinMapTableLen {
		t.Errorf("table was too large: %d", stats.TableLen)
	}
	if stats.TotalShrinks == 0 {
		t.Errorf("non-zero total shrinks expected: %d", stats.TotalShrinks)
	}
	stats.Print()
}

func TestMapOfResize_CounterLenLimit(t *testing.T) {
	const numEntries = 1_000_000
	m := NewMapOf[string]()

	for i := 0; i < numEntries; i++ {
		m.Store("foo"+strconv.Itoa(i), "bar"+strconv.Itoa(i))
	}
	stats := CollectMapOfStats(m)
	if stats.Size != numEntries {
		t.Errorf("size was too small: %d", stats.Size)
	}
	if stats.CounterLen != MaxMapCounterLen {
		t.Errorf("number of counter stripes was too large: %d, expected: %d",
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
		t.Errorf("unexpected size: %v", s)
	}
}

func parallelRandTypedResizer(t *testing.T, m *MapOf[int, int], numIters, numEntries int, cdone chan bool) {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < numIters; i++ {
		coin := r.Int63n(2)
		for j := 0; j < numEntries; j++ {
			if coin == 1 {
				m.Store(j, j)
			} else {
				m.Delete(j)
			}
		}
	}
	cdone <- true
}

func TestMapOfParallelResize(t *testing.T) {
	const numIters = 1000
	const numEntries = 2 * EntriesPerMapBucket * MinMapTableLen
	m := NewIntegerMapOf[int, int]()
	cdone := make(chan bool)
	go parallelRandTypedResizer(t, m, numIters, numEntries, cdone)
	go parallelRandTypedResizer(t, m, numIters, numEntries, cdone)
	// Wait for the goroutines to finish.
	<-cdone
	<-cdone
	// Verify map contents.
	for i := 0; i < numEntries; i++ {
		v, ok := m.Load(i)
		if !ok {
			// The entry may be deleted and that's ok.
			continue
		}
		if v != i {
			t.Errorf("values do not match for %d: %v", i, v)
		}
	}
	s := m.Size()
	if s > numEntries {
		t.Errorf("unexpected size: %v", s)
	}
	rs := 0
	m.Range(func(key int, value int) bool {
		rs++
		return true
	})
	if s != rs {
		t.Errorf("size does not match number of entries in Range: %v, %v", s, rs)
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
			m := NewMapOf[int]()
			for i := 0; i < benchmarkNumEntries; i++ {
				m.Store(benchmarkKeyPrefix+strconv.Itoa(i), i)
			}
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
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		r := rand.New(rand.NewSource(time.Now().UnixNano()))
		// convert percent to permille to support 99% case
		storeThreshold := 10 * readPercentage
		deleteThreshold := 10*readPercentage + ((1000 - 10*readPercentage) / 2)
		for pb.Next() {
			op := r.Intn(1000)
			i := r.Intn(benchmarkNumEntries)
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
			m := NewIntegerMapOf[int, int]()
			for i := 0; i < benchmarkNumEntries; i++ {
				m.Store(i, i)
			}
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
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		r := rand.New(rand.NewSource(time.Now().UnixNano()))
		// convert percent to permille to support 99% case
		storeThreshold := 10 * readPercentage
		deleteThreshold := 10*readPercentage + ((1000 - 10*readPercentage) / 2)
		for pb.Next() {
			op := r.Intn(1000)
			i := r.Intn(benchmarkNumEntries)
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
	m := NewMapOf[int]()
	for i := 0; i < benchmarkNumEntries; i++ {
		m.Store(benchmarkKeys[i], i)
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
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
