package xsync_test

import (
	"fmt"
	"math"
	"math/bits"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	. "github.com/puzpuzpuz/xsync/v2"
)

const (
	// number of entries to use in benchmarks
	benchmarkNumEntries = 1_000
	// key prefix used in benchmarks
	benchmarkKeyPrefix = "what_a_looooooooooooooooooooooong_key_prefix_"
)

var benchmarkCases = []struct {
	name           string
	readPercentage int
}{
	{"reads=100%", 100}, // 100% loads,    0% stores,    0% deletes
	{"reads=99%", 99},   //  99% loads,  0.5% stores,  0.5% deletes
	{"reads=90%", 90},   //  90% loads,    5% stores,    5% deletes
	{"reads=75%", 75},   //  75% loads, 12.5% stores, 12.5% deletes
}

var benchmarkKeys []string

func init() {
	benchmarkKeys = make([]string, benchmarkNumEntries)
	for i := 0; i < benchmarkNumEntries; i++ {
		benchmarkKeys[i] = benchmarkKeyPrefix + strconv.Itoa(i)
	}
}

func runParallel(b *testing.B, benchFn func(pb *testing.PB)) {
	b.ResetTimer()
	start := time.Now()
	b.RunParallel(benchFn)
	opsPerSec := float64(b.N) / float64(time.Since(start).Seconds())
	b.ReportMetric(opsPerSec, "ops/s")
}

func TestMap_BucketStructSize(t *testing.T) {
	size := unsafe.Sizeof(BucketPadded{})
	if size != 64 {
		t.Fatalf("size of 64B (one cache line) is expected, got: %d", size)
	}
}

func TestMap_UniqueValuePointers_Int(t *testing.T) {
	EnableAssertions()
	m := NewMap()
	v := 42
	m.Store("foo", v)
	m.Store("foo", v)
	DisableAssertions()
}

func TestMap_UniqueValuePointers_Struct(t *testing.T) {
	type foo struct{}
	EnableAssertions()
	m := NewMap()
	v := foo{}
	m.Store("foo", v)
	m.Store("foo", v)
	DisableAssertions()
}

func TestMap_UniqueValuePointers_Pointer(t *testing.T) {
	type foo struct{}
	EnableAssertions()
	m := NewMap()
	v := &foo{}
	m.Store("foo", v)
	m.Store("foo", v)
	DisableAssertions()
}

func TestMap_UniqueValuePointers_Slice(t *testing.T) {
	EnableAssertions()
	m := NewMap()
	v := make([]int, 13)
	m.Store("foo", v)
	m.Store("foo", v)
	DisableAssertions()
}

func TestMap_UniqueValuePointers_String(t *testing.T) {
	EnableAssertions()
	m := NewMap()
	v := "bar"
	m.Store("foo", v)
	m.Store("foo", v)
	DisableAssertions()
}

func TestMap_UniqueValuePointers_Nil(t *testing.T) {
	EnableAssertions()
	m := NewMap()
	m.Store("foo", nil)
	m.Store("foo", nil)
	DisableAssertions()
}

func TestMap_MissingEntry(t *testing.T) {
	m := NewMap()
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

func TestMap_EmptyStringKey(t *testing.T) {
	m := NewMap()
	m.Store("", "foobar")
	v, ok := m.Load("")
	if !ok {
		t.Fatal("value was expected")
	}
	if vs, ok := v.(string); ok && vs != "foobar" {
		t.Fatalf("value does not match: %v", v)
	}
}

func TestMapStore_NilValue(t *testing.T) {
	m := NewMap()
	m.Store("foo", nil)
	v, ok := m.Load("foo")
	if !ok {
		t.Fatal("nil value was expected")
	}
	if v != nil {
		t.Fatalf("value was not nil: %v", v)
	}
}

func TestMapLoadOrStore_NilValue(t *testing.T) {
	m := NewMap()
	m.LoadOrStore("foo", nil)
	v, loaded := m.LoadOrStore("foo", nil)
	if !loaded {
		t.Fatal("nil value was expected")
	}
	if v != nil {
		t.Fatalf("value was not nil: %v", v)
	}
}

func TestMapLoadOrStore_NonNilValue(t *testing.T) {
	type foo struct{}
	m := NewMap()
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

func TestMapLoadAndStore_NilValue(t *testing.T) {
	m := NewMap()
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

func TestMapLoadAndStore_NonNilValue(t *testing.T) {
	type foo struct{}
	m := NewMap()
	v1 := &foo{}
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

func TestMapRange(t *testing.T) {
	const numEntries = 1000
	m := NewMap()
	for i := 0; i < numEntries; i++ {
		m.Store(strconv.Itoa(i), i)
	}
	iters := 0
	met := make(map[string]int)
	m.Range(func(key string, value interface{}) bool {
		if key != strconv.Itoa(value.(int)) {
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
			t.Fatalf("met key %d multiple times: %d", i, c)
		}
	}
}

func TestMapRange_FalseReturned(t *testing.T) {
	m := NewMap()
	for i := 0; i < 100; i++ {
		m.Store(strconv.Itoa(i), i)
	}
	iters := 0
	m.Range(func(key string, value interface{}) bool {
		iters++
		return iters != 13
	})
	if iters != 13 {
		t.Fatalf("got unexpected number of iterations: %d", iters)
	}
}

func TestMapRange_NestedDelete(t *testing.T) {
	const numEntries = 256
	m := NewMap()
	for i := 0; i < numEntries; i++ {
		m.Store(strconv.Itoa(i), i)
	}
	m.Range(func(key string, value interface{}) bool {
		m.Delete(key)
		return true
	})
	for i := 0; i < numEntries; i++ {
		if _, ok := m.Load(strconv.Itoa(i)); ok {
			t.Fatalf("value found for %d", i)
		}
	}
}

func TestMapStore(t *testing.T) {
	const numEntries = 128
	m := NewMap()
	for i := 0; i < numEntries; i++ {
		m.Store(strconv.Itoa(i), i)
	}
	for i := 0; i < numEntries; i++ {
		v, ok := m.Load(strconv.Itoa(i))
		if !ok {
			t.Fatalf("value not found for %d", i)
		}
		if vi, ok := v.(int); ok && vi != i {
			t.Fatalf("values do not match for %d: %v", i, v)
		}
	}
}

func TestMapLoadOrStore(t *testing.T) {
	const numEntries = 1000
	m := NewMap()
	for i := 0; i < numEntries; i++ {
		m.Store(strconv.Itoa(i), i)
	}
	for i := 0; i < numEntries; i++ {
		if _, loaded := m.LoadOrStore(strconv.Itoa(i), i); !loaded {
			t.Fatalf("value not found for %d", i)
		}
	}
}

func TestMapLoadOrCompute(t *testing.T) {
	const numEntries = 1000
	m := NewMap()
	for i := 0; i < numEntries; i++ {
		v, loaded := m.LoadOrCompute(strconv.Itoa(i), func() interface{} {
			return i
		})
		if loaded {
			t.Fatalf("value not computed for %d", i)
		}
		if vi, ok := v.(int); ok && vi != i {
			t.Fatalf("values do not match for %d: %v", i, v)
		}
	}
	for i := 0; i < numEntries; i++ {
		v, loaded := m.LoadOrCompute(strconv.Itoa(i), func() interface{} {
			return i
		})
		if !loaded {
			t.Fatalf("value not loaded for %d", i)
		}
		if vi, ok := v.(int); ok && vi != i {
			t.Fatalf("values do not match for %d: %v", i, v)
		}
	}
}

func TestMapLoadOrCompute_FunctionCalledOnce(t *testing.T) {
	m := NewMap()
	for i := 0; i < 100; {
		m.LoadOrCompute(strconv.Itoa(i), func() (v interface{}) {
			v, i = i, i+1
			return v
		})
	}

	m.Range(func(k string, v interface{}) bool {
		if vi, ok := v.(int); !ok || strconv.Itoa(vi) != k {
			t.Fatalf("%sth key is not equal to value %d", k, v)
		}
		return true
	})
}

func TestMapCompute(t *testing.T) {
	var zeroedV interface{}
	m := NewMap()
	// Store a new value.
	v, ok := m.Compute("foobar", func(oldValue interface{}, loaded bool) (newValue interface{}, delete bool) {
		if oldValue != zeroedV {
			t.Fatalf("oldValue should be empty interface{} when computing a new value: %d", oldValue)
		}
		if loaded {
			t.Fatal("loaded should be false when computing a new value")
		}
		newValue = 42
		delete = false
		return
	})
	if v.(int) != 42 {
		t.Fatalf("v should be 42 when computing a new value: %d", v)
	}
	if !ok {
		t.Fatal("ok should be true when computing a new value")
	}
	// Update an existing value.
	v, ok = m.Compute("foobar", func(oldValue interface{}, loaded bool) (newValue interface{}, delete bool) {
		if oldValue.(int) != 42 {
			t.Fatalf("oldValue should be 42 when updating the value: %d", oldValue)
		}
		if !loaded {
			t.Fatal("loaded should be true when updating the value")
		}
		newValue = oldValue.(int) + 42
		delete = false
		return
	})
	if v.(int) != 84 {
		t.Fatalf("v should be 84 when updating the value: %d", v)
	}
	if !ok {
		t.Fatal("ok should be true when updating the value")
	}
	// Delete an existing value.
	v, ok = m.Compute("foobar", func(oldValue interface{}, loaded bool) (newValue interface{}, delete bool) {
		if oldValue != 84 {
			t.Fatalf("oldValue should be 84 when deleting the value: %d", oldValue)
		}
		if !loaded {
			t.Fatal("loaded should be true when deleting the value")
		}
		delete = true
		return
	})
	if v.(int) != 84 {
		t.Fatalf("v should be 84 when deleting the value: %d", v)
	}
	if ok {
		t.Fatal("ok should be false when deleting the value")
	}
	// Try to delete a non-existing value. Notice different key.
	v, ok = m.Compute("barbaz", func(oldValue interface{}, loaded bool) (newValue interface{}, delete bool) {
		var zeroedV interface{}
		if oldValue != zeroedV {
			t.Fatalf("oldValue should be empty interface{} when trying to delete a non-existing value: %d", oldValue)
		}
		if loaded {
			t.Fatal("loaded should be false when trying to delete a non-existing value")
		}
		// We're returning a non-zero value, but the map should ignore it.
		newValue = 42
		delete = true
		return
	})
	if v != zeroedV {
		t.Fatalf("v should be empty interface{} when trying to delete a non-existing value: %d", v)
	}
	if ok {
		t.Fatal("ok should be false when trying to delete a non-existing value")
	}
}

func TestMapStoreThenDelete(t *testing.T) {
	const numEntries = 1000
	m := NewMap()
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

func TestMapStoreThenLoadAndDelete(t *testing.T) {
	const numEntries = 1000
	m := NewMap()
	for i := 0; i < numEntries; i++ {
		m.Store(strconv.Itoa(i), i)
	}
	for i := 0; i < numEntries; i++ {
		if v, loaded := m.LoadAndDelete(strconv.Itoa(i)); !loaded || v.(int) != i {
			t.Fatalf("value was not found or different for %d: %v", i, v)
		}
		if _, ok := m.Load(strconv.Itoa(i)); ok {
			t.Fatalf("value was not expected for %d", i)
		}
	}
}

func sizeBasedOnRange(m *Map) int {
	size := 0
	m.Range(func(key string, value interface{}) bool {
		size++
		return true
	})
	return size
}

func TestMapSize(t *testing.T) {
	const numEntries = 1000
	m := NewMap()
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
		rsize := sizeBasedOnRange(m)
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
		rsize := sizeBasedOnRange(m)
		if size != rsize {
			t.Fatalf("size does not match number of entries in Range: %v, %v", size, rsize)
		}
	}
}

func TestMapClear(t *testing.T) {
	const numEntries = 1000
	m := NewMap()
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
	rsize := sizeBasedOnRange(m)
	if rsize != 0 {
		t.Fatalf("zero number of entries in Range was expected, got: %d", rsize)
	}
}

func assertMapCapacity(t *testing.T, m *Map, expectedCap int) {
	stats := CollectMapStats(m)
	if stats.Capacity != expectedCap {
		t.Fatalf("capacity was different from %d: %d", expectedCap, stats.Capacity)
	}
}

func TestNewMapPresized(t *testing.T) {
	assertMapCapacity(t, NewMap(), MinMapTableCap)
	assertMapCapacity(t, NewMapPresized(1000), 1536)
	assertMapCapacity(t, NewMapPresized(0), MinMapTableCap)
	assertMapCapacity(t, NewMapPresized(-1), MinMapTableCap)
}

func TestMapResize(t *testing.T) {
	const numEntries = 100_000
	m := NewMap()

	for i := 0; i < numEntries; i++ {
		m.Store(strconv.Itoa(i), i)
	}
	stats := CollectMapStats(m)
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
	stats = CollectMapStats(m)
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

func TestMapResize_CounterLenLimit(t *testing.T) {
	const numEntries = 1_000_000
	m := NewMap()

	for i := 0; i < numEntries; i++ {
		m.Store("foo"+strconv.Itoa(i), "bar"+strconv.Itoa(i))
	}
	stats := CollectMapStats(m)
	if stats.Size != numEntries {
		t.Fatalf("size was too small: %d", stats.Size)
	}
	if stats.CounterLen != MaxMapCounterLen {
		t.Fatalf("number of counter stripes was too large: %d, expected: %d",
			stats.CounterLen, MaxMapCounterLen)
	}
}

func parallelSeqResizer(t *testing.T, m *Map, numEntries int, positive bool, cdone chan bool) {
	for i := 0; i < numEntries; i++ {
		if positive {
			m.Store(strconv.Itoa(i), i)
		} else {
			m.Store(strconv.Itoa(-i), -i)
		}
	}
	cdone <- true
}

func TestMapParallelResize_GrowOnly(t *testing.T) {
	const numEntries = 100_000
	m := NewMap()
	cdone := make(chan bool)
	go parallelSeqResizer(t, m, numEntries, true, cdone)
	go parallelSeqResizer(t, m, numEntries, false, cdone)
	// Wait for the goroutines to finish.
	<-cdone
	<-cdone
	// Verify map contents.
	for i := -numEntries + 1; i < numEntries; i++ {
		v, ok := m.Load(strconv.Itoa(i))
		if !ok {
			t.Fatalf("value not found for %d", i)
		}
		if vi, ok := v.(int); ok && vi != i {
			t.Fatalf("values do not match for %d: %v", i, v)
		}
	}
	if s := m.Size(); s != 2*numEntries-1 {
		t.Fatalf("unexpected size: %v", s)
	}
}

func parallelRandResizer(t *testing.T, m *Map, numIters, numEntries int, cdone chan bool) {
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

func TestMapParallelResize(t *testing.T) {
	const numIters = 1_000
	const numEntries = 2 * EntriesPerMapBucket * MinMapTableLen
	m := NewMap()
	cdone := make(chan bool)
	go parallelRandResizer(t, m, numIters, numEntries, cdone)
	go parallelRandResizer(t, m, numIters, numEntries, cdone)
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
		if vi, ok := v.(int); ok && vi != i {
			t.Fatalf("values do not match for %d: %v", i, v)
		}
	}
	s := m.Size()
	if s > numEntries {
		t.Fatalf("unexpected size: %v", s)
	}
	rs := sizeBasedOnRange(m)
	if s != rs {
		t.Fatalf("size does not match number of entries in Range: %v, %v", s, rs)
	}
}

func parallelRandClearer(t *testing.T, m *Map, numIters, numEntries int, cdone chan bool) {
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

func TestMapParallelClear(t *testing.T) {
	const numIters = 100
	const numEntries = 1_000
	m := NewMap()
	cdone := make(chan bool)
	go parallelRandClearer(t, m, numIters, numEntries, cdone)
	go parallelRandClearer(t, m, numIters, numEntries, cdone)
	// Wait for the goroutines to finish.
	<-cdone
	<-cdone
	// Verify map size.
	s := m.Size()
	if s > numEntries {
		t.Fatalf("unexpected size: %v", s)
	}
	rs := sizeBasedOnRange(m)
	if s != rs {
		t.Fatalf("size does not match number of entries in Range: %v, %v", s, rs)
	}
}

func parallelSeqStorer(t *testing.T, m *Map, storeEach, numIters, numEntries int, cdone chan bool) {
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
				if vi, ok := v.(int); !ok || vi != j {
					t.Errorf("value was not expected for %d: %d", j, vi)
					break
				}
			}
		}
	}
	cdone <- true
}

func TestMapParallelStores(t *testing.T) {
	const numStorers = 4
	const numIters = 10_000
	const numEntries = 100
	m := NewMap()
	cdone := make(chan bool)
	for i := 0; i < numStorers; i++ {
		go parallelSeqStorer(t, m, i, numIters, numEntries, cdone)
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
		if vi, ok := v.(int); ok && vi != i {
			t.Fatalf("values do not match for %d: %v", i, v)
		}
	}
}

func parallelRandStorer(t *testing.T, m *Map, numIters, numEntries int, cdone chan bool) {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < numIters; i++ {
		j := r.Intn(numEntries)
		if v, loaded := m.LoadOrStore(strconv.Itoa(j), j); loaded {
			if vi, ok := v.(int); !ok || vi != j {
				t.Errorf("value was not expected for %d: %d", j, vi)
			}
		}
	}
	cdone <- true
}

func parallelRandDeleter(t *testing.T, m *Map, numIters, numEntries int, cdone chan bool) {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < numIters; i++ {
		j := r.Intn(numEntries)
		if v, loaded := m.LoadAndDelete(strconv.Itoa(j)); loaded {
			if vi, ok := v.(int); !ok || vi != j {
				t.Errorf("value was not expected for %d: %d", j, vi)
			}
		}
	}
	cdone <- true
}

func parallelLoader(t *testing.T, m *Map, numIters, numEntries int, cdone chan bool) {
	for i := 0; i < numIters; i++ {
		for j := 0; j < numEntries; j++ {
			// Due to atomic snapshots we must either see no entry, or a "<j>"/j pair.
			if v, ok := m.Load(strconv.Itoa(j)); ok {
				if vi, ok := v.(int); !ok || vi != j {
					t.Errorf("value was not expected for %d: %d", j, vi)
				}
			}
		}
	}
	cdone <- true
}

func TestMapAtomicSnapshot(t *testing.T) {
	const numIters = 100_000
	const numEntries = 100
	m := NewMap()
	cdone := make(chan bool)
	// Update or delete random entry in parallel with loads.
	go parallelRandStorer(t, m, numIters, numEntries, cdone)
	go parallelRandDeleter(t, m, numIters, numEntries, cdone)
	go parallelLoader(t, m, numIters, numEntries, cdone)
	// Wait for the goroutines to finish.
	for i := 0; i < 3; i++ {
		<-cdone
	}
}

func TestMapParallelStoresAndDeletes(t *testing.T) {
	const numWorkers = 2
	const numIters = 100_000
	const numEntries = 1000
	m := NewMap()
	cdone := make(chan bool)
	// Update random entry in parallel with deletes.
	for i := 0; i < numWorkers; i++ {
		go parallelRandStorer(t, m, numIters, numEntries, cdone)
		go parallelRandDeleter(t, m, numIters, numEntries, cdone)
	}
	// Wait for the goroutines to finish.
	for i := 0; i < 2*numWorkers; i++ {
		<-cdone
	}
}

func parallelComputer(t *testing.T, m *Map, numIters, numEntries int, cdone chan bool) {
	for i := 0; i < numIters; i++ {
		for j := 0; j < numEntries; j++ {
			m.Compute(strconv.Itoa(j), func(oldValue interface{}, loaded bool) (newValue interface{}, delete bool) {
				if !loaded {
					return uint64(1), false
				}
				return uint64(oldValue.(uint64) + 1), false
			})
		}
	}
	cdone <- true
}

func TestMapParallelComputes(t *testing.T) {
	const numWorkers = 4 // Also stands for numEntries.
	const numIters = 10_000
	m := NewMap()
	cdone := make(chan bool)
	for i := 0; i < numWorkers; i++ {
		go parallelComputer(t, m, numIters, numWorkers, cdone)
	}
	// Wait for the goroutines to finish.
	for i := 0; i < numWorkers; i++ {
		<-cdone
	}
	// Verify map contents.
	for i := 0; i < numWorkers; i++ {
		v, ok := m.Load(strconv.Itoa(i))
		if !ok {
			t.Fatalf("value not found for %d", i)
		}
		if v.(uint64) != numWorkers*numIters {
			t.Fatalf("values do not match for %d: %v", i, v)
		}
	}
}

func parallelRangeStorer(t *testing.T, m *Map, numEntries int, stopFlag *int64, cdone chan bool) {
	for {
		for i := 0; i < numEntries; i++ {
			m.Store(strconv.Itoa(i), i)
		}
		if atomic.LoadInt64(stopFlag) != 0 {
			break
		}
	}
	cdone <- true
}

func parallelRangeDeleter(t *testing.T, m *Map, numEntries int, stopFlag *int64, cdone chan bool) {
	for {
		for i := 0; i < numEntries; i++ {
			m.Delete(strconv.Itoa(i))
		}
		if atomic.LoadInt64(stopFlag) != 0 {
			break
		}
	}
	cdone <- true
}

func TestMapParallelRange(t *testing.T) {
	const numEntries = 10_000
	m := NewMap()
	for i := 0; i < numEntries; i++ {
		m.Store(strconv.Itoa(i), i)
	}
	// Start goroutines that would be storing and deleting items in parallel.
	cdone := make(chan bool)
	stopFlag := int64(0)
	go parallelRangeStorer(t, m, numEntries, &stopFlag, cdone)
	go parallelRangeDeleter(t, m, numEntries, &stopFlag, cdone)
	// Iterate the map and verify that no duplicate keys were met.
	met := make(map[string]int)
	m.Range(func(key string, value interface{}) bool {
		if key != strconv.Itoa(value.(int)) {
			t.Fatalf("got unexpected value for key %s: %v", key, value)
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
			t.Fatalf("met key %s multiple times: %d", k, c)
		}
	}
	// Make sure that both goroutines finish.
	atomic.StoreInt64(&stopFlag, 1)
	<-cdone
	<-cdone
}

func TestMapTopHashMutex(t *testing.T) {
	const (
		numLockers    = 4
		numIterations = 1000
	)
	var (
		activity int32
		mu       uint64
	)
	cdone := make(chan bool)
	for i := 0; i < numLockers; i++ {
		go func() {
			for i := 0; i < numIterations; i++ {
				LockBucket(&mu)
				n := atomic.AddInt32(&activity, 1)
				if n != 1 {
					UnlockBucket(&mu)
					panic(fmt.Sprintf("lock(%d)\n", n))
				}
				atomic.AddInt32(&activity, -1)
				UnlockBucket(&mu)
			}
			cdone <- true
		}()
	}
	// Wait for all lockers to finish.
	for i := 0; i < numLockers; i++ {
		<-cdone
	}
}

func TestMapTopHashMutex_Store_NoLock(t *testing.T) {
	mu := uint64(0)
	testMapTopHashMutex_Store(t, &mu)
}

func TestMapTopHashMutex_Store_WhileLocked(t *testing.T) {
	mu := uint64(0)
	LockBucket(&mu)
	defer UnlockBucket(&mu)
	testMapTopHashMutex_Store(t, &mu)
}

func testMapTopHashMutex_Store(t *testing.T, topHashes *uint64) {
	hash := uint64(0b1101_0100_1010_1011_1101 << 44)
	for i := 0; i < EntriesPerMapBucket; i++ {
		if TopHashMatch(hash, *topHashes, i) {
			t.Fatalf("top hash match for all zeros for index %d", i)
		}

		prevOnes := bits.OnesCount64(*topHashes)
		*topHashes = StoreTopHash(hash, *topHashes, i)
		newOnes := bits.OnesCount64(*topHashes)
		expectedInc := bits.OnesCount64(hash) + 1
		if newOnes != prevOnes+expectedInc {
			t.Fatalf("unexpected bits change after store for index %d: %d, %d, %#b",
				i, newOnes, prevOnes+expectedInc, *topHashes)
		}

		if !TopHashMatch(hash, *topHashes, i) {
			t.Fatalf("top hash mismatch after store for index %d: %#b", i, *topHashes)
		}
	}
}

func TestMapTopHashMutex_Erase_NoLock(t *testing.T) {
	mu := uint64(0)
	testMapTopHashMutex_Erase(t, &mu)
}

func TestMapTopHashMutex_Erase_WhileLocked(t *testing.T) {
	mu := uint64(0)
	LockBucket(&mu)
	defer UnlockBucket(&mu)
	testMapTopHashMutex_Erase(t, &mu)
}

func testMapTopHashMutex_Erase(t *testing.T, topHashes *uint64) {
	hash := uint64(0xababaaaaaaaaaaaa) // top hash is 1010_1011_1010_1011_1010
	for i := 0; i < EntriesPerMapBucket; i++ {
		*topHashes = StoreTopHash(hash, *topHashes, i)
		ones := bits.OnesCount64(*topHashes)

		*topHashes = EraseTopHash(*topHashes, i)
		if TopHashMatch(hash, *topHashes, i) {
			t.Fatalf("top hash match after erase for index %d: %#b", i, *topHashes)
		}

		erasedBits := ones - bits.OnesCount64(*topHashes)
		if erasedBits != 1 {
			t.Fatalf("more than one bit changed after erase: %d, %d", i, erasedBits)
		}
	}
}

func TestMapTopHashMutex_StoreAfterErase_NoLock(t *testing.T) {
	mu := uint64(0)
	testMapTopHashMutex_StoreAfterErase(t, &mu)
}

func TestMapTopHashMutex_StoreAfterErase_WhileLocked(t *testing.T) {
	mu := uint64(0)
	LockBucket(&mu)
	defer UnlockBucket(&mu)
	testMapTopHashMutex_StoreAfterErase(t, &mu)
}

func testMapTopHashMutex_StoreAfterErase(t *testing.T, topHashes *uint64) {
	hashOne := uint64(0b1101_0100_1101_0100_1101_1111 << 40) // top hash is 1101_0100_1101_0100_1101
	hashTwo := uint64(0b1010_1011_1010_1011_1010_1111 << 40) // top hash is 1010_1011_1010_1011_1010
	idx := 2

	*topHashes = StoreTopHash(hashOne, *topHashes, idx)
	if !TopHashMatch(hashOne, *topHashes, idx) {
		t.Fatalf("top hash mismatch for hash one: %#b, %#b", hashOne, *topHashes)
	}
	if TopHashMatch(hashTwo, *topHashes, idx) {
		t.Fatalf("top hash match for hash two: %#b, %#b", hashTwo, *topHashes)
	}

	*topHashes = EraseTopHash(*topHashes, idx)
	*topHashes = StoreTopHash(hashTwo, *topHashes, idx)
	if TopHashMatch(hashOne, *topHashes, idx) {
		t.Fatalf("top hash match for hash one: %#b, %#b", hashOne, *topHashes)
	}
	if !TopHashMatch(hashTwo, *topHashes, idx) {
		t.Fatalf("top hash mismatch for hash two: %#b, %#b", hashTwo, *topHashes)
	}
}

func BenchmarkMap_NoWarmUp(b *testing.B) {
	for _, bc := range benchmarkCases {
		if bc.readPercentage == 100 {
			// This benchmark doesn't make sense without a warm-up.
			continue
		}
		b.Run(bc.name, func(b *testing.B) {
			m := NewMap()
			benchmarkMap(b, func(k string) (interface{}, bool) {
				return m.Load(k)
			}, func(k string, v interface{}) {
				m.Store(k, v)
			}, func(k string) {
				m.Delete(k)
			}, bc.readPercentage)
		})
	}
}

func BenchmarkMapStandard_NoWarmUp(b *testing.B) {
	for _, bc := range benchmarkCases {
		if bc.readPercentage == 100 {
			// This benchmark doesn't make sense without a warm-up.
			continue
		}
		b.Run(bc.name, func(b *testing.B) {
			var m sync.Map
			benchmarkMap(b, func(k string) (interface{}, bool) {
				return m.Load(k)
			}, func(k string, v interface{}) {
				m.Store(k, v)
			}, func(k string) {
				m.Delete(k)
			}, bc.readPercentage)
		})
	}
}

func BenchmarkMap_WarmUp(b *testing.B) {
	for _, bc := range benchmarkCases {
		b.Run(bc.name, func(b *testing.B) {
			m := NewMapPresized(benchmarkNumEntries)
			for i := 0; i < benchmarkNumEntries; i++ {
				m.Store(benchmarkKeyPrefix+strconv.Itoa(i), i)
			}
			b.ResetTimer()
			benchmarkMap(b, func(k string) (interface{}, bool) {
				return m.Load(k)
			}, func(k string, v interface{}) {
				m.Store(k, v)
			}, func(k string) {
				m.Delete(k)
			}, bc.readPercentage)
		})
	}
}

// This is a nice scenario for sync.Map since a lot of updates
// will hit the readOnly part of the map.
func BenchmarkMapStandard_WarmUp(b *testing.B) {
	for _, bc := range benchmarkCases {
		b.Run(bc.name, func(b *testing.B) {
			var m sync.Map
			for i := 0; i < benchmarkNumEntries; i++ {
				m.Store(benchmarkKeyPrefix+strconv.Itoa(i), i)
			}
			b.ResetTimer()
			benchmarkMap(b, func(k string) (interface{}, bool) {
				return m.Load(k)
			}, func(k string, v interface{}) {
				m.Store(k, v)
			}, func(k string) {
				m.Delete(k)
			}, bc.readPercentage)
		})
	}
}

func benchmarkMap(
	b *testing.B,
	loadFn func(k string) (interface{}, bool),
	storeFn func(k string, v interface{}),
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

func BenchmarkMapRange(b *testing.B) {
	m := NewMap()
	for i := 0; i < benchmarkNumEntries; i++ {
		m.Store(benchmarkKeys[i], i)
	}
	b.ResetTimer()
	runParallel(b, func(pb *testing.PB) {
		foo := 0
		for pb.Next() {
			m.Range(func(key string, value interface{}) bool {
				// Dereference the value to have an apple-to-apple
				// comparison with MapOf.Range.
				_ = value.(int)
				foo++
				return true
			})
			_ = foo
		}
	})
}

func BenchmarkMapRangeStandard(b *testing.B) {
	var m sync.Map
	for i := 0; i < benchmarkNumEntries; i++ {
		m.Store(benchmarkKeys[i], i)
	}
	b.ResetTimer()
	runParallel(b, func(pb *testing.PB) {
		foo := 0
		for pb.Next() {
			m.Range(func(key interface{}, value interface{}) bool {
				// Dereference the key and the value to have an apple-to-apple
				// comparison with MapOf.Range.
				_, _ = key.(string), value.(int)
				foo++
				return true
			})
			_ = foo
		}
	})
}
