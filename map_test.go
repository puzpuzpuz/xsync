package xsync_test

import (
	"math"
	"math/rand"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	. "github.com/puzpuzpuz/xsync/v4"
)

const (
	// number of entries to use in benchmarks
	benchmarkNumEntries = 1_000
	// key prefix used in benchmarks
	benchmarkKeyPrefix = "what_a_looooooooooooooooooooooong_key_prefix_"
)

type point struct {
	x int32
	y int32
}

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
	for i := range benchmarkNumEntries {
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
	size = unsafe.Sizeof(BucketPadded{})
	if size != 64 {
		t.Fatalf("size of 64B (one cache line) is expected, got: %d", size)
	}
}

func TestMap_MissingEntry(t *testing.T) {
	m := NewMap[string, string]()
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
	m := NewMap[string, string]()
	m.Store("", "foobar")
	v, ok := m.Load("")
	if !ok {
		t.Fatal("value was expected")
	}
	if v != "foobar" {
		t.Fatalf("value does not match: %v", v)
	}
}

func TestMapStore_NilValue(t *testing.T) {
	m := NewMap[string, *struct{}]()
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
	m := NewMap[string, *struct{}]()
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
	m := NewMap[string, *foo]()
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
	m := NewMap[string, *struct{}]()
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
	m := NewMap[string, int]()
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

func TestMapAll(t *testing.T) {
	m := NewMap[string, int]()
	for range m.All() {
		t.Fatal("got an iteration on empty map")
	}

	for i := range 1000 {
		m.Store(strconv.Itoa(i), i)
	}

	iters := 0
	met := make(map[string]int)
	for key, value := range m.All() {
		if key != strconv.Itoa(value) {
			t.Fatalf("got unexpected key/value for iteration %d: %v/%v", iters, key, value)
			break
		}
		met[key] += 1
		iters++
	}

	if iters != 1000 {
		t.Fatalf("got unexpected number of iterations: %d", iters)
	}
	for i := range 1000 {
		if c := met[strconv.Itoa(i)]; c != 1 {
			t.Fatalf("range did not iterate correctly over %d: %d", i, c)
		}
	}
}

func TestMapAll_Break(t *testing.T) {
	m := NewMap[string, int]()
	for i := range 100 {
		m.Store(strconv.Itoa(i), i)
	}

	iters := 0
	for range m.All() {
		iters++

		if iters == 50 {
			break
		}
	}

	if iters != 50 {
		t.Fatalf("got unexpected number of iterations: %d", iters)
	}
}

func TestMapAll_NestedDelete(t *testing.T) {
	const numEntries = 256
	m := NewMap[string, int]()
	for i := range numEntries {
		m.Store(strconv.Itoa(i), i)
	}

	for key := range m.All() {
		m.Delete(key)
	}

	for i := range numEntries {
		if _, ok := m.Load(strconv.Itoa(i)); ok {
			t.Fatalf("value found for %d", i)
		}
	}
}

func TestMapRange(t *testing.T) {
	const numEntries = 1000
	m := NewMap[string, int]()
	for i := range numEntries {
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
	for i := range numEntries {
		if c := met[strconv.Itoa(i)]; c != 1 {
			t.Fatalf("range did not iterate correctly over %d: %d", i, c)
		}
	}
}

func TestMapRange_FalseReturned(t *testing.T) {
	m := NewMap[string, int]()
	for i := range 100 {
		m.Store(strconv.Itoa(i), i)
	}
	iters := 0
	m.Range(func(key string, value int) bool {
		if key != strconv.Itoa(value) {
			t.Fatalf("got unexpected key/value for iteration %d: %v/%v", iters, key, value)
		}
		iters++
		return iters != 13
	})
	if iters != 13 {
		t.Fatalf("got unexpected number of iterations: %d", iters)
	}
}

func TestMapRange_NestedDelete(t *testing.T) {
	const numEntries = 256
	m := NewMap[string, int]()
	for i := range numEntries {
		m.Store(strconv.Itoa(i), i)
	}
	m.Range(func(key string, value int) bool {
		if key != strconv.Itoa(value) {
			t.Fatalf("got unexpected key/value: %v/%v", key, value)
		}
		m.Delete(key)
		return true
	})
	for i := range numEntries {
		if _, ok := m.Load(strconv.Itoa(i)); ok {
			t.Fatalf("value found for %d", i)
		}
	}
}

func TestMapRangeRelaxed(t *testing.T) {
	const numEntries = 1000
	m := NewMap[string, int]()
	for i := range numEntries {
		m.Store(strconv.Itoa(i), i)
	}
	iters := 0
	met := make(map[string]int)
	m.RangeRelaxed(func(key string, value int) bool {
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
	for i := range numEntries {
		if c := met[strconv.Itoa(i)]; c != 1 {
			t.Fatalf("range did not iterate correctly over %d: %d", i, c)
		}
	}
}

func TestMapRangeRelaxed_FalseReturned(t *testing.T) {
	m := NewMap[string, int]()
	for i := range 100 {
		m.Store(strconv.Itoa(i), i)
	}
	iters := 0
	m.RangeRelaxed(func(key string, value int) bool {
		if key != strconv.Itoa(value) {
			t.Fatalf("got unexpected key/value for iteration %d: %v/%v", iters, key, value)
		}
		iters++
		return iters != 13
	})
	if iters != 13 {
		t.Fatalf("got unexpected number of iterations: %d", iters)
	}
}

func TestMapRangeRelaxed_NestedDelete(t *testing.T) {
	const numEntries = 256
	m := NewMap[string, int]()
	for i := range numEntries {
		m.Store(strconv.Itoa(i), i)
	}
	m.RangeRelaxed(func(key string, value int) bool {
		if key != strconv.Itoa(value) {
			t.Fatalf("got unexpected key/value: %v/%v", key, value)
		}
		m.Delete(key)
		return true
	})
	for i := range numEntries {
		if _, ok := m.Load(strconv.Itoa(i)); ok {
			t.Fatalf("value found for %d", i)
		}
	}
}

func TestMapAllRelaxed(t *testing.T) {
	const numEntries = 1000
	m := NewMap[string, int]()
	for i := range numEntries {
		m.Store(strconv.Itoa(i), i)
	}
	iters := 0
	met := make(map[string]int)
	for key, value := range m.AllRelaxed() {
		if key != strconv.Itoa(value) {
			t.Fatalf("got unexpected key/value for iteration %d: %v/%v", iters, key, value)
		}
		met[key] += 1
		iters++
	}
	if iters != numEntries {
		t.Fatalf("got unexpected number of iterations: %d", iters)
	}
	for i := range numEntries {
		if c := met[strconv.Itoa(i)]; c != 1 {
			t.Fatalf("all did not iterate correctly over %d: %d", i, c)
		}
	}
}

func TestMapDeleteMatching(t *testing.T) {
	const numEntries = 1000
	m := NewMap[string, int]()
	for i := range numEntries {
		m.Store(strconv.Itoa(i), i)
	}
	// Delete even values.
	deleted := m.DeleteMatching(func(key string, value int) (del, stop bool) {
		return value%2 == 0, false
	})
	if deleted != numEntries/2 {
		t.Fatalf("expected %d deleted, got %d", numEntries/2, deleted)
	}
	if m.Size() != numEntries/2 {
		t.Fatalf("expected size %d, got %d", numEntries/2, m.Size())
	}
	// Verify only odd values remain.
	for i := range numEntries {
		_, ok := m.Load(strconv.Itoa(i))
		if i%2 == 0 && ok {
			t.Fatalf("even value %d should have been deleted", i)
		}
		if i%2 != 0 && !ok {
			t.Fatalf("odd value %d should not have been deleted", i)
		}
	}
}

func TestMapDeleteMatching_Cancel(t *testing.T) {
	const numEntries = 100
	m := NewMap[string, int]()
	for i := range numEntries {
		m.Store(strconv.Itoa(i), i)
	}
	// Delete entries and cancel after 10 deletions.
	callCount := 0
	deleted := m.DeleteMatching(func(key string, value int) (del, stop bool) {
		callCount++
		if callCount == 10 {
			return true, true // delete this one and cancel
		}
		return true, false
	})
	if deleted != 10 {
		t.Fatalf("expected 10 deleted, got %d", deleted)
	}
	if callCount != 10 {
		t.Fatalf("expected f to be called 10 times, got %d", callCount)
	}
	if m.Size() != numEntries-10 {
		t.Fatalf("expected size %d, got %d", numEntries-10, m.Size())
	}
}

func TestMapDeleteMatching_EmptyMap(t *testing.T) {
	m := NewMap[string, int]()
	callCount := 0
	deleted := m.DeleteMatching(func(key string, value int) (del, stop bool) {
		callCount++
		return false, false
	})
	if deleted != 0 {
		t.Fatalf("expected 0 deleted on empty map, got %d", deleted)
	}
	if callCount != 0 {
		t.Fatalf("expected f to be called 0 times, got %d", callCount)
	}
}

func TestMapDeleteMatching_NoDeletions(t *testing.T) {
	const numEntries = 100
	m := NewMap[string, int]()
	for i := range numEntries {
		m.Store(strconv.Itoa(i), i)
	}
	callCount := 0
	deleted := m.DeleteMatching(func(key string, value int) (del, stop bool) {
		callCount++
		return false, false // never delete
	})
	if deleted != 0 {
		t.Fatalf("expected 0 deleted, got %d", deleted)
	}
	if callCount != numEntries {
		t.Fatalf("expected f to be called %d times, got %d", numEntries, callCount)
	}
	if m.Size() != numEntries {
		t.Fatalf("expected size %d, got %d", numEntries, m.Size())
	}
}

func TestMapDeleteMatching_AllDeleted(t *testing.T) {
	const numEntries = 256
	m := NewMap[string, int]()
	for i := range numEntries {
		m.Store(strconv.Itoa(i), i)
	}
	deleted := m.DeleteMatching(func(key string, value int) (del, stop bool) {
		return true, false // delete all
	})
	if deleted != numEntries {
		t.Fatalf("expected %d deleted, got %d", numEntries, deleted)
	}
	if m.Size() != 0 {
		t.Fatalf("expected size 0, got %d", m.Size())
	}
}

func testParallelRangeRelaxed(t *testing.T, numGoroutines int) {
	const numEntries = 10000
	const numIterations = 50

	m := NewMap[int, int]()
	for i := range numEntries {
		m.Store(i, i)
	}

	var wg sync.WaitGroup
	var totalIterations atomic.Int64

	// Launch goroutines that iterate using RangeRelaxed.
	for range numGoroutines / 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range numIterations {
				m.RangeRelaxed(func(key int, value int) bool {
					if key != value {
						t.Errorf("key %d != value %d", key, value)
					}
					totalIterations.Add(1)
					return true
				})
			}
		}()
	}

	// Launch goroutines that modify the map.
	for range numGoroutines / 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range numIterations {
				for i := range numEntries {
					m.Store(i, i)
					if i%10 == 0 {
						m.Delete(i)
						m.Store(i, i)
					}
				}
			}
		}()
	}

	wg.Wait()

	if totalIterations.Load() == 0 {
		t.Error("expected some iterations to occur")
	}
}

func TestMapParallelRangeRelaxed(t *testing.T) {
	testParallelRangeRelaxed(t, 2)
	testParallelRangeRelaxed(t, runtime.GOMAXPROCS(0))
	testParallelRangeRelaxed(t, 100)
}

func testParallelDeleteMatching(t *testing.T, numGoroutines int) {
	const numEntries = 10000
	const numIterations = 50

	m := NewMap[int, int]()
	for i := range numEntries {
		m.Store(i, i)
	}

	var wg sync.WaitGroup
	var totalDeleted atomic.Int64

	// Launch goroutines that delete even numbers.
	for range numGoroutines / 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range numIterations {
				deleted := m.DeleteMatching(func(key int, value int) (del, stop bool) {
					return key%2 == 0, false
				})
				totalDeleted.Add(int64(deleted))
			}
		}()
	}

	// Launch goroutines that re-add entries.
	for range numGoroutines / 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range numIterations {
				for i := range numEntries {
					m.Store(i, i)
				}
			}
		}()
	}

	wg.Wait()

	// Verify map is in consistent state.
	size := m.Size()
	rangeCount := 0
	m.Range(func(key int, value int) bool {
		if key != value {
			t.Errorf("key %d != value %d", key, value)
		}
		rangeCount++
		return true
	})
	if size != rangeCount {
		t.Errorf("size %d != range count %d", size, rangeCount)
	}
	if totalDeleted.Load() == 0 {
		t.Error("expected some deletions to occur")
	}
}

func TestMapParallelDeleteMatching(t *testing.T) {
	testParallelDeleteMatching(t, 2)
	testParallelDeleteMatching(t, runtime.GOMAXPROCS(0))
	testParallelDeleteMatching(t, 100)
}

func TestMapStringStore(t *testing.T) {
	const numEntries = 128
	m := NewMap[string, int]()
	for i := range numEntries {
		m.Store(strconv.Itoa(i), i)
	}
	for i := range numEntries {
		v, ok := m.Load(strconv.Itoa(i))
		if !ok {
			t.Fatalf("value not found for %d", i)
		}
		if v != i {
			t.Fatalf("values do not match for %d: %v", i, v)
		}
	}
}

func TestMapIntStore(t *testing.T) {
	const numEntries = 128
	m := NewMap[int, int]()
	for i := range numEntries {
		m.Store(i, i)
	}
	for i := range numEntries {
		v, ok := m.Load(i)
		if !ok {
			t.Fatalf("value not found for %d", i)
		}
		if v != i {
			t.Fatalf("values do not match for %d: %v", i, v)
		}
	}
}

func TestMapStore_Int64Keys(t *testing.T) {
	const numEntries = 128
	m := NewMap[int64, int64]()
	for i := range numEntries {
		m.Store(int64(i), int64(i))
	}
	for i := range numEntries {
		v, ok := m.Load(int64(i))
		if !ok {
			t.Fatalf("value not found for %d", i)
		}
		if v != int64(i) {
			t.Fatalf("values do not match for %d: %v", i, v)
		}
	}
}

func TestMapStore_Uint64Keys(t *testing.T) {
	const numEntries = 128
	m := NewMap[uint64, uint64]()
	for i := range numEntries {
		m.Store(uint64(i), uint64(i))
	}
	for i := range numEntries {
		v, ok := m.Load(uint64(i))
		if !ok {
			t.Fatalf("value not found for %d", i)
		}
		if v != uint64(i) {
			t.Fatalf("values do not match for %d: %v", i, v)
		}
	}
}

func TestMapStore_UintptrKeys(t *testing.T) {
	const numEntries = 128
	m := NewMap[uintptr, uintptr]()
	for i := range numEntries {
		m.Store(uintptr(i), uintptr(i))
	}
	for i := range numEntries {
		v, ok := m.Load(uintptr(i))
		if !ok {
			t.Fatalf("value not found for %d", i)
		}
		if v != uintptr(i) {
			t.Fatalf("values do not match for %d: %v", i, v)
		}
	}
}

func TestMapStore_StructKeys_IntValues(t *testing.T) {
	const numEntries = 128
	m := NewMap[point, int]()
	for i := range numEntries {
		m.Store(point{int32(i), -int32(i)}, i)
	}
	for i := range numEntries {
		v, ok := m.Load(point{int32(i), -int32(i)})
		if !ok {
			t.Fatalf("value not found for %d", i)
		}
		if v != i {
			t.Fatalf("values do not match for %d: %v", i, v)
		}
	}
}

func TestMapStore_StructKeys_StructValues(t *testing.T) {
	const numEntries = 128
	m := NewMap[point, point]()
	for i := range numEntries {
		m.Store(point{int32(i), -int32(i)}, point{-int32(i), int32(i)})
	}
	for i := range numEntries {
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

func TestMapLoadOrStore(t *testing.T) {
	const numEntries = 1000
	m := NewMap[string, int]()
	for i := range numEntries {
		m.Store(strconv.Itoa(i), i)
	}
	for i := range numEntries {
		if _, loaded := m.LoadOrStore(strconv.Itoa(i), i); !loaded {
			t.Fatalf("value not found for %d", i)
		}
	}
}

func TestMapLoadOrCompute(t *testing.T) {
	const numEntries = 1000
	m := NewMap[string, int]()
	for i := range numEntries {
		v, loaded := m.LoadOrCompute(strconv.Itoa(i), func() (newValue int, cancel bool) {
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
	for i := range numEntries {
		v, loaded := m.LoadOrCompute(strconv.Itoa(i), func() (newValue int, cancel bool) {
			return i, false
		})
		if loaded {
			t.Fatalf("value not computed for %d", i)
		}
		if v != i {
			t.Fatalf("values do not match for %d: %v", i, v)
		}
	}
	for i := range numEntries {
		v, loaded := m.LoadOrCompute(strconv.Itoa(i), func() (newValue int, cancel bool) {
			t.Fatalf("value func invoked")
			return newValue, false
		})
		if !loaded {
			t.Fatalf("value not loaded for %d", i)
		}
		if v != i {
			t.Fatalf("values do not match for %d: %v", i, v)
		}
	}
}

func TestMapLoadOrCompute_FunctionCalledOnce(t *testing.T) {
	m := NewMap[int, int]()
	for i := 0; i < 100; {
		m.LoadOrCompute(i, func() (newValue int, cancel bool) {
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

func TestMapOfCompute(t *testing.T) {
	m := NewMap[string, int]()
	// Store a new value.
	v, ok := m.Compute("foobar", func(oldValue int, loaded bool) (newValue int, op ComputeOp) {
		if oldValue != 0 {
			t.Fatalf("oldValue should be 0 when computing a new value: %d", oldValue)
		}
		if loaded {
			t.Fatal("loaded should be false when computing a new value")
		}
		newValue = 42
		op = UpdateOp
		return
	})
	if v != 42 {
		t.Fatalf("v should be 42 when computing a new value: %d", v)
	}
	if !ok {
		t.Fatal("ok should be true when computing a new value")
	}
	// Update an existing value.
	v, ok = m.Compute("foobar", func(oldValue int, loaded bool) (newValue int, op ComputeOp) {
		if oldValue != 42 {
			t.Fatalf("oldValue should be 42 when updating the value: %d", oldValue)
		}
		if !loaded {
			t.Fatal("loaded should be true when updating the value")
		}
		newValue = oldValue + 42
		op = UpdateOp
		return
	})
	if v != 84 {
		t.Fatalf("v should be 84 when updating the value: %d", v)
	}
	if !ok {
		t.Fatal("ok should be true when updating the value")
	}
	// Check that NoOp doesn't update the value
	v, ok = m.Compute("foobar", func(oldValue int, loaded bool) (newValue int, op ComputeOp) {
		return 0, CancelOp
	})
	if v != 84 {
		t.Fatalf("v should be 84 after using NoOp: %d", v)
	}
	if !ok {
		t.Fatal("ok should be true when updating the value")
	}
	// Delete an existing value.
	v, ok = m.Compute("foobar", func(oldValue int, loaded bool) (newValue int, op ComputeOp) {
		if oldValue != 84 {
			t.Fatalf("oldValue should be 84 when deleting the value: %d", oldValue)
		}
		if !loaded {
			t.Fatal("loaded should be true when deleting the value")
		}
		op = DeleteOp
		return
	})
	if v != 84 {
		t.Fatalf("v should be 84 when deleting the value: %d", v)
	}
	if ok {
		t.Fatal("ok should be false when deleting the value")
	}
	// Try to delete a non-existing value. Notice different key.
	v, ok = m.Compute("barbaz", func(oldValue int, loaded bool) (newValue int, op ComputeOp) {
		if oldValue != 0 {
			t.Fatalf("oldValue should be 0 when trying to delete a non-existing value: %d", oldValue)
		}
		if loaded {
			t.Fatal("loaded should be false when trying to delete a non-existing value")
		}
		// We're returning a non-zero value, but the map should ignore it.
		newValue = 42
		op = DeleteOp
		return
	})
	if v != 0 {
		t.Fatalf("v should be 0 when trying to delete a non-existing value: %d", v)
	}
	if ok {
		t.Fatal("ok should be false when trying to delete a non-existing value")
	}
	// Try NoOp on a non-existing value
	v, ok = m.Compute("barbaz", func(oldValue int, loaded bool) (newValue int, op ComputeOp) {
		if oldValue != 0 {
			t.Fatalf("oldValue should be 0 when trying to delete a non-existing value: %d", oldValue)
		}
		if loaded {
			t.Fatal("loaded should be false when trying to delete a non-existing value")
		}
		// We're returning a non-zero value, but the map should ignore it.
		newValue = 42
		op = CancelOp
		return
	})
	if v != 0 {
		t.Fatalf("v should be 0 when trying to delete a non-existing value: %d", v)
	}
	if ok {
		t.Fatal("ok should be false when trying to delete a non-existing value")
	}
}

func TestMapStringStoreThenDelete(t *testing.T) {
	const numEntries = 1000
	m := NewMap[string, int]()
	for i := range numEntries {
		m.Store(strconv.Itoa(i), i)
	}
	for i := range numEntries {
		m.Delete(strconv.Itoa(i))
		if _, ok := m.Load(strconv.Itoa(i)); ok {
			t.Fatalf("value was not expected for %d", i)
		}
	}
}

func TestMapIntStoreThenDelete(t *testing.T) {
	const numEntries = 1000
	m := NewMap[int32, int32]()
	for i := range numEntries {
		m.Store(int32(i), int32(i))
	}
	for i := range numEntries {
		m.Delete(int32(i))
		if _, ok := m.Load(int32(i)); ok {
			t.Fatalf("value was not expected for %d", i)
		}
	}
}

func TestMapStoreThenDelete_Int64Keys(t *testing.T) {
	const numEntries = 1000
	m := NewMap[int64, int64]()
	for i := range numEntries {
		m.Store(int64(i), int64(i))
	}
	for i := range numEntries {
		m.Delete(int64(i))
		if _, ok := m.Load(int64(i)); ok {
			t.Fatalf("value was not expected for %d", i)
		}
	}
}

func TestMapStoreThenDelete_Uint64Keys(t *testing.T) {
	const numEntries = 1000
	m := NewMap[uint64, uint64]()
	for i := range numEntries {
		m.Store(uint64(i), uint64(i))
	}
	for i := range numEntries {
		m.Delete(uint64(i))
		if _, ok := m.Load(uint64(i)); ok {
			t.Fatalf("value was not expected for %d", i)
		}
	}
}

func TestMapStoreThenDelete_UintptrKeys(t *testing.T) {
	const numEntries = 1000
	m := NewMap[uintptr, uintptr]()
	for i := range numEntries {
		m.Store(uintptr(i), uintptr(i))
	}
	for i := range numEntries {
		m.Delete(uintptr(i))
		if _, ok := m.Load(uintptr(i)); ok {
			t.Fatalf("value was not expected for %d", i)
		}
	}
}

func TestMapStructStoreThenDelete(t *testing.T) {
	const numEntries = 1000
	m := NewMap[point, string]()
	for i := range numEntries {
		m.Store(point{int32(i), 42}, strconv.Itoa(i))
	}
	for i := range numEntries {
		m.Delete(point{int32(i), 42})
		if _, ok := m.Load(point{int32(i), 42}); ok {
			t.Fatalf("value was not expected for %d", i)
		}
	}
}

func TestMapStringStoreThenLoadAndDelete(t *testing.T) {
	const numEntries = 1000
	m := NewMap[string, int]()
	for i := range numEntries {
		m.Store(strconv.Itoa(i), i)
	}
	for i := range numEntries {
		if v, loaded := m.LoadAndDelete(strconv.Itoa(i)); !loaded || v != i {
			t.Fatalf("value was not found or different for %d: %v", i, v)
		}
		if _, ok := m.Load(strconv.Itoa(i)); ok {
			t.Fatalf("value was not expected for %d", i)
		}
	}
}

func TestMapIntStoreThenLoadAndDelete(t *testing.T) {
	const numEntries = 1000
	m := NewMap[int, int]()
	for i := range numEntries {
		m.Store(i, i)
	}
	for i := range numEntries {
		if _, loaded := m.LoadAndDelete(i); !loaded {
			t.Fatalf("value was not found for %d", i)
		}
		if _, ok := m.Load(i); ok {
			t.Fatalf("value was not expected for %d", i)
		}
	}
}

func TestMapStoreThenLoadAndDelete_Int64Keys(t *testing.T) {
	const numEntries = 1000
	m := NewMap[int64, int64]()
	for i := range numEntries {
		m.Store(int64(i), int64(i))
	}
	for i := range numEntries {
		if _, loaded := m.LoadAndDelete(int64(i)); !loaded {
			t.Fatalf("value was not found for %d", i)
		}
		if _, ok := m.Load(int64(i)); ok {
			t.Fatalf("value was not expected for %d", i)
		}
	}
}

func TestMapStoreThenLoadAndDelete_Uint64Keys(t *testing.T) {
	const numEntries = 1000
	m := NewMap[uint64, uint64]()
	for i := range numEntries {
		m.Store(uint64(i), uint64(i))
	}
	for i := range numEntries {
		if _, loaded := m.LoadAndDelete(uint64(i)); !loaded {
			t.Fatalf("value was not found for %d", i)
		}
		if _, ok := m.Load(uint64(i)); ok {
			t.Fatalf("value was not expected for %d", i)
		}
	}
}

func TestMapStoreThenLoadAndDelete_UintptrKeys(t *testing.T) {
	const numEntries = 1000
	m := NewMap[uintptr, uintptr]()
	for i := range numEntries {
		m.Store(uintptr(i), uintptr(i))
	}
	for i := range numEntries {
		if _, loaded := m.LoadAndDelete(uintptr(i)); !loaded {
			t.Fatalf("value was not found for %d", i)
		}
		if _, ok := m.Load(uintptr(i)); ok {
			t.Fatalf("value was not expected for %d", i)
		}
	}
}

func TestMapStructStoreThenLoadAndDelete(t *testing.T) {
	const numEntries = 1000
	m := NewMap[point, int]()
	for i := range numEntries {
		m.Store(point{42, int32(i)}, i)
	}
	for i := range numEntries {
		if _, loaded := m.LoadAndDelete(point{42, int32(i)}); !loaded {
			t.Fatalf("value was not found for %d", i)
		}
		if _, ok := m.Load(point{42, int32(i)}); ok {
			t.Fatalf("value was not expected for %d", i)
		}
	}
}

func TestMapStoreThenParallelDelete_DoesNotShrinkBelowMinTableLen(t *testing.T) {
	const numEntries = 1000
	m := NewMap[int, int]()
	for i := range numEntries {
		m.Store(i, i)
	}

	cdone := make(chan bool)
	go func() {
		for i := range numEntries {
			m.Delete(i)
		}
		cdone <- true
	}()
	go func() {
		for i := range numEntries {
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

func sizeBasedOnTypedRange(m *Map[string, int]) int {
	size := 0
	m.Range(func(key string, value int) bool {
		size++
		return true
	})
	return size
}

func TestMapSize(t *testing.T) {
	const numEntries = 1000
	m := NewMap[string, int]()
	size := m.Size()
	if size != 0 {
		t.Fatalf("zero size expected: %d", size)
	}
	expectedSize := 0
	for i := range numEntries {
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
	for i := range numEntries {
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

func TestMapClear(t *testing.T) {
	const numEntries = 1000
	m := NewMap[string, int]()
	for i := range numEntries {
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

func assertMapCapacity[K comparable, V any](t *testing.T, m *Map[K, V], expectedCap int) {
	stats := m.Stats()
	if stats.Capacity != expectedCap {
		t.Fatalf("capacity was different from %d: %d", expectedCap, stats.Capacity)
	}
}

func TestNewMapWithPresize(t *testing.T) {
	assertMapCapacity(t, NewMap[string, string](), DefaultMinMapTableCap)
	assertMapCapacity(t, NewMap[string, string](WithPresize(0)), DefaultMinMapTableCap)
	assertMapCapacity(t, NewMap[string, string](WithPresize(-100)), DefaultMinMapTableCap)
	assertMapCapacity(t, NewMap[string, string](WithPresize(500)), 1280)
	assertMapCapacity(t, NewMap[int, int](WithPresize(1_000_000)), 2621440)
	assertMapCapacity(t, NewMap[point, point](WithPresize(100)), 160)
}

func TestNewMapWithPresize_DoesNotShrinkBelowMinTableLen(t *testing.T) {
	const minTableLen = 1024
	const numEntries = int(minTableLen * EntriesPerMapBucket * MapLoadFactor)
	m := NewMap[int, int](WithPresize(numEntries))
	for i := range 2 * numEntries {
		m.Store(i, i)
	}

	stats := m.Stats()
	if stats.RootBuckets <= minTableLen {
		t.Fatalf("table did not grow: %d", stats.RootBuckets)
	}

	for i := range 2 * numEntries {
		m.Delete(i)
	}

	stats = m.Stats()
	if stats.RootBuckets != minTableLen {
		t.Fatalf("table length was different from the minimum: %d", stats.RootBuckets)
	}
}

func TestNewMapGrowOnly_OnlyShrinksOnClear(t *testing.T) {
	const minTableLen = 128
	const numEntries = minTableLen * EntriesPerMapBucket
	m := NewMap[int, int](WithPresize(numEntries), WithGrowOnly())

	stats := m.Stats()
	initialTableLen := stats.RootBuckets

	for i := range 2 * numEntries {
		m.Store(i, i)
	}
	stats = m.Stats()
	maxTableLen := stats.RootBuckets
	if maxTableLen <= minTableLen {
		t.Fatalf("table did not grow: %d", maxTableLen)
	}

	for i := range numEntries {
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

func TestMapResize(t *testing.T) {
	m := NewMap[string, int]()
	const numEntries = 100_000

	for i := range numEntries {
		m.Store(strconv.Itoa(i), i)
	}
	stats := m.Stats()
	if stats.Size != numEntries {
		t.Fatalf("size was too small: %d", stats.Size)
	}
	expectedCapacity := int(math.RoundToEven(MapLoadFactor+1)) * stats.RootBuckets * EntriesPerMapBucket
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

	for i := range numEntries {
		m.Delete(strconv.Itoa(i))
	}
	stats = m.Stats()
	if stats.Size > 0 {
		t.Fatalf("zero size was expected: %d", stats.Size)
	}
	expectedCapacity = stats.RootBuckets * EntriesPerMapBucket
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

func TestMapResize_CounterLenLimit(t *testing.T) {
	const numEntries = 1_000_000
	m := NewMap[string, string]()

	for i := range numEntries {
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

func testParallelResize(t *testing.T, numGoroutines int) {
	m := NewMap[int, int]()

	// Fill the map to trigger resizing
	const initialEntries = 10000
	const newEntries = 5000
	for i := range initialEntries {
		m.Store(i, i*2)
	}

	// Start concurrent operations that should trigger helping behavior
	var wg sync.WaitGroup

	// Launch goroutines that will encounter resize operations
	for g := range numGoroutines {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()

			// Perform many operations to trigger resize and helping
			for i := range newEntries {
				key := goroutineID*newEntries + i + initialEntries
				m.Store(key, key*2)

				// Verify the value
				if val, ok := m.Load(key); !ok || val != key*2 {
					t.Errorf("Failed to load key %d: got %v, %v", key, val, ok)
					return
				}
			}
		}(g)
	}

	wg.Wait()

	// Verify all entries are present
	finalSize := m.Size()
	expectedSize := initialEntries + numGoroutines*newEntries
	if finalSize != expectedSize {
		t.Errorf("Expected size %d, got %d", expectedSize, finalSize)
	}

	stats := m.Stats()
	if stats.TotalGrowths == 0 {
		t.Error("Expected at least one table growth due to concurrent operations")
	}
}

func TestMapParallelResize(t *testing.T) {
	testParallelResize(t, 1)
	testParallelResize(t, runtime.GOMAXPROCS(0))
	testParallelResize(t, 100)
}

func testParallelResizeWithSameKeys(t *testing.T, numGoroutines int) {
	m := NewMap[int, int]()

	// Fill the map to trigger resizing
	const entries = 1000
	for i := range entries {
		m.Store(2*i, 2*i)
	}

	// Start concurrent operations that should trigger helping behavior
	var wg sync.WaitGroup

	// Launch goroutines that will encounter resize operations
	for g := range numGoroutines {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			for i := range 10 * entries {
				m.Store(i, i)
			}
		}(g)
	}

	wg.Wait()

	// Verify all entries are present
	finalSize := m.Size()
	expectedSize := 10 * entries
	if finalSize != expectedSize {
		t.Errorf("Expected size %d, got %d", expectedSize, finalSize)
	}

	stats := m.Stats()
	if stats.TotalGrowths == 0 {
		t.Error("Expected at least one table growth due to concurrent operations")
	}
}

func TestMapParallelResize_IntersectingKeys(t *testing.T) {
	testParallelResizeWithSameKeys(t, 1)
	testParallelResizeWithSameKeys(t, runtime.GOMAXPROCS(0))
	testParallelResizeWithSameKeys(t, 100)
}

func testParallelShrinking(t *testing.T, numGoroutines int) {
	m := NewMap[int, int]()

	// Fill the map to trigger resizing
	const entries = 100000
	for i := range entries {
		m.Store(i, i)
	}

	// Start concurrent operations that should trigger helping behavior
	var wg sync.WaitGroup

	// Launch goroutines that will encounter resize operations
	for g := range numGoroutines {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			for i := range entries {
				m.Delete(i)
			}
		}(g)
	}

	wg.Wait()

	// Verify all entries are present
	finalSize := m.Size()
	if finalSize != 0 {
		t.Errorf("Expected size 0, got %d", finalSize)
	}

	stats := m.Stats()
	if stats.TotalShrinks == 0 {
		t.Error("Expected at least one table shrinking due to concurrent operations")
	}
}

func TestMapParallelShrinking(t *testing.T) {
	testParallelShrinking(t, 1)
	testParallelShrinking(t, runtime.GOMAXPROCS(0))
	testParallelShrinking(t, 100)
}

func parallelSeqMapGrower(m *Map[int, int], numEntries int, positive bool, cdone chan bool) {
	for i := range numEntries {
		if positive {
			m.Store(i, i)
		} else {
			m.Store(-i, -i)
		}
	}
	cdone <- true
}

func TestMapParallelGrowth_GrowOnly(t *testing.T) {
	const numEntries = 100_000
	m := NewMap[int, int]()
	cdone := make(chan bool)
	go parallelSeqMapGrower(m, numEntries, true, cdone)
	go parallelSeqMapGrower(m, numEntries, false, cdone)
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

func parallelRandMapResizer(t *testing.T, m *Map[string, int], numIters, numEntries int, cdone chan bool) {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for range numIters {
		coin := r.Int63n(2)
		for j := range numEntries {
			if coin == 1 {
				m.Store(strconv.Itoa(j), j)
			} else {
				m.Delete(strconv.Itoa(j))
			}
		}
	}
	cdone <- true
}

func TestMapParallelGrowth(t *testing.T) {
	const numIters = 1_000
	const numEntries = 2 * EntriesPerMapBucket * DefaultMinMapTableLen
	m := NewMap[string, int]()
	cdone := make(chan bool)
	go parallelRandMapResizer(t, m, numIters, numEntries, cdone)
	go parallelRandMapResizer(t, m, numIters, numEntries, cdone)
	// Wait for the goroutines to finish.
	<-cdone
	<-cdone
	// Verify map contents.
	for i := range numEntries {
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

func parallelRandMapClearer(t *testing.T, m *Map[string, int], numIters, numEntries int, cdone chan bool) {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for range numIters {
		coin := r.Int63n(2)
		for j := range numEntries {
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
	m := NewMap[string, int]()
	cdone := make(chan bool)
	go parallelRandMapClearer(t, m, numIters, numEntries, cdone)
	go parallelRandMapClearer(t, m, numIters, numEntries, cdone)
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

func parallelSeqMapStorer(t *testing.T, m *Map[string, int], storeEach, numIters, numEntries int, cdone chan bool) {
	for range numIters {
		for j := range numEntries {
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

func TestMapParallelStores(t *testing.T) {
	const numStorers = 4
	const numIters = 10_000
	const numEntries = 100
	m := NewMap[string, int]()
	cdone := make(chan bool)
	for i := range numStorers {
		go parallelSeqMapStorer(t, m, i, numIters, numEntries, cdone)
	}
	// Wait for the goroutines to finish.
	for range numStorers {
		<-cdone
	}
	// Verify map contents.
	for i := range numEntries {
		v, ok := m.Load(strconv.Itoa(i))
		if !ok {
			t.Fatalf("value not found for %d", i)
		}
		if v != i {
			t.Fatalf("values do not match for %d: %v", i, v)
		}
	}
}

func parallelRandMapStorer(t *testing.T, m *Map[string, int], numIters, numEntries int, cdone chan bool) {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for range numIters {
		j := r.Intn(numEntries)
		if v, loaded := m.LoadOrStore(strconv.Itoa(j), j); loaded {
			if v != j {
				t.Errorf("value was not expected for %d: %d", j, v)
			}
		}
	}
	cdone <- true
}

func parallelRandMapDeleter(t *testing.T, m *Map[string, int], numIters, numEntries int, cdone chan bool) {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for range numIters {
		j := r.Intn(numEntries)
		if v, loaded := m.LoadAndDelete(strconv.Itoa(j)); loaded {
			if v != j {
				t.Errorf("value was not expected for %d: %d", j, v)
			}
		}
	}
	cdone <- true
}

func parallelMapLoader(t *testing.T, m *Map[string, int], numIters, numEntries int, cdone chan bool) {
	for range numIters {
		for j := range numEntries {
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

func TestMapAtomicSnapshot(t *testing.T) {
	const numIters = 100_000
	const numEntries = 100
	m := NewMap[string, int]()
	cdone := make(chan bool)
	// Update or delete random entry in parallel with loads.
	go parallelRandMapStorer(t, m, numIters, numEntries, cdone)
	go parallelRandMapDeleter(t, m, numIters, numEntries, cdone)
	go parallelMapLoader(t, m, numIters, numEntries, cdone)
	// Wait for the goroutines to finish.
	for range 3 {
		<-cdone
	}
}

func TestMapParallelStoresAndDeletes(t *testing.T) {
	const numWorkers = 2
	const numIters = 100_000
	const numEntries = 1000
	m := NewMap[string, int]()
	cdone := make(chan bool)
	// Update random entry in parallel with deletes.
	for range numWorkers {
		go parallelRandMapStorer(t, m, numIters, numEntries, cdone)
		go parallelRandMapDeleter(t, m, numIters, numEntries, cdone)
	}
	// Wait for the goroutines to finish.
	for range 2 * numWorkers {
		<-cdone
	}
}

func parallelMapComputer(m *Map[uint64, uint64], numIters, numEntries int, cdone chan bool) {
	for range numIters {
		for j := range numEntries {
			m.Compute(uint64(j), func(oldValue uint64, loaded bool) (newValue uint64, op ComputeOp) {
				return oldValue + 1, UpdateOp
			})
		}
	}
	cdone <- true
}

func TestMapParallelComputes(t *testing.T) {
	const numWorkers = 4 // Also stands for numEntries.
	const numIters = 10_000
	m := NewMap[uint64, uint64]()
	cdone := make(chan bool)
	for range numWorkers {
		go parallelMapComputer(m, numIters, numWorkers, cdone)
	}
	// Wait for the goroutines to finish.
	for range numWorkers {
		<-cdone
	}
	// Verify map contents.
	for i := range numWorkers {
		v, ok := m.Load(uint64(i))
		if !ok {
			t.Fatalf("value not found for %d", i)
		}
		if v != numWorkers*numIters {
			t.Fatalf("values do not match for %d: %v", i, v)
		}
	}
}

func parallelRangeMapStorer(m *Map[int, int], numEntries int, stopFlag *int64, cdone chan bool) {
	for {
		for i := range numEntries {
			m.Store(i, i)
		}
		if atomic.LoadInt64(stopFlag) != 0 {
			break
		}
	}
	cdone <- true
}

func parallelRangeMapDeleter(m *Map[int, int], numEntries int, stopFlag *int64, cdone chan bool) {
	for {
		for i := range numEntries {
			m.Delete(i)
		}
		if atomic.LoadInt64(stopFlag) != 0 {
			break
		}
	}
	cdone <- true
}

func TestMapParallelRange(t *testing.T) {
	const numEntries = 10_000
	m := NewMap[int, int](WithPresize(numEntries))
	for i := range numEntries {
		m.Store(i, i)
	}
	// Start goroutines that would be storing and deleting items in parallel.
	cdone := make(chan bool)
	stopFlag := int64(0)
	go parallelRangeMapStorer(m, numEntries, &stopFlag, cdone)
	go parallelRangeMapDeleter(m, numEntries, &stopFlag, cdone)
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

func parallelMapShrinker(t *testing.T, m *Map[uint64, *point], numIters, numEntries int, stopFlag *int64, cdone chan bool) {
	for range numIters {
		for j := range numEntries {
			if p, loaded := m.LoadOrStore(uint64(j), &point{int32(j), int32(j)}); loaded {
				t.Errorf("value was present for %d: %v", j, p)
			}
		}
		for j := range numEntries {
			m.Delete(uint64(j))
		}
	}
	atomic.StoreInt64(stopFlag, 1)
	cdone <- true
}

func parallelMapUpdater(t *testing.T, m *Map[uint64, *point], idx int, stopFlag *int64, cdone chan bool) {
	for atomic.LoadInt64(stopFlag) != 1 {
		sleepUs := int(Cheaprand() % 10)
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

func TestMapDoesNotLoseEntriesOnResize(t *testing.T) {
	const numIters = 10_000
	const numEntries = 128
	m := NewMap[uint64, *point]()
	cdone := make(chan bool)
	stopFlag := int64(0)
	go parallelMapShrinker(t, m, numIters, numEntries, &stopFlag, cdone)
	go parallelMapUpdater(t, m, numEntries, &stopFlag, cdone)
	// Wait for the goroutines to finish.
	<-cdone
	<-cdone
	// Verify map contents.
	if s := m.Size(); s != 0 {
		t.Fatalf("map is not empty: %d", s)
	}
}

func TestMapStats(t *testing.T) {
	m := NewMap[int, int]()

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
	if stats.Capacity != EntriesPerMapBucket*DefaultMinMapTableLen {
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

	for i := range 200 {
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
	if stats.Capacity < 2*EntriesPerMapBucket*DefaultMinMapTableLen {
		t.Fatalf("unexpected capacity: %d", stats.Capacity)
	}
	if stats.Size != 200 {
		t.Fatalf("unexpected size: %d", stats.Size)
	}
	if stats.Counter != 200 {
		t.Fatalf("unexpected counter: %d", stats.Counter)
	}
	if stats.CounterLen != 8 {
		t.Fatalf("unexpected counter length: %d", stats.CounterLen)
	}
}

func TestToPlainMap_NilPointer(t *testing.T) {
	pm := ToPlainMap[int, int](nil)
	if len(pm) != 0 {
		t.Fatalf("got unexpected size of nil map copy: %d", len(pm))
	}
}

func TestToPlainMap(t *testing.T) {
	const numEntries = 1000
	m := NewMap[int, int]()
	for i := range numEntries {
		m.Store(i, i)
	}
	pm := ToPlainMap[int, int](m)
	if len(pm) != numEntries {
		t.Fatalf("got unexpected size of nil map copy: %d", len(pm))
	}
	for i := range numEntries {
		if v := pm[i]; v != i {
			t.Fatalf("unexpected value for key %d: %d", i, v)
		}
	}
}

func BenchmarkMap_NoWarmUp(b *testing.B) {
	for _, bc := range benchmarkCases {
		if bc.readPercentage == 100 {
			// This benchmark doesn't make sense without a warm-up.
			continue
		}
		b.Run(bc.name, func(b *testing.B) {
			m := NewMap[string, int]()
			benchmarkMapStringKeys(b, func(k string) (int, bool) {
				return m.Load(k)
			}, func(k string, v int) {
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
			m := NewMap[string, int](WithPresize(benchmarkNumEntries))
			for i := range benchmarkNumEntries {
				m.Store(benchmarkKeyPrefix+strconv.Itoa(i), i)
			}
			b.ResetTimer()
			benchmarkMapStringKeys(b, func(k string) (int, bool) {
				return m.Load(k)
			}, func(k string, v int) {
				m.Store(k, v)
			}, func(k string) {
				m.Delete(k)
			}, bc.readPercentage)
		})
	}
}

func benchmarkMapStringKeys(
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
			op := int(Cheaprand() % 1000)
			i := int(Cheaprand() % benchmarkNumEntries)
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

func BenchmarkMapInt_NoWarmUp(b *testing.B) {
	for _, bc := range benchmarkCases {
		if bc.readPercentage == 100 {
			// This benchmark doesn't make sense without a warm-up.
			continue
		}
		b.Run(bc.name, func(b *testing.B) {
			m := NewMap[int, int]()
			benchmarkMapIntKeys(b, func(k int) (int, bool) {
				return m.Load(k)
			}, func(k int, v int) {
				m.Store(k, v)
			}, func(k int) {
				m.Delete(k)
			}, bc.readPercentage)
		})
	}
}

func BenchmarkMapInt_WarmUp(b *testing.B) {
	for _, bc := range benchmarkCases {
		b.Run(bc.name, func(b *testing.B) {
			m := NewMap[int, int](WithPresize(benchmarkNumEntries))
			for i := range benchmarkNumEntries {
				m.Store(i, i)
			}
			b.ResetTimer()
			benchmarkMapIntKeys(b, func(k int) (int, bool) {
				return m.Load(k)
			}, func(k int, v int) {
				m.Store(k, v)
			}, func(k int) {
				m.Delete(k)
			}, bc.readPercentage)
		})
	}
}

func BenchmarkIntMapStandard_NoWarmUp(b *testing.B) {
	for _, bc := range benchmarkCases {
		if bc.readPercentage == 100 {
			// This benchmark doesn't make sense without a warm-up.
			continue
		}
		b.Run(bc.name, func(b *testing.B) {
			var m sync.Map
			benchmarkMapIntKeys(b, func(k int) (value int, ok bool) {
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
func BenchmarkIntMapStandard_WarmUp(b *testing.B) {
	for _, bc := range benchmarkCases {
		b.Run(bc.name, func(b *testing.B) {
			var m sync.Map
			for i := range benchmarkNumEntries {
				m.Store(i, i)
			}
			b.ResetTimer()
			benchmarkMapIntKeys(b, func(k int) (value int, ok bool) {
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

func benchmarkMapIntKeys(
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
			op := int(Cheaprand() % 1000)
			i := int(Cheaprand() % benchmarkNumEntries)
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

func BenchmarkMapRange(b *testing.B) {
	m := NewMap[string, int](WithPresize(benchmarkNumEntries))
	for i := range benchmarkNumEntries {
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

func BenchmarkMapRangeRelaxed(b *testing.B) {
	m := NewMap[string, int](WithPresize(benchmarkNumEntries))
	for i := range benchmarkNumEntries {
		m.Store(benchmarkKeys[i], i)
	}
	b.ResetTimer()
	runParallel(b, func(pb *testing.PB) {
		foo := 0
		for pb.Next() {
			m.RangeRelaxed(func(key string, value int) bool {
				foo++
				return true
			})
			_ = foo
		}
	})
}

// Benchmarks noop performance of Compute
func BenchmarkMapCompute(b *testing.B) {
	tests := []struct {
		Name string
		Op   ComputeOp
	}{
		{
			Name: "UpdateOp",
			Op:   UpdateOp,
		},
		{
			Name: "CancelOp",
			Op:   CancelOp,
		},
	}
	for _, test := range tests {
		b.Run("op="+test.Name, func(b *testing.B) {
			m := NewMap[struct{}, bool]()
			m.Store(struct{}{}, true)
			for b.Loop() {
				m.Compute(struct{}{}, func(oldValue bool, loaded bool) (newValue bool, op ComputeOp) {
					return oldValue, test.Op
				})
			}
		})
	}
}

func BenchmarkMapParallelRehashing(b *testing.B) {
	tests := []struct {
		name       string
		goroutines int
		numEntries int
	}{
		{"1goroutine_10M", 1, 10_000_000},
		{"4goroutines_10M", 4, 10_000_000},
		{"8goroutines_10M", 8, 10_000_000},
	}
	for _, test := range tests {
		b.Run(test.name, func(b *testing.B) {
			for b.Loop() {
				m := NewMap[int, int]()

				var wg sync.WaitGroup
				entriesPerGoroutine := test.numEntries / test.goroutines

				start := time.Now()

				for g := 0; g < test.goroutines; g++ {
					wg.Add(1)
					go func(goroutineID int) {
						defer wg.Done()
						base := goroutineID * entriesPerGoroutine
						for j := range entriesPerGoroutine {
							key := base + j
							m.Store(key, key)
						}
					}(g)
				}

				wg.Wait()
				duration := time.Since(start)

				b.ReportMetric(float64(test.numEntries)/duration.Seconds(), "entries/s")

				finalSize := m.Size()
				if finalSize != test.numEntries {
					b.Fatalf("Expected size %d, got %d", test.numEntries, finalSize)
				}

				stats := m.Stats()
				if stats.TotalGrowths == 0 {
					b.Error("Expected at least one table growth during rehashing")
				}
			}
		})
	}
}

func BenchmarkMapDeleteMatching(b *testing.B) {
	tests := []struct {
		name          string
		numEntries    int
		deletePercent int
	}{
		{"entries=1000_delete=10%", 1000, 10},
		{"entries=1000_delete=50%", 1000, 50},
		{"entries=1000_delete=100%", 1000, 100},
		{"entries=100000_delete=10%", 100000, 10},
		{"entries=100000_delete=50%", 100000, 50},
		{"entries=100000_delete=100%", 100000, 100},
		{"entries=1000000_delete=10%", 1000000, 10},
		{"entries=1000000_delete=50%", 1000000, 50},
		{"entries=1000000_delete=100%", 1000000, 100},
	}
	for _, test := range tests {
		b.Run(test.name, func(b *testing.B) {
			for b.Loop() {
				m := NewMap[int, int](WithPresize(test.numEntries))
				for i := range test.numEntries {
					m.Store(i, i)
				}
				threshold := test.numEntries * test.deletePercent / 100
				m.DeleteMatching(func(key int, value int) (del, stop bool) {
					return key < threshold, false
				})
			}
		})
	}
}

func BenchmarkMapRangeDelete(b *testing.B) {
	tests := []struct {
		name          string
		numEntries    int
		deletePercent int
	}{
		{"entries=1000_delete=10%", 1000, 10},
		{"entries=1000_delete=50%", 1000, 50},
		{"entries=1000_delete=100%", 1000, 100},
		{"entries=100000_delete=10%", 100000, 10},
		{"entries=100000_delete=50%", 100000, 50},
		{"entries=100000_delete=100%", 100000, 100},
		{"entries=1000000_delete=10%", 1000000, 10},
		{"entries=1000000_delete=50%", 1000000, 50},
		{"entries=1000000_delete=100%", 1000000, 100},
	}
	for _, test := range tests {
		b.Run(test.name, func(b *testing.B) {
			for b.Loop() {
				m := NewMap[int, int](WithPresize(test.numEntries))
				for i := range test.numEntries {
					m.Store(i, i)
				}
				threshold := test.numEntries * test.deletePercent / 100
				m.Range(func(key int, value int) bool {
					if key < threshold {
						m.Delete(key)
					}
					return true
				})
			}
		})
	}
}
