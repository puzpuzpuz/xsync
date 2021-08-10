package xsync_test

import (
	"math/bits"
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"
	"unsafe"

	. "github.com/puzpuzpuz/xsync"
)

const (
	// number of entries to use in benchmarks
	benchmarkNumEntries = 1_000_000
	// key prefix used in benchmarks
	benchmarkKeyPrefix = "what_a_looooooooooooooooooooooong_key_prefix_"
)

var benchmarkCases = []struct {
	name           string
	writeThreshold int
}{
	{"99%-reads", 99}, // 99% loads,  0.5% stores,  0.5% deletes
	{"90%-reads", 90}, // 90% loads,    5% stores,    5% deletes
	{"75%-reads", 75}, // 75% loads, 12.5% stores, 12.5% deletes
	{"50%-reads", 50}, // 50% loads,   25% stores,   25% deletes
	{"0%-reads", 0},   //  0% loads,   50% stores,   50% deletes
}

func TestMapBucketStructSize(t *testing.T) {
	if bits.UintSize != 64 {
		return // skip for 32-bit builds
	}
	size := unsafe.Sizeof(Bucket{})
	if size != 64 {
		t.Errorf("size of 64B (cache line) is expected, got: %d", size)
	}
}

func TestMapMissingEntry(t *testing.T) {
	m := NewMap()
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

func TestMapStoreNilValue(t *testing.T) {
	m := NewMap()
	defer func() {
		recover()
	}()
	m.Store("foo", nil)
	t.Error("no panic was raised")
}

func TestMapLoadOrStoreNilValue(t *testing.T) {
	m := NewMap()
	defer func() {
		recover()
	}()
	m.LoadOrStore("foo", nil)
	t.Error("no panic was raised")
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
		t.Errorf("got unexpected number of iterations: %d", iters)
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
			t.Errorf("value found for %d", i)
		}
	}
}

func TestMapSerialStore(t *testing.T) {
	const numEntries = 128
	m := NewMap()
	for i := 0; i < numEntries; i++ {
		m.Store(strconv.Itoa(i), i)
	}
	for i := 0; i < numEntries; i++ {
		v, ok := m.Load(strconv.Itoa(i))
		if !ok {
			t.Errorf("value not found for %d", i)
		}
		if v == nil {
			t.Errorf("nil value found for %d", i)
		}
		if vi, ok := v.(int); ok && vi != i {
			t.Errorf("values do not match for %d: %v", i, v)
		}
	}
}

func TestMapSerialLoadOrStore(t *testing.T) {
	const numEntries = 1000
	m := NewMap()
	for i := 0; i < numEntries; i++ {
		m.Store(strconv.Itoa(i), i)
	}
	for i := 0; i < numEntries; i++ {
		if _, loaded := m.LoadOrStore(strconv.Itoa(i), i); !loaded {
			t.Errorf("value not found for %d", i)
		}
	}
}

func TestMapSerialStoreThenDelete(t *testing.T) {
	const numEntries = 1000
	m := NewMap()
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

func TestMapSerialStoreThenLoadAndDelete(t *testing.T) {
	const numEntries = 1000
	m := NewMap()
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

func parallelSeqStorer(t *testing.T, m *Map, storeEach, numIters, numEntries int, cdone chan bool) {
	for j := 0; j < numEntries; j++ {
		if storeEach == 0 || j%storeEach == 0 {
			m.Store(strconv.Itoa(j), j)
			// Due to atomic snapshots we must either see no entry, or a "<j>"/j pair.
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
	cdone <- true
}

func TestMapParallelStores(t *testing.T) {
	const numStorers = 4
	const numIters = 100_000
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
			t.Errorf("value not found for %d", i)
		}
		if v == nil {
			t.Errorf("nil value found for %d", i)
		}
		if vi, ok := v.(int); ok && vi != i {
			t.Errorf("values do not match for %d: %v", i, v)
		}
	}
}

func parallelRandStorer(m *Map, numIters, numEntries int, cdone chan bool) {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < numIters; i++ {
		j := r.Intn(numEntries)
		m.Store(strconv.Itoa(j), j)
	}
	cdone <- true
}

func parallelRandDeleter(m *Map, numIters, numEntries int, cdone chan bool) {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < numIters; i++ {
		j := r.Intn(numEntries)
		m.Delete(strconv.Itoa(j))
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
	// Update or delete each entry in parallel with loads.
	go parallelRandStorer(m, numIters, numEntries, cdone)
	go parallelRandDeleter(m, numIters, numEntries, cdone)
	go parallelLoader(t, m, numIters, numEntries, cdone)
	// Wait for the goroutines to finish.
	for i := 0; i < 3; i++ {
		<-cdone
	}
}

type SyncMap interface {
	Load(key string) (value interface{}, ok bool)
	Store(key string, value interface{})
}

func BenchmarkMap_NoWarmUp(b *testing.B) {
	for _, bc := range benchmarkCases {
		b.Run(bc.name, func(b *testing.B) {
			m := NewMap()
			benchmarkMap(b, func(k string) (interface{}, bool) {
				return m.Load(k)
			}, func(k string, v interface{}) {
				m.Store(k, v)
			}, func(k string) {
				m.Delete(k)
			}, bc.writeThreshold)
		})
	}
}

func BenchmarkMapStandard_NoWarmUp(b *testing.B) {
	for _, bc := range benchmarkCases {
		b.Run(bc.name, func(b *testing.B) {
			var m sync.Map
			benchmarkMap(b, func(k string) (interface{}, bool) {
				return m.Load(k)
			}, func(k string, v interface{}) {
				m.Store(k, v)
			}, func(k string) {
				m.Delete(k)
			}, bc.writeThreshold)
		})
	}
}

func BenchmarkMap_WarmUp(b *testing.B) {
	for _, bc := range benchmarkCases {
		b.Run(bc.name, func(b *testing.B) {
			m := NewMap()
			for i := 0; i < benchmarkNumEntries; i++ {
				m.Store(benchmarkKeyPrefix+strconv.Itoa(i), i)
			}
			benchmarkMap(b, func(k string) (interface{}, bool) {
				return m.Load(k)
			}, func(k string, v interface{}) {
				m.Store(k, v)
			}, func(k string) {
				m.Delete(k)
			}, bc.writeThreshold)
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
			benchmarkMap(b, func(k string) (interface{}, bool) {
				return m.Load(k)
			}, func(k string, v interface{}) {
				m.Store(k, v)
			}, func(k string) {
				m.Delete(k)
			}, bc.writeThreshold)
		})
	}
}

func benchmarkMap(
	b *testing.B,
	loadFn func(k string) (interface{}, bool),
	storeFn func(k string, v interface{}),
	deleteFn func(k string),
	writeThreshold int) {

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		r := rand.New(rand.NewSource(time.Now().UnixNano()))
		// convert percent to permille to support 99% case
		storeThreshold := 10 * writeThreshold
		deleteThreshold := 10*writeThreshold + ((1000 - 10*writeThreshold) / 2)
		for pb.Next() {
			op := r.Intn(1000)
			i := r.Intn(benchmarkNumEntries)
			if op >= deleteThreshold {
				deleteFn(benchmarkKeyPrefix + strconv.Itoa(i))
			} else if op >= storeThreshold {
				storeFn(benchmarkKeyPrefix+strconv.Itoa(i), i)
			} else {
				loadFn(benchmarkKeyPrefix + strconv.Itoa(i))
			}
		}
	})
}

func BenchmarkMapRange(b *testing.B) {
	m := NewMap()
	for i := 0; i < benchmarkNumEntries; i++ {
		m.Store(benchmarkKeyPrefix+strconv.Itoa(i), i)
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		foo := 0
		for pb.Next() {
			m.Range(func(key string, value interface{}) bool {
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
		m.Store(benchmarkKeyPrefix+strconv.Itoa(i), i)
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		foo := 0
		for pb.Next() {
			m.Range(func(key interface{}, value interface{}) bool {
				foo++
				return true
			})
			_ = foo
		}
	})
}
