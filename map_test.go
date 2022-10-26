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
	benchmarkNumEntries = 1_000_000
	// key prefix used in benchmarks
	benchmarkKeyPrefix = "what_a_looooooooooooooooooooooong_key_prefix_"
)

var benchmarkCases = []struct {
	name           string
	readPercentage int
}{
	{"reads=100%", 100},     // 100% loads,    0% stores,    0% deletes
	{"reads=99%", 99},       //  99% loads,  0.5% stores,  0.5% deletes
	{"reads=90%-reads", 90}, //  90% loads,    5% stores,    5% deletes
	{"reads=75%-reads", 75}, //  75% loads, 12.5% stores, 12.5% deletes
}

var benchmarkKeys []string

func init() {
	benchmarkKeys = make([]string, benchmarkNumEntries)
	for i := 0; i < benchmarkNumEntries; i++ {
		benchmarkKeys[i] = benchmarkKeyPrefix + strconv.Itoa(i)
	}
}

func TestMap_BucketStructSize(t *testing.T) {
	size := unsafe.Sizeof(BucketPadded{})
	if size != 64 {
		t.Errorf("size of 64B (one cache line) is expected, got: %d", size)
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
		t.Errorf("value was not expected: %v", v)
	}
	if deleted, loaded := m.LoadAndDelete("foo"); loaded {
		t.Errorf("value was not expected %v", deleted)
	}
	if actual, loaded := m.LoadOrStore("foo", "bar"); loaded {
		t.Errorf("value was not expected %v", actual)
	}
}

func TestMap_EmptyStringKey(t *testing.T) {
	m := NewMap()
	m.Store("", "foobar")
	v, ok := m.Load("")
	if !ok {
		t.Error("value was expected")
	}
	if vs, ok := v.(string); ok && vs != "foobar" {
		t.Errorf("value does not match: %v", v)
	}
}

func TestMapStore_NilValue(t *testing.T) {
	m := NewMap()
	m.Store("foo", nil)
	v, ok := m.Load("foo")
	if !ok {
		t.Error("nil value was expected")
	}
	if v != nil {
		t.Errorf("value was not nil: %v", v)
	}
}

func TestMapLoadOrStore_NilValue(t *testing.T) {
	m := NewMap()
	m.LoadOrStore("foo", nil)
	v, loaded := m.LoadOrStore("foo", nil)
	if !loaded {
		t.Error("nil value was expected")
	}
	if v != nil {
		t.Errorf("value was not nil: %v", v)
	}
}

func TestMapLoadOrStore_NonNilValue(t *testing.T) {
	type foo struct{}
	m := NewMap()
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

func TestMapLoadAndStore_NilValue(t *testing.T) {
	m := NewMap()
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

func TestMapLoadAndStore_NonNilValue(t *testing.T) {
	type foo struct{}
	m := NewMap()
	v1 := &foo{}
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

func TestMapSerialLoadOrCompute(t *testing.T) {
	const numEntries = 1000
	m := NewMap()
	for i := 0; i < numEntries; i++ {
		v, loaded := m.LoadOrCompute(strconv.Itoa(i), func() interface{} {
			return i
		})
		if loaded {
			t.Errorf("value not computed for %d", i)
		}
		if vi, ok := v.(int); ok && vi != i {
			t.Errorf("values do not match for %d: %v", i, v)
		}
	}
	for i := 0; i < numEntries; i++ {
		v, loaded := m.LoadOrCompute(strconv.Itoa(i), func() interface{} {
			return i
		})
		if !loaded {
			t.Errorf("value not loaded for %d", i)
		}
		if vi, ok := v.(int); ok && vi != i {
			t.Errorf("values do not match for %d: %v", i, v)
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

func TestMapSize(t *testing.T) {
	const numEntries = 1000
	m := NewMap()
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

func TestMapResize(t *testing.T) {
	const numEntries = 100_000
	m := NewMap()

	for i := 0; i < numEntries; i++ {
		m.Store(strconv.Itoa(i), i)
	}
	stats := CollectMapStats(m)
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
	stats = CollectMapStats(m)
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

func TestMapResize_CounterLenLimit(t *testing.T) {
	const numEntries = 1_000_000
	m := NewMap()

	for i := 0; i < numEntries; i++ {
		m.Store("foo"+strconv.Itoa(i), "bar"+strconv.Itoa(i))
	}
	stats := CollectMapStats(m)
	if stats.Size != numEntries {
		t.Errorf("size was too small: %d", stats.Size)
	}
	if stats.CounterLen != MaxMapCounterLen {
		t.Errorf("number of counter stripes was too large: %d, expected: %d",
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

func TestMapParallelResizeGrowOnly(t *testing.T) {
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
			t.Errorf("value not found for %d", i)
		}
		if vi, ok := v.(int); ok && vi != i {
			t.Errorf("values do not match for %d: %v", i, v)
		}
	}
	if s := m.Size(); s != 2*numEntries-1 {
		t.Errorf("unexpected size: %v", s)
	}
}

func parallelRandResizer(t *testing.T, m *Map, numIters, numEntries int, cdone chan bool) {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < numEntries; i++ {
		coin := r.Int63n(2)
		for j := 0; j < numEntries; j++ {
			if coin == 1 {
				m.Store(strconv.Itoa(i), i)
			} else {
				m.Delete(strconv.Itoa(i))
			}
		}
	}
	cdone <- true
}

func TestMapParallelResize(t *testing.T) {
	const numIters = 100
	const numEntries = 1_000
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
			t.Errorf("values do not match for %d: %v", i, v)
		}
	}
	if s := m.Size(); s > numEntries {
		t.Errorf("unexpected size: %v", s)
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
			t.Errorf("top hash match for all zeros for index %d", i)
		}

		prevOnes := bits.OnesCount64(*topHashes)
		*topHashes = StoreTopHash(hash, *topHashes, i)
		newOnes := bits.OnesCount64(*topHashes)
		expectedInc := bits.OnesCount64(hash) + 1
		if newOnes != prevOnes+expectedInc {
			t.Errorf("unexpected bits change after store for index %d: %d, %d, %#b",
				i, newOnes, prevOnes+expectedInc, *topHashes)
		}

		if !TopHashMatch(hash, *topHashes, i) {
			t.Errorf("top hash mismatch after store for index %d: %#b", i, *topHashes)
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
			t.Errorf("top hash match after erase for index %d: %#b", i, *topHashes)
		}

		erasedBits := ones - bits.OnesCount64(*topHashes)
		if erasedBits != 1 {
			t.Errorf("more than one bit changed after erase: %d, %d", i, erasedBits)
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
		t.Errorf("top hash mismatch for hash one: %#b, %#b", hashOne, *topHashes)
	}
	if TopHashMatch(hashTwo, *topHashes, idx) {
		t.Errorf("top hash match for hash two: %#b, %#b", hashTwo, *topHashes)
	}

	*topHashes = EraseTopHash(*topHashes, idx)
	*topHashes = StoreTopHash(hashTwo, *topHashes, idx)
	if TopHashMatch(hashOne, *topHashes, idx) {
		t.Errorf("top hash match for hash one: %#b, %#b", hashOne, *topHashes)
	}
	if !TopHashMatch(hashTwo, *topHashes, idx) {
		t.Errorf("top hash mismatch for hash two: %#b, %#b", hashTwo, *topHashes)
	}
}

type SyncMap interface {
	Load(key string) (value interface{}, ok bool)
	Store(key string, value interface{})
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

func BenchmarkMapRange(b *testing.B) {
	m := NewMap()
	for i := 0; i < benchmarkNumEntries; i++ {
		m.Store(benchmarkKeys[i], i)
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
		m.Store(benchmarkKeys[i], i)
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
