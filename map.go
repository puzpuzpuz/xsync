package xsync

import (
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"unsafe"
)

type mapResizeHint int

const (
	mapGrowHint   mapResizeHint = 0
	mapShrinkHint mapResizeHint = 1
)

const (
	// number of entries per bucket; 7 entries lead to size of 128B
	// (2 cache lines) on 64-bit machines
	entriesPerMapBucket = 7
	// threshold fraction of table occupation to start a table shrinking
	// when deleting the last entry in a bucket chain
	mapShrinkFraction = 128
	// minimal table size, i.e. number of buckets; thus, minimal map
	// capacity can be calculated as entriesPerMapBucket*minMapTableLen
	minMapTableLen = 32
	// maximum counter stripes to use; stands for around 8KB of memory
	maxMapCounterLen = 128
)

// Map is like a Go map[string]interface{} but is safe for concurrent
// use by multiple goroutines without additional locking or
// coordination. It follows the interface of sync.Map.
//
// A Map must not be copied after first use.
//
// Map uses a modified version of Cache-Line Hash Table (CLHT)
// data structure: https://github.com/LPD-EPFL/CLHT
//
// CLHT is built around idea to organize the hash table in
// cache-line-sized buckets, so that on all modern CPUs update
// operations complete with at most one cache-line transfer.
// Also, Get operations involve no write to memory, as well as no
// mutexes or any other sort of locks. Due to this design, in all
// considered scenarios Map outperforms sync.Map.
//
// One important difference with sync.Map is that only string keys
// are supported. That's because Golang standard library does not
// expose the built-in hash functions for interface{} values.
type Map struct {
	totalGrowths int64
	totalShrinks int64
	resizing     int64          // resize in progress flag; updated atomically
	resizeMu     sync.Mutex     // only used along with resizeCond
	resizeCond   sync.Cond      // used to wake up resize waiters (concurrent modifications)
	table        unsafe.Pointer // *mapTable
}

type mapTable struct {
	buckets []bucket
	// striped counter for number of table entries;
	// used to determine if a table shrinking is needed
	// occupies min(buckets_memory/1024, 64KB) of memory
	size []counterStripe
}

type counterStripe struct {
	c int64
	//lint:ignore U1000 prevents false sharing
	pad [cacheLineSize - 8]byte
}

type bucket struct {
	mu     sync.Mutex
	keys   [entriesPerMapBucket]unsafe.Pointer
	values [entriesPerMapBucket]unsafe.Pointer
	// contains packed top bytes (8 MSBs) of hash codes for keys
	// stored in the bucket:
	// | key 0's top hash | ... | key 7's top hash | bitmap for keys |
	// |      1 byte      | ... |      1 byte      |     1 byte      |
	topHashes uint64
}

type rangeEntry struct {
	key   unsafe.Pointer
	value unsafe.Pointer
}

// NewMap creates a new Map instance.
func NewMap() *Map {
	m := &Map{}
	m.resizeCond = *sync.NewCond(&m.resizeMu)
	table := newMapTable(minMapTableLen)
	atomic.StorePointer(&m.table, unsafe.Pointer(table))
	return m
}

func newMapTable(size int) *mapTable {
	buckets := make([]bucket, size)
	counterLen := size >> 10
	if counterLen < minMapTableLen {
		counterLen = minMapTableLen
	} else if counterLen > maxMapCounterLen {
		counterLen = maxMapCounterLen
	}
	counter := make([]counterStripe, counterLen)
	t := &mapTable{
		buckets: buckets,
		size:    counter,
	}
	return t
}

// Load returns the value stored in the map for a key, or nil if no
// value is present.
// The ok result indicates whether value was found in the map.
func (m *Map) Load(key string) (value interface{}, ok bool) {
	hash := maphash64(key)
	table := (*mapTable)(atomic.LoadPointer(&m.table))
	bidx := bucketIdx(table, hash)
	b := &table.buckets[bidx]
	topHashes := atomic.LoadUint64(&b.topHashes)
	for i := 0; i < entriesPerMapBucket; i++ {
		if !topHashMatch(hash, topHashes, i) {
			continue
		}
	atomic_snapshot:
		// Start atomic snapshot.
		vp := atomic.LoadPointer(&b.values[i])
		kp := atomic.LoadPointer(&b.keys[i])
		if kp != nil && vp != nil {
			if key == derefKey(kp) {
				if uintptr(vp) == uintptr(atomic.LoadPointer(&b.values[i])) {
					// Atomic snapshot succeeded.
					return derefValue(vp), true
				}
				// Concurrent update/remove. Go for another spin.
				goto atomic_snapshot
			}
		}
	}
	return nil, false
}

// Store sets the value for a key.
func (m *Map) Store(key string, value interface{}) {
	m.doStore(key, value, false)
}

// LoadOrStore returns the existing value for the key if present.
// Otherwise, it stores and returns the given value.
// The loaded result is true if the value was loaded, false if stored.
func (m *Map) LoadOrStore(key string, value interface{}) (actual interface{}, loaded bool) {
	return m.doStore(key, value, true)
}

// LoadAndStore returns the existing value for the key if present,
// while setting the new value for the key.
// Otherwise, it stores and returns the given value.
// The loaded result is true if the value was loaded, false otherwise.
func (m *Map) LoadAndStore(key string, value interface{}) (actual interface{}, loaded bool) {
	return m.doStore(key, value, false)
}

func (m *Map) doStore(key string, value interface{}, loadIfExists bool) (interface{}, bool) {
	// Read-only path.
	if loadIfExists {
		if v, ok := m.Load(key); ok {
			return v, true
		}
	}
	// Write path.
	hash := maphash64(key)
	for {
		var (
			emptykp, emptyvp *unsafe.Pointer
			emptyidx         int
		)
		table := (*mapTable)(atomic.LoadPointer(&m.table))
		bidx := bucketIdx(table, hash)
		b := &table.buckets[bidx]
		b.mu.Lock()
		if m.newerTableExists(table) {
			// Someone resized the table. Go for another attempt.
			b.mu.Unlock()
			continue
		}
		if m.resizeInProgress() {
			// Resize is in progress. Wait, then go for another attempt.
			b.mu.Unlock()
			m.waitForResize()
			continue
		}
		for i := 0; i < entriesPerMapBucket; i++ {
			if b.keys[i] == nil {
				if emptykp == nil {
					emptykp = &b.keys[i]
					emptyvp = &b.values[i]
					emptyidx = i
				}
				continue
			}
			if !topHashMatch(hash, b.topHashes, i) {
				continue
			}
			if key == derefKey(b.keys[i]) {
				vp := b.values[i]
				if loadIfExists {
					b.mu.Unlock()
					return derefValue(vp), true
				}
				// In-place update case. Luckily we get a copy of the value
				// interface{} on each call, thus the live value pointers are
				// unique. Otherwise atomic snapshot won't be correct in case
				// of multiple Store calls using the same value.
				nvp := unsafe.Pointer(&value)
				if assertionsEnabled && vp == nvp {
					panic("non-unique value pointer")
				}
				atomic.StorePointer(&b.values[i], nvp)
				b.mu.Unlock()
				return derefValue(vp), true
			}
		}
		if emptykp != nil {
			// Insertion case. First we update the value, then the key.
			// This is important for atomic snapshot states.
			atomic.StoreUint64(&b.topHashes, storeTopHash(hash, b.topHashes, emptyidx))
			atomic.StorePointer(emptyvp, unsafe.Pointer(&value))
			atomic.StorePointer(emptykp, unsafe.Pointer(&key))
			b.mu.Unlock()
			addSize(table, bidx, 1)
			return value, false
		}
		// Need to grow the table. Then go for another attempt.
		b.mu.Unlock()
		m.resize(table, mapGrowHint)
	}
}

func (m *Map) newerTableExists(table *mapTable) bool {
	curTablePtr := atomic.LoadPointer(&m.table)
	return uintptr(curTablePtr) != uintptr(unsafe.Pointer(table))
}

func (m *Map) resizeInProgress() bool {
	return atomic.LoadInt64(&m.resizing) == 1
}

func (m *Map) waitForResize() {
	m.resizeMu.Lock()
	for m.resizeInProgress() {
		m.resizeCond.Wait()
	}
	m.resizeMu.Unlock()
}

func (m *Map) resize(table *mapTable, hint mapResizeHint) {
	var shrinkThreshold int64
	tableLen := len(table.buckets)
	// Fast path for shrink attempts.
	if hint == mapShrinkHint {
		shrinkThreshold = int64((tableLen * entriesPerMapBucket) / mapShrinkFraction)
		if tableLen == minMapTableLen || sumSize(table) > shrinkThreshold {
			return
		}
	}
	// Slow path.
	if !atomic.CompareAndSwapInt64(&m.resizing, 0, 1) {
		// Someone else started resize. Wait for it to finish.
		m.waitForResize()
		return
	}
	var newTable *mapTable
	switch hint {
	case mapGrowHint:
		// Grow the table with factor of 2.
		atomic.AddInt64(&m.totalGrowths, 1)
		newTable = newMapTable(tableLen << 1)
	case mapShrinkHint:
		if sumSize(table) <= shrinkThreshold {
			// Shrink the table with factor of 2.
			atomic.AddInt64(&m.totalShrinks, 1)
			newTable = newMapTable(tableLen >> 1)
		} else {
			// No need to shrink. Wake up all waiters and give up.
			m.resizeMu.Lock()
			atomic.StoreInt64(&m.resizing, 0)
			m.resizeCond.Broadcast()
			m.resizeMu.Unlock()
			return
		}
	default:
		panic(fmt.Sprintf("unexpected resize hint: %d", hint))
	}
copy:
	for i := 0; i < tableLen; i++ {
		copied, ok := copyBucket(&table.buckets[i], newTable)
		if !ok {
			// Table size is insufficient, need to grow it.
			newTable = newMapTable(len(newTable.buckets) << 1)
			goto copy
		}
		addSizeNonAtomic(newTable, uint64(i), copied)
	}
	// Publish the new table and wake up all waiters.
	atomic.StorePointer(&m.table, unsafe.Pointer(newTable))
	m.resizeMu.Lock()
	atomic.StoreInt64(&m.resizing, 0)
	m.resizeCond.Broadcast()
	m.resizeMu.Unlock()
}

func copyBucket(b *bucket, destTable *mapTable) (copied int, ok bool) {
	b.mu.Lock()
	for i := 0; i < entriesPerMapBucket; i++ {
		if b.keys[i] != nil {
			k := derefKey(b.keys[i])
			hash := maphash64(k)
			bidx := bucketIdx(destTable, hash)
			destb := &destTable.buckets[bidx]
			appended := appendToBucket(hash, b.keys[i], b.values[i], destb)
			if !appended {
				b.mu.Unlock()
				return 0, false
			}
			copied++
		}
	}
	b.mu.Unlock()
	return copied, true
}

func appendToBucket(hash uint64, keyPtr, valPtr unsafe.Pointer, b *bucket) (appended bool) {
	for i := 0; i < entriesPerMapBucket; i++ {
		if b.keys[i] == nil {
			b.keys[i] = keyPtr
			b.values[i] = valPtr
			b.topHashes = storeTopHash(hash, b.topHashes, i)
			return true
		}
	}
	return false
}

// LoadAndDelete deletes the value for a key, returning the previous
// value if any. The loaded result reports whether the key was
// present.
func (m *Map) LoadAndDelete(key string) (value interface{}, loaded bool) {
	hash := maphash64(key)
delete_attempt:
	hintNonEmpty := 0
	table := (*mapTable)(atomic.LoadPointer(&m.table))
	bidx := bucketIdx(table, hash)
	b := &table.buckets[bidx]
	b.mu.Lock()
	if m.newerTableExists(table) {
		// Someone resized the table. Go for another attempt.
		b.mu.Unlock()
		goto delete_attempt
	}
	if m.resizeInProgress() {
		// Resize is in progress. Wait, then go for another attempt.
		b.mu.Unlock()
		m.waitForResize()
		goto delete_attempt
	}
	for i := 0; i < entriesPerMapBucket; i++ {
		kp := b.keys[i]
		if kp == nil || !topHashMatch(hash, b.topHashes, i) {
			continue
		}
		if key == derefKey(kp) {
			vp := b.values[i]
			// Deletion case. First we update the value, then the key.
			// This is important for atomic snapshot states.
			atomic.StoreUint64(&b.topHashes, eraseTopHash(b.topHashes, i))
			atomic.StorePointer(&b.values[i], nil)
			atomic.StorePointer(&b.keys[i], nil)
			leftEmpty := false
			if hintNonEmpty == 0 {
				leftEmpty = isEmpty(b)
			}
			b.mu.Unlock()
			addSize(table, bidx, -1)
			// Might need to shrink the table.
			if leftEmpty {
				m.resize(table, mapShrinkHint)
			}
			return derefValue(vp), true
		}
		hintNonEmpty++
	}
	b.mu.Unlock()
	return nil, false
}

// Delete deletes the value for a key.
func (m *Map) Delete(key string) {
	m.LoadAndDelete(key)
}

func isEmpty(b *bucket) bool {
	for i := 0; i < entriesPerMapBucket; i++ {
		if b.keys[i] != nil {
			return false
		}
	}
	return true
}

// Range calls f sequentially for each key and value present in the
// map. If f returns false, range stops the iteration.
//
// Range does not necessarily correspond to any consistent snapshot
// of the Map's contents: no key will be visited more than once, but
// if the value for any key is stored or deleted concurrently, Range
// may reflect any mapping for that key from any point during the
// Range call.
//
// It is safe to modify the map while iterating it. However, the
// concurrent modification rule apply, i.e. the changes may be not
// reflected in the subsequently iterated entries.
func (m *Map) Range(f func(key string, value interface{}) bool) {
	var bentries [entriesPerMapBucket]rangeEntry
	tablep := atomic.LoadPointer(&m.table)
	table := *(*mapTable)(tablep)
	for i := range table.buckets {
		n := copyRangeEntries(&table.buckets[i], &bentries)
		for j := 0; j < n; j++ {
			k := derefKey(bentries[j].key)
			v := derefValue(bentries[j].value)
			if !f(k, v) {
				return
			}
		}
	}
}

func copyRangeEntries(b *bucket, destEntries *[entriesPerMapBucket]rangeEntry) int {
	n := 0
	for i := 0; i < entriesPerMapBucket; i++ {
	atomic_snapshot:
		// Start atomic snapshot.
		vp := atomic.LoadPointer(&b.values[i])
		kp := atomic.LoadPointer(&b.keys[i])
		if kp != nil && vp != nil {
			if uintptr(vp) == uintptr(atomic.LoadPointer(&b.values[i])) {
				// Atomic snapshot succeeded.
				destEntries[n] = rangeEntry{
					key:   kp,
					value: vp,
				}
				n++
				continue
			}
			// Concurrent update/remove. Go for another spin.
			goto atomic_snapshot
		}
	}
	return n
}

// Size returns current size of the map.
func (m *Map) Size() int {
	table := (*mapTable)(atomic.LoadPointer(&m.table))
	return int(sumSize(table))
}

func derefKey(keyPtr unsafe.Pointer) string {
	return *(*string)(keyPtr)
}

func derefValue(valuePtr unsafe.Pointer) interface{} {
	return *(*interface{})(valuePtr)
}

func bucketIdx(table *mapTable, hash uint64) uint64 {
	return uint64(len(table.buckets)-1) & hash
}

func topHashMatch(hash, topHashes uint64, idx int) bool {
	if topHashes&(1<<idx) == 0 {
		// Entry is not present.
		return false
	}
	top := uint8(hash >> 56)
	topHashes = topHashes >> (8 * (entriesPerMapBucket - idx))
	return top == uint8(topHashes)
}

func storeTopHash(hash, topHashes uint64, idx int) uint64 {
	// Zero out top hash at idx.
	mask := uint64(255) << (8 * (entriesPerMapBucket - idx))
	topHashes = topHashes &^ mask
	// Store top byte of the given hash.
	top := (hash >> 56) << (8 * (entriesPerMapBucket - idx))
	topHashes = topHashes | top
	return topHashes | (1 << idx)
}

func eraseTopHash(topHashes uint64, idx int) uint64 {
	return topHashes &^ (1 << idx)
}

func addSize(table *mapTable, bucketIdx uint64, delta int) {
	cidx := uint64(len(table.size)-1) & bucketIdx
	atomic.AddInt64(&table.size[cidx].c, int64(delta))
}

func addSizeNonAtomic(table *mapTable, bucketIdx uint64, delta int) {
	cidx := uint64(len(table.size)-1) & bucketIdx
	table.size[cidx].c += int64(delta)
}

func sumSize(table *mapTable) int64 {
	sum := int64(0)
	for i := range table.size {
		sum += atomic.LoadInt64(&table.size[i].c)
	}
	return sum
}

type mapStats struct {
	TableLen     int
	Capacity     int
	Size         int // calculated number of entries
	Counter      int // number of entries according to table counter
	CounterLen   int // number of counter stripes
	MinEntries   int // min entries per chain of buckets
	MaxEntries   int // max entries per chain of buckets
	TotalGrowths int64
	TotalShrinks int64
}

func (s *mapStats) Print() {
	fmt.Println("---")
	fmt.Printf("TableLen:     %d\n", s.TableLen)
	fmt.Printf("Capacity:     %d\n", s.Capacity)
	fmt.Printf("Size:         %d\n", s.Size)
	fmt.Printf("Counter:      %d\n", s.Counter)
	fmt.Printf("CounterLen:   %d\n", s.CounterLen)
	fmt.Printf("MinEntries:   %d\n", s.MinEntries)
	fmt.Printf("MaxEntries:   %d\n", s.MaxEntries)
	fmt.Printf("TotalGrowths: %d\n", s.TotalGrowths)
	fmt.Printf("TotalShrinks: %d\n", s.TotalShrinks)
	fmt.Println("---")
}

// O(N) operation; use for debug purposes only
func (m *Map) stats() mapStats {
	stats := mapStats{
		TotalGrowths: atomic.LoadInt64(&m.totalGrowths),
		TotalShrinks: atomic.LoadInt64(&m.totalShrinks),
		MinEntries:   math.MaxInt32,
	}
	table := (*mapTable)(atomic.LoadPointer(&m.table))
	stats.TableLen = len(table.buckets)
	stats.Counter = int(sumSize(table))
	stats.CounterLen = len(table.size)
	for i := range table.buckets {
		numEntries := 0
		stats.Capacity += entriesPerMapBucket
		b := &table.buckets[i]
		for i := 0; i < entriesPerMapBucket; i++ {
			if atomic.LoadPointer(&b.keys[i]) != nil {
				stats.Size++
				numEntries++
			}
		}
		if numEntries < stats.MinEntries {
			stats.MinEntries = numEntries
		}
		if numEntries > stats.MaxEntries {
			stats.MaxEntries = numEntries
		}
	}
	return stats
}
