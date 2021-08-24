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
	// number of entries per bucket; 3 entries lead to size of 64B
	// (cache line) on 64-bit machines
	entriesPerMapBucket = 3
	// threshold number of linked buckets (chain size) to trigger a
	// table resize during insertion; thus, each chain holds up to
	// resizeMapThreshold+1 buckets
	resizeMapThreshold = 2
	// threshold fraction of table occupation to start a table shrinking
	// when deleting the last entry in a bucket chain
	mapShrinkThreshold = 16
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
//
// Also note that, unlike in sync.Map, the underlying hash table used
// by Map never shrinks and only grows on demand. However, this
// should not be an issue in many cases since updates happen in-place
// leaving no tombstone entries.
type Map struct {
	table        unsafe.Pointer // *mapTable
	resizing     int64          // resize in progress flag; updated atomically
	resizeMu     sync.Mutex     // only used along with resizeCond
	resizeCond   sync.Cond      // used to wake up resize waiters (concurrent modifications)
	totalGrowths int64
	totalShrinks int64
}

type mapTable struct {
	buckets []bucket
	// striped counter for number of table entries;
	// used to determine if a table shrinking is needed
	// occupies min(buckets_memory/1024, 64KB) of memory
	size []counterStripe
}

type counterStripe struct {
	c   int64
	pad [cacheLineSize - 8]byte
}

type bucket struct {
	mu     sync.Mutex
	keys   [entriesPerMapBucket]unsafe.Pointer
	values [entriesPerMapBucket]unsafe.Pointer
	next   unsafe.Pointer // *bucket
}

type rangeEntry struct {
	key   unsafe.Pointer
	value unsafe.Pointer
}

// special type to mark nil values
type nilValue struct{}

var nilVal = new(nilValue)

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
	for {
		for i := 0; i < entriesPerMapBucket; i++ {
			// Start atomic snapshot.
			for {
				vp := atomic.LoadPointer(&b.values[i])
				kp := atomic.LoadPointer(&b.keys[i])
				if kp != nil && vp != nil {
					if key == derefKey(kp) {
						if uintptr(vp) == uintptr(atomic.LoadPointer(&b.values[i])) {
							// Atomic snapshot succeeded.
							return derefValue(vp), true
						}
						// Concurrent update/remove of the key case. Go for another spin.
						continue
					}
					// Different key case. Fall into break.
				}
				// In progress update/remove case. Fall into break.
				break
			}
		}
		bucketPtr := atomic.LoadPointer(&b.next)
		if bucketPtr == nil {
			return nil, false
		}
		b = (*bucket)(bucketPtr)
	}
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

func (m *Map) doStore(key string, value interface{}, loadIfExists bool) (actual interface{}, loaded bool) {
	if value == nil {
		value = nilVal
	}
	// Read-only path.
	if loadIfExists {
		if v, ok := m.Load(key); ok {
			return v, true
		}
	}
	// Write path.
	hash := maphash64(key)
	for {
	store_attempt:
		var emptykp, emptyvp *unsafe.Pointer
		table := (*mapTable)(atomic.LoadPointer(&m.table))
		bidx := bucketIdx(table, hash)
		rootb := &table.buckets[bidx]
		b := rootb
		chainLen := 0
		rootb.mu.Lock()
		if m.newerTableExists(table) {
			// Someone resized the table. Go for another attempt.
			rootb.mu.Unlock()
			goto store_attempt
		}
		if m.resizeInProgress() {
			// Resize is in progress. Wait, then go for another attempt.
			rootb.mu.Unlock()
			m.waitForResize()
			goto store_attempt
		}
		for {
			for i := 0; i < entriesPerMapBucket; i++ {
				if b.keys[i] != nil {
					k := derefKey(b.keys[i])
					if k == key {
						if loadIfExists {
							rootb.mu.Unlock()
							return derefValue(b.values[i]), true
						}
						// In-place update case. Luckily we get a copy of the value
						// interface{} on each call, thus the live value pointers are
						// unique. Otherwise atomic snapshot won't be correct in case
						// of multiple Store calls using the same value.
						atomic.StorePointer(&b.values[i], unsafe.Pointer(&value))
						rootb.mu.Unlock()
						return
					}
				} else if emptykp == nil {
					emptykp = &b.keys[i]
					emptyvp = &b.values[i]
				}
			}
			if b.next == nil {
				if emptykp != nil {
					// Insertion case. First we update the value, then the key.
					// This is important for atomic snapshot states.
					atomic.StorePointer(emptyvp, unsafe.Pointer(&value))
					atomic.StorePointer(emptykp, unsafe.Pointer(&key))
					rootb.mu.Unlock()
					addSize(table, bidx, 1)
					return
				}
				if chainLen == resizeMapThreshold {
					// Need to grow the table. Then go for another attempt.
					rootb.mu.Unlock()
					m.resize(table, mapGrowHint)
					goto store_attempt
				}
				// Create and append a new bucket.
				newb := new(bucket)
				newb.keys[0] = unsafe.Pointer(&key)
				newb.values[0] = unsafe.Pointer(&value)
				atomic.StorePointer(&b.next, unsafe.Pointer(newb))
				rootb.mu.Unlock()
				addSize(table, bidx, 1)
				return
			}
			b = (*bucket)(b.next)
			chainLen++
		}
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
		shrinkThreshold = int64((tableLen * entriesPerMapBucket) / mapShrinkThreshold)
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
	for i := 0; i < tableLen; i++ {
		copied := copyBucket(&table.buckets[i], newTable)
		addSizeNonAtomic(newTable, uint64(i), copied)
	}
	// Publish the new table and wake up all waiters.
	atomic.StorePointer(&m.table, unsafe.Pointer(newTable))
	m.resizeMu.Lock()
	atomic.StoreInt64(&m.resizing, 0)
	m.resizeCond.Broadcast()
	m.resizeMu.Unlock()
}

func copyBucket(b *bucket, destTable *mapTable) (copied int) {
	rootb := b
	rootb.mu.Lock()
	for {
		for i := 0; i < entriesPerMapBucket; i++ {
			if b.keys[i] != nil {
				k := derefKey(b.keys[i])
				hash := maphash64(k)
				bidx := bucketIdx(destTable, hash)
				destb := &destTable.buckets[bidx]
				appendToBucket(b.keys[i], b.values[i], destb)
				copied++
			}
		}
		if b.next == nil {
			rootb.mu.Unlock()
			return
		}
		b = (*bucket)(b.next)
	}
}

func appendToBucket(keyPtr, valPtr unsafe.Pointer, destBucket *bucket) {
	for {
		for i := 0; i < entriesPerMapBucket; i++ {
			if destBucket.keys[i] == nil {
				destBucket.keys[i] = keyPtr
				destBucket.values[i] = valPtr
				return
			}
		}
		if destBucket.next == nil {
			newb := new(bucket)
			newb.keys[0] = keyPtr
			newb.values[0] = valPtr
			destBucket.next = unsafe.Pointer(newb)
			return
		}
		destBucket = (*bucket)(destBucket.next)
	}
}

// LoadAndDelete deletes the value for a key, returning the previous
// value if any. The loaded result reports whether the key was
// present.
func (m *Map) LoadAndDelete(key string) (value interface{}, loaded bool) {
	hash := maphash64(key)
	for {
		hintNonEmpty := 0
		table := (*mapTable)(atomic.LoadPointer(&m.table))
		bidx := bucketIdx(table, hash)
		rootb := &table.buckets[bidx]
		b := rootb
		rootb.mu.Lock()
		if m.newerTableExists(table) {
			// Someone resized the table. Go for another attempt.
			rootb.mu.Unlock()
			continue
		}
		if m.resizeInProgress() {
			// Resize is in progress. Wait, then go for another attempt.
			rootb.mu.Unlock()
			m.waitForResize()
			continue
		}
		for {
			for i := 0; i < entriesPerMapBucket; i++ {
				kp := b.keys[i]
				if kp != nil {
					k := derefKey(kp)
					if k == key {
						vp := b.values[i]
						// Deletion case. First we update the value, then the key.
						// This is important for atomic snapshot states.
						atomic.StorePointer(&b.values[i], nil)
						atomic.StorePointer(&b.keys[i], nil)
						leftEmpty := false
						if hintNonEmpty == 0 {
							leftEmpty = isEmpty(rootb)
						}
						rootb.mu.Unlock()
						addSize(table, bidx, -1)
						// Might need to shrink the table.
						if leftEmpty {
							m.resize(table, mapShrinkHint)
						}
						return derefValue(vp), true
					}
					hintNonEmpty++
				}
			}
			if b.next == nil {
				rootb.mu.Unlock()
				return nil, false
			}
			b = (*bucket)(b.next)
		}
	}
}

// Delete deletes the value for a key.
func (m *Map) Delete(key string) {
	m.LoadAndDelete(key)
}

func isEmpty(rootb *bucket) bool {
	b := rootb
	for {
		for i := 0; i < entriesPerMapBucket; i++ {
			if b.keys[i] != nil {
				return false
			}
		}
		if b.next == nil {
			return true
		}
		b = (*bucket)(b.next)
	}
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
	tablep := atomic.LoadPointer(&m.table)
	table := *(*mapTable)(tablep)
	bentries := make([]rangeEntry, 0, entriesPerMapBucket*(resizeMapThreshold+1))
	for i := range table.buckets {
		copyRangeEntries(&table.buckets[i], &bentries)
		for j := range bentries {
			k := derefKey(bentries[j].key)
			v := derefValue(bentries[j].value)
			if !f(k, v) {
				return
			}
		}
	}
}

func copyRangeEntries(b *bucket, destEntries *[]rangeEntry) {
	// Clean up the slice.
	for i := range *destEntries {
		(*destEntries)[i] = rangeEntry{}
	}
	*destEntries = (*destEntries)[:0]
	// Make a copy.
	rootb := b
	rootb.mu.Lock()
	for {
		for i := 0; i < entriesPerMapBucket; i++ {
			if b.keys[i] != nil {
				*destEntries = append(*destEntries, rangeEntry{
					key:   b.keys[i],
					value: b.values[i],
				})
			}
		}
		if b.next == nil {
			rootb.mu.Unlock()
			return
		}
		b = (*bucket)(b.next)
	}
}

// used in tests to verify the table counter
func (m *Map) size() int {
	table := (*mapTable)(atomic.LoadPointer(&m.table))
	return int(sumSize(table))
}

func derefKey(keyPtr unsafe.Pointer) string {
	return *(*string)(keyPtr)
}

func derefValue(valuePtr unsafe.Pointer) interface{} {
	value := *(*interface{})(valuePtr)
	if _, ok := value.(*nilValue); ok {
		return nil
	}
	return value
}

func bucketIdx(table *mapTable, hash uint64) uint64 {
	return uint64(len(table.buckets)-1) & hash
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
		b := &table.buckets[i]
		for {
			stats.Capacity += entriesPerMapBucket
			for i := 0; i < entriesPerMapBucket; i++ {
				if atomic.LoadPointer(&b.keys[i]) != nil {
					stats.Size++
					numEntries++
				}
			}
			if b.next == nil {
				break
			}
			b = (*bucket)(b.next)
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
