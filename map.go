package xsync

import (
	"fmt"
	"hash/maphash"
	"math"
	"runtime"
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
	// (one cache line) on 64-bit machines
	entriesPerMapBucket = 3
	// threshold fraction of table occupation to start a table shrinking
	// when deleting the last entry in a bucket chain
	mapShrinkFraction = 128
	// map load factor to trigger a table resize during insertion;
	// a map holds up to mapLoadFactor*entriesPerMapBucket*mapTableLen
	// key-value pairs (this is a soft limit)
	mapLoadFactor = 0.75
	// minimal table size, i.e. number of buckets; thus, minimal map
	// capacity can be calculated as entriesPerMapBucket*minMapTableLen
	minMapTableLen = 32
	// minimum counter stripes to use
	minMapCounterLen = 8
	// maximum counter stripes to use; stands for around 4KB of memory
	maxMapCounterLen = 32
)

var (
	topHashMask       = uint64((1<<20)-1) << 44
	topHashEntryMasks = [3]uint64{
		topHashMask,
		topHashMask >> 20,
		topHashMask >> 40,
	}
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
	buckets []bucketPadded
	// striped counter for number of table entries;
	// used to determine if a table shrinking is needed
	// occupies min(buckets_memory/1024, 64KB) of memory
	size []counterStripe
	seed maphash.Seed
}

type counterStripe struct {
	c int64
	//lint:ignore U1000 prevents false sharing
	pad [cacheLineSize - 8]byte
}

type bucketPadded struct {
	//lint:ignore U1000 ensure each bucket takes two cache lines on both 32 and 64-bit archs
	pad [cacheLineSize - unsafe.Sizeof(bucket{})]byte
	bucket
}

type bucket struct {
	next   unsafe.Pointer // *bucketPadded
	keys   [entriesPerMapBucket]unsafe.Pointer
	values [entriesPerMapBucket]unsafe.Pointer
	// topHashMutex is a 2-in-1 value.
	//
	// It contains packed top 20 bits (20 MSBs) of hash codes for keys
	// stored in the bucket:
	// | key 0's top hash | key 1's top hash | key 2's top hash | bitmap for keys | mutex |
	// |      20 bits     |      20 bits     |      20 bits     |     3 bits      | 1 bit |
	//
	// The least significant bit is used for the mutex (TTAS spinlock).
	topHashMutex uint64
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
	buckets := make([]bucketPadded, size)
	counterLen := size >> 10
	if counterLen < minMapCounterLen {
		counterLen = minMapCounterLen
	} else if counterLen > maxMapCounterLen {
		counterLen = maxMapCounterLen
	}
	counter := make([]counterStripe, counterLen)
	t := &mapTable{
		buckets: buckets,
		size:    counter,
		seed:    maphash.MakeSeed(),
	}
	return t
}

// Load returns the value stored in the map for a key, or nil if no
// value is present.
// The ok result indicates whether value was found in the map.
func (m *Map) Load(key string) (value interface{}, ok bool) {
	table := (*mapTable)(atomic.LoadPointer(&m.table))
	hash := hashString(table.seed, key)
	bidx := bucketIdx(table, hash)
	b := &table.buckets[bidx]
	for {
		topHashes := atomic.LoadUint64(&b.topHashMutex)
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
		bucketPtr := atomic.LoadPointer(&b.next)
		if bucketPtr == nil {
			return
		}
		b = (*bucketPadded)(bucketPtr)
	}
}

// Store sets the value for a key.
func (m *Map) Store(key string, value interface{}) {
	m.doStore(
		key,
		func() interface{} {
			return value
		},
		false,
	)
}

// LoadOrStore returns the existing value for the key if present.
// Otherwise, it stores and returns the given value.
// The loaded result is true if the value was loaded, false if stored.
func (m *Map) LoadOrStore(key string, value interface{}) (actual interface{}, loaded bool) {
	return m.doStore(
		key,
		func() interface{} {
			return value
		},
		true,
	)
}

// LoadAndStore returns the existing value for the key if present,
// while setting the new value for the key.
// It stores the new value and returns the existing one, if present.
// The loaded result is true if the existing value was loaded,
// false otherwise.
func (m *Map) LoadAndStore(key string, value interface{}) (actual interface{}, loaded bool) {
	return m.doStore(
		key,
		func() interface{} {
			return value
		},
		false,
	)
}

// LoadOrCompute returns the existing value for the key if present.
// Otherwise, it computes the value using the provided function and
// returns the computed value. The loaded result is true if the value
// was loaded, false if stored.
func (m *Map) LoadOrCompute(key string, valueFn func() interface{}) (actual interface{}, loaded bool) {
	return m.doStore(key, valueFn, true)
}

func (m *Map) doStore(key string, valueFn func() interface{}, loadIfExists bool) (interface{}, bool) {
	// Read-only path.
	if loadIfExists {
		if v, ok := m.Load(key); ok {
			return v, true
		}
	}
	// Write path.
	for {
	store_attempt:
		var (
			emptyb   *bucketPadded
			emptyidx int
		)
		table := (*mapTable)(atomic.LoadPointer(&m.table))
		tableLen := len(table.buckets)
		hash := hashString(table.seed, key)
		bidx := bucketIdx(table, hash)
		rootb := &table.buckets[bidx]
		b := rootb
		lockBucket(&rootb.topHashMutex)
		if m.newerTableExists(table) {
			// Someone resized the table. Go for another attempt.
			unlockBucket(&rootb.topHashMutex)
			goto store_attempt
		}
		if m.resizeInProgress() {
			// Resize is in progress. Wait, then go for another attempt.
			unlockBucket(&rootb.topHashMutex)
			m.waitForResize()
			goto store_attempt
		}
		for {
			topHashes := atomic.LoadUint64(&b.topHashMutex)
			for i := 0; i < entriesPerMapBucket; i++ {
				if b.keys[i] == nil {
					if emptyb == nil {
						emptyb = b
						emptyidx = i
					}
					continue
				}
				if !topHashMatch(hash, topHashes, i) {
					continue
				}
				if key == derefKey(b.keys[i]) {
					vp := b.values[i]
					if loadIfExists {
						unlockBucket(&rootb.topHashMutex)
						return derefValue(vp), true
					}
					// In-place update case. Luckily we get a copy of the value
					// interface{} on each call, thus the live value pointers are
					// unique. Otherwise atomic snapshot won't be correct in case
					// of multiple Store calls using the same value.
					value := valueFn()
					nvp := unsafe.Pointer(&value)
					if assertionsEnabled && vp == nvp {
						panic("non-unique value pointer")
					}
					atomic.StorePointer(&b.values[i], nvp)
					unlockBucket(&rootb.topHashMutex)
					// LoadAndStore expects the old value to be returned.
					return derefValue(vp), true
				}
			}
			if b.next == nil {
				if emptyb != nil {
					// Insertion case. First we update the value, then the key.
					// This is important for atomic snapshot states.
					topHashes = atomic.LoadUint64(&emptyb.topHashMutex)
					atomic.StoreUint64(&emptyb.topHashMutex, storeTopHash(hash, topHashes, emptyidx))
					value := valueFn()
					atomic.StorePointer(&emptyb.values[emptyidx], unsafe.Pointer(&value))
					atomic.StorePointer(&emptyb.keys[emptyidx], unsafe.Pointer(&key))
					unlockBucket(&rootb.topHashMutex)
					table.addSize(bidx, 1)
					return value, false
				}
				growThreshold := float64(tableLen) * entriesPerMapBucket * mapLoadFactor
				if table.sumSize() > int64(growThreshold) {
					// Need to grow the table. Then go for another attempt.
					unlockBucket(&rootb.topHashMutex)
					m.resize(table, mapGrowHint)
					goto store_attempt
				}
				// Create and append a new bucket.
				newb := new(bucketPadded)
				newb.keys[0] = unsafe.Pointer(&key)
				value := valueFn()
				newb.values[0] = unsafe.Pointer(&value)
				newb.topHashMutex = storeTopHash(hash, newb.topHashMutex, 0)
				atomic.StorePointer(&b.next, unsafe.Pointer(newb))
				unlockBucket(&rootb.topHashMutex)
				table.addSize(bidx, 1)
				return value, false
			}
			b = (*bucketPadded)(b.next)
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
		shrinkThreshold = int64((tableLen * entriesPerMapBucket) / mapShrinkFraction)
		if tableLen == minMapTableLen || table.sumSize() > shrinkThreshold {
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
		if table.sumSize() <= shrinkThreshold {
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
		newTable.addSizePlain(uint64(i), copied)
	}
	// Publish the new table and wake up all waiters.
	atomic.StorePointer(&m.table, unsafe.Pointer(newTable))
	m.resizeMu.Lock()
	atomic.StoreInt64(&m.resizing, 0)
	m.resizeCond.Broadcast()
	m.resizeMu.Unlock()
}

func copyBucket(b *bucketPadded, destTable *mapTable) (copied int) {
	rootb := b
	lockBucket(&rootb.topHashMutex)
	for {
		for i := 0; i < entriesPerMapBucket; i++ {
			if b.keys[i] != nil {
				k := derefKey(b.keys[i])
				hash := hashString(destTable.seed, k)
				bidx := bucketIdx(destTable, hash)
				destb := &destTable.buckets[bidx]
				appendToBucket(hash, b.keys[i], b.values[i], destb)
				copied++
			}
		}
		if b.next == nil {
			unlockBucket(&rootb.topHashMutex)
			return
		}
		b = (*bucketPadded)(b.next)
	}
}

func appendToBucket(hash uint64, keyPtr, valPtr unsafe.Pointer, b *bucketPadded) {
	for {
		for i := 0; i < entriesPerMapBucket; i++ {
			if b.keys[i] == nil {
				b.keys[i] = keyPtr
				b.values[i] = valPtr
				b.topHashMutex = storeTopHash(hash, b.topHashMutex, i)
				return
			}
		}
		if b.next == nil {
			newb := new(bucketPadded)
			newb.keys[0] = keyPtr
			newb.values[0] = valPtr
			newb.topHashMutex = storeTopHash(hash, newb.topHashMutex, 0)
			b.next = unsafe.Pointer(newb)
			return
		}
		b = (*bucketPadded)(b.next)
	}
}

// LoadAndDelete deletes the value for a key, returning the previous
// value if any. The loaded result reports whether the key was
// present.
func (m *Map) LoadAndDelete(key string) (value interface{}, loaded bool) {
	for {
		hintNonEmpty := 0
		table := (*mapTable)(atomic.LoadPointer(&m.table))
		hash := hashString(table.seed, key)
		bidx := bucketIdx(table, hash)
		rootb := &table.buckets[bidx]
		b := rootb
		lockBucket(&rootb.topHashMutex)
		if m.newerTableExists(table) {
			// Someone resized the table. Go for another attempt.
			unlockBucket(&rootb.topHashMutex)
			continue
		}
		if m.resizeInProgress() {
			// Resize is in progress. Wait, then go for another attempt.
			unlockBucket(&rootb.topHashMutex)
			m.waitForResize()
			continue
		}
		for {
			topHashes := atomic.LoadUint64(&b.topHashMutex)
			for i := 0; i < entriesPerMapBucket; i++ {
				kp := b.keys[i]
				if kp == nil || !topHashMatch(hash, topHashes, i) {
					continue
				}
				if key == derefKey(kp) {
					vp := b.values[i]
					// Deletion case. First we update the value, then the key.
					// This is important for atomic snapshot states.
					atomic.StoreUint64(&b.topHashMutex, eraseTopHash(topHashes, i))
					atomic.StorePointer(&b.values[i], nil)
					atomic.StorePointer(&b.keys[i], nil)
					leftEmpty := false
					if hintNonEmpty == 0 {
						leftEmpty = isEmpty(b)
					}
					unlockBucket(&rootb.topHashMutex)
					table.addSize(bidx, -1)
					// Might need to shrink the table.
					if leftEmpty {
						m.resize(table, mapShrinkHint)
					}
					return derefValue(vp), true
				}
				hintNonEmpty++
			}
			if b.next == nil {
				unlockBucket(&rootb.topHashMutex)
				return
			}
			b = (*bucketPadded)(b.next)
		}
	}
}

// Delete deletes the value for a key.
func (m *Map) Delete(key string) {
	m.LoadAndDelete(key)
}

func isEmpty(rootb *bucketPadded) bool {
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
		b = (*bucketPadded)(b.next)
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
	var bentries [entriesPerMapBucket]rangeEntry
	tablep := atomic.LoadPointer(&m.table)
	table := *(*mapTable)(tablep)
	for i := range table.buckets {
		b := &table.buckets[i]
		for {
			n := copyRangeEntries(b, &bentries)
			for j := 0; j < n; j++ {
				k := derefKey(bentries[j].key)
				v := derefValue(bentries[j].value)
				if !f(k, v) {
					return
				}
			}
			bucketPtr := atomic.LoadPointer(&b.next)
			if bucketPtr == nil {
				break
			}
			b = (*bucketPadded)(bucketPtr)
		}
	}
}

func copyRangeEntries(b *bucketPadded, destEntries *[entriesPerMapBucket]rangeEntry) int {
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
	return int(table.sumSize())
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

func lockBucket(mu *uint64) {
	for {
		var v uint64
		for {
			v = atomic.LoadUint64(mu)
			if v&1 != 1 {
				break
			}
			runtime.Gosched()
		}
		if atomic.CompareAndSwapUint64(mu, v, v|1) {
			return
		}
		runtime.Gosched()
	}
}

func unlockBucket(mu *uint64) {
	v := atomic.LoadUint64(mu)
	atomic.StoreUint64(mu, v&^1)
}

func topHashMatch(hash, topHashes uint64, idx int) bool {
	if topHashes&(1<<(idx+1)) == 0 {
		// Entry is not present.
		return false
	}
	hash = hash & topHashMask
	topHashes = (topHashes & topHashEntryMasks[idx]) << (20 * idx)
	return hash == topHashes
}

func storeTopHash(hash, topHashes uint64, idx int) uint64 {
	// Zero out top hash at idx.
	topHashes = topHashes &^ topHashEntryMasks[idx]
	// Chop top 20 MSBs of the given hash and position them at idx.
	hash = (hash & topHashMask) >> (20 * idx)
	// Store the MSBs.
	topHashes = topHashes | hash
	// Mark the entry as present.
	return topHashes | (1 << (idx + 1))
}

func eraseTopHash(topHashes uint64, idx int) uint64 {
	return topHashes &^ (1 << (idx + 1))
}

func (table *mapTable) addSize(bucketIdx uint64, delta int) {
	cidx := uint64(len(table.size)-1) & bucketIdx
	atomic.AddInt64(&table.size[cidx].c, int64(delta))
}

func (table *mapTable) addSizePlain(bucketIdx uint64, delta int) {
	cidx := uint64(len(table.size)-1) & bucketIdx
	table.size[cidx].c += int64(delta)
}

func (table *mapTable) sumSize() int64 {
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
	stats.Counter = int(table.sumSize())
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
			b = (*bucketPadded)(b.next)
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
