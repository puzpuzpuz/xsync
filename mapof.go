//go:build go1.18
// +build go1.18

package xsync

import (
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"unsafe"
)

// MapOf is like a Go map[string]V but is safe for concurrent
// use by multiple goroutines without additional locking or
// coordination. It follows the interface of sync.Map.
//
// A MapOf must not be copied after first use.
//
// MapOf uses a modified version of Cache-Line Hash Table (CLHT)
// data structure: https://github.com/LPD-EPFL/CLHT
//
// CLHT is built around idea to organize the hash table in
// cache-line-sized buckets, so that on all modern CPUs update
// operations complete with at most one cache-line transfer.
// Also, Get operations involve no write to memory, as well as no
// mutexes or any other sort of locks. Due to this design, in all
// considered scenarios MapOf outperforms sync.Map.
//
// One important difference with sync.Map is that only string keys
// are supported. That's because Golang standard library does not
// expose the built-in hash functions for interface{} values.
type MapOf[V any] struct {
	totalGrowths int64
	totalShrinks int64
	resizing     int64          // resize in progress flag; updated atomically
	resizeMu     sync.Mutex     // only used along with resizeCond
	resizeCond   sync.Cond      // used to wake up resize waiters (concurrent modifications)
	table        unsafe.Pointer // *mapTable
}

// NewMapOf creates a new MapOf instance.
func NewMapOf[V any]() *MapOf[V] {
	m := &MapOf[V]{}
	m.resizeCond = *sync.NewCond(&m.resizeMu)
	table := newMapTable(minMapTableLen)
	atomic.StorePointer(&m.table, unsafe.Pointer(table))
	return m
}

// Load returns the value stored in the map for a key, or nil if no
// value is present.
// The ok result indicates whether value was found in the map.
func (m *MapOf[V]) Load(key string) (value V, ok bool) {
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
					return derefTypedValue[V](vp), true
				}
				// Concurrent update/remove. Go for another spin.
				goto atomic_snapshot
			}
		}
	}
	return value, false
}

// Store sets the value for a key.
func (m *MapOf[V]) Store(key string, value V) {
	m.doStore(key, value, false)
}

// LoadOrStore returns the existing value for the key if present.
// Otherwise, it stores and returns the given value.
// The loaded result is true if the value was loaded, false if stored.
func (m *MapOf[V]) LoadOrStore(key string, value V) (actual V, loaded bool) {
	return m.doStore(key, value, true)
}

// LoadAndStore returns the existing value for the key if present,
// while setting the new value for the key.
// Otherwise, it stores and returns the given value.
// The loaded result is true if the value was loaded, false otherwise.
func (m *MapOf[V]) LoadAndStore(key string, value V) (actual V, loaded bool) {
	return m.doStore(key, value, false)
}

func (m *MapOf[V]) doStore(key string, value V, loadIfExists bool) (V, bool) {
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
					return derefTypedValue[V](vp), true
				}
				// In-place update case. We get a copy of the value via an
				// interface{} on each call, thus the live value pointers are
				// unique. Otherwise atomic snapshot won't be correct in case
				// of multiple Store calls using the same value.
				var wv interface{} = value
				nvp := unsafe.Pointer(&wv)
				if assertionsEnabled && vp == nvp {
					panic("non-unique value pointer")
				}
				atomic.StorePointer(&b.values[i], nvp)
				b.mu.Unlock()
				return derefTypedValue[V](vp), true
			}
		}
		if emptykp != nil {
			// Insertion case. First we update the value, then the key.
			// This is important for atomic snapshot states.
			atomic.StoreUint64(&b.topHashes, storeTopHash(hash, b.topHashes, emptyidx))
			var wv interface{} = value
			atomic.StorePointer(emptyvp, unsafe.Pointer(&wv))
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

func (m *MapOf[V]) newerTableExists(table *mapTable) bool {
	curTablePtr := atomic.LoadPointer(&m.table)
	return uintptr(curTablePtr) != uintptr(unsafe.Pointer(table))
}

func (m *MapOf[V]) resizeInProgress() bool {
	return atomic.LoadInt64(&m.resizing) == 1
}

func (m *MapOf[V]) waitForResize() {
	m.resizeMu.Lock()
	for m.resizeInProgress() {
		m.resizeCond.Wait()
	}
	m.resizeMu.Unlock()
}

func (m *MapOf[V]) resize(table *mapTable, hint mapResizeHint) {
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

// LoadAndDelete deletes the value for a key, returning the previous
// value if any. The loaded result reports whether the key was
// present.
func (m *MapOf[V]) LoadAndDelete(key string) (value V, loaded bool) {
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
			return derefTypedValue[V](vp), true
		}
		hintNonEmpty++
	}
	b.mu.Unlock()
	return value, false
}

// Delete deletes the value for a key.
func (m *MapOf[V]) Delete(key string) {
	m.LoadAndDelete(key)
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
func (m *MapOf[V]) Range(f func(key string, value V) bool) {
	var bentries [entriesPerMapBucket]rangeEntry
	tablep := atomic.LoadPointer(&m.table)
	table := *(*mapTable)(tablep)
	for i := range table.buckets {
		n := copyRangeEntries(&table.buckets[i], &bentries)
		for j := 0; j < n; j++ {
			k := derefKey(bentries[j].key)
			v := derefTypedValue[V](bentries[j].value)
			if !f(k, v) {
				return
			}
		}
	}
}

// Size returns current size of the map.
func (m *MapOf[V]) Size() int {
	table := (*mapTable)(atomic.LoadPointer(&m.table))
	return int(sumSize(table))
}

func derefTypedValue[V any](valuePtr unsafe.Pointer) (val V) {
	return (*(*interface{})(valuePtr)).(V)
}

// O(N) operation; use for debug purposes only
func (m *MapOf[V]) stats() mapStats {
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
