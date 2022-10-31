//go:build go1.18
// +build go1.18

package xsync

import (
	"fmt"
	"hash/maphash"
	"math"
	"sync"
	"sync/atomic"
	"unsafe"
)

// MapOf is like a Go map[string]V but is safe for concurrent
// use by multiple goroutines without additional locking or
// coordination. It follows the interface of sync.Map with
// a number of valuable extensions like Compute or Size.
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
type MapOf[K comparable, V any] struct {
	totalGrowths int64
	totalShrinks int64
	resizing     int64          // resize in progress flag; updated atomically
	resizeMu     sync.Mutex     // only used along with resizeCond
	resizeCond   sync.Cond      // used to wake up resize waiters (concurrent modifications)
	table        unsafe.Pointer // *mapTable
	hasher       func(maphash.Seed, K) uint64
}

// NewMapOf creates a new MapOf instance with string keys
func NewMapOf[V any]() *MapOf[string, V] {
	return NewTypedMapOf[string, V](hashString)
}

// IntegerConstraint represents any integer type.
type IntegerConstraint interface {
	// Recreation of golang.org/x/exp/constraints.Integer to avoid taking a dependency on an
	// experimental package.
	~int | ~int8 | ~int16 | ~int32 | ~int64 | ~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~uintptr
}

// NewIntegerMapOf creates a new MapOf instance with integer typed keys.
func NewIntegerMapOf[K IntegerConstraint, V any]() *MapOf[K, V] {
	return NewTypedMapOf[K, V](func(s maphash.Seed, k K) uint64 {
		return hashUint64(s, uint64(k))
	})
}

// NewTypedMapOf creates a new MapOf instance with arbitrarily typed keys.
// Keys are hashed to uint64 using the hasher function. It is strongly
// recommended to use the hash/maphash package to implement hasher. See the
// example for how to do that.
func NewTypedMapOf[K comparable, V any](hasher func(maphash.Seed, K) uint64) *MapOf[K, V] {
	m := &MapOf[K, V]{}
	m.resizeCond = *sync.NewCond(&m.resizeMu)
	m.hasher = hasher
	table := newMapTable(minMapTableLen)
	atomic.StorePointer(&m.table, unsafe.Pointer(table))
	return m
}

// Load returns the value stored in the map for a key, or nil if no
// value is present.
// The ok result indicates whether value was found in the map.
func (m *MapOf[K, V]) Load(key K) (value V, ok bool) {
	table := (*mapTable)(atomic.LoadPointer(&m.table))
	hash := m.hasher(table.seed, key)
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
				if key == derefTypedKey[K](kp) {
					if uintptr(vp) == uintptr(atomic.LoadPointer(&b.values[i])) {
						// Atomic snapshot succeeded.
						return derefTypedValue[V](vp), true
					}
					// Concurrent update/remove. Go for another spin.
					goto atomic_snapshot
				}
			}
		}
		bptr := atomic.LoadPointer(&b.next)
		if bptr == nil {
			return
		}
		b = (*bucketPadded)(bptr)
	}
}

// Store sets the value for a key.
func (m *MapOf[K, V]) Store(key K, value V) {
	m.doCompute(
		key,
		func(V, bool) (V, bool) {
			return value, false
		},
		false,
		false,
	)
}

// LoadOrStore returns the existing value for the key if present.
// Otherwise, it stores and returns the given value.
// The loaded result is true if the value was loaded, false if stored.
func (m *MapOf[K, V]) LoadOrStore(key K, value V) (actual V, loaded bool) {
	return m.doCompute(
		key,
		func(V, bool) (V, bool) {
			return value, false
		},
		true,
		false,
	)
}

// LoadAndStore returns the existing value for the key if present,
// while setting the new value for the key.
// It stores the new value and returns the existing one, if present.
// The loaded result is true if the existing value was loaded,
// false otherwise.
func (m *MapOf[K, V]) LoadAndStore(key K, value V) (actual V, loaded bool) {
	return m.doCompute(
		key,
		func(V, bool) (V, bool) {
			return value, false
		},
		false,
		false,
	)
}

// LoadOrCompute returns the existing value for the key if present.
// Otherwise, it computes the value using the provided function and
// returns the computed value. The loaded result is true if the value
// was loaded, false if stored.
func (m *MapOf[K, V]) LoadOrCompute(key K, valueFn func() V) (actual V, loaded bool) {
	return m.doCompute(
		key,
		func(V, bool) (V, bool) {
			return valueFn(), false
		},
		true,
		false,
	)
}

// Compute either sets the computed new value for the key or deletes
// the value for the key. When the delete result of the valueFn function
// is set to true, the value will be deleted, if it exists. When delete
// is set to false, the value is updated to the newValue.
// The ok result indicates whether value was computed and stored, thus, is
// present in the map. The actual result contains the new value in cases where
// the value was computed and stored. See the example for a few use cases.
func (m *MapOf[K, V]) Compute(
	key K,
	valueFn func(oldValue V, loaded bool) (newValue V, delete bool),
) (actual V, ok bool) {
	return m.doCompute(key, valueFn, false, true)
}

// LoadAndDelete deletes the value for a key, returning the previous
// value if any. The loaded result reports whether the key was
// present.
func (m *MapOf[K, V]) LoadAndDelete(key K) (value V, loaded bool) {
	return m.doCompute(
		key,
		func(value V, loaded bool) (V, bool) {
			return value, true
		},
		false,
		false,
	)
}

// Delete deletes the value for a key.
func (m *MapOf[K, V]) Delete(key K) {
	m.doCompute(
		key,
		func(value V, loaded bool) (V, bool) {
			return value, true
		},
		false,
		false,
	)
}

func (m *MapOf[K, V]) doCompute(
	key K,
	valueFn func(oldValue V, loaded bool) (V, bool),
	loadIfExists, computeOnly bool,
) (V, bool) {
	// Read-only path.
	if loadIfExists {
		if v, ok := m.Load(key); ok {
			return v, !computeOnly
		}
	}
	// Write path.
	for {
	compute_attempt:
		var (
			emptyb   *bucketPadded
			emptyidx int
		)
		hintNonEmpty := 0
		table := (*mapTable)(atomic.LoadPointer(&m.table))
		tableLen := len(table.buckets)
		hash := m.hasher(table.seed, key)
		bidx := bucketIdx(table, hash)
		rootb := &table.buckets[bidx]
		b := rootb
		lockBucket(&rootb.topHashMutex)
		if m.newerTableExists(table) {
			// Someone resized the table. Go for another attempt.
			unlockBucket(&rootb.topHashMutex)
			goto compute_attempt
		}
		if m.resizeInProgress() {
			// Resize is in progress. Wait, then go for another attempt.
			unlockBucket(&rootb.topHashMutex)
			m.waitForResize()
			goto compute_attempt
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
				if key == derefTypedKey[K](b.keys[i]) {
					vp := b.values[i]
					if loadIfExists {
						unlockBucket(&rootb.topHashMutex)
						return derefTypedValue[V](vp), !computeOnly
					}
					// In-place update/delete.
					// We get a copy of the value via an interface{} on each call,
					// thus the live value pointers are unique. Otherwise atomic
					// snapshot won't be correct in case of multiple Store calls
					// using the same value.
					oldValue := derefTypedValue[V](vp)
					newValue, del := valueFn(oldValue, true)
					if del {
						// Deletion.
						// First we update the value, then the key.
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
						return oldValue, !computeOnly
					}
					var wv interface{} = newValue
					nvp := unsafe.Pointer(&wv)
					if assertionsEnabled && vp == nvp {
						panic("non-unique value pointer")
					}
					atomic.StorePointer(&b.values[i], nvp)
					unlockBucket(&rootb.topHashMutex)
					if computeOnly {
						// Compute expects the new value to be returned.
						return newValue, true
					}
					// LoadAndStore expects the old value to be returned.
					return oldValue, true
				}
				hintNonEmpty++
			}
			if b.next == nil {
				if emptyb != nil {
					// Insertion into an existing bucket.
					var zeroedV V
					newValue, del := valueFn(zeroedV, false)
					if del {
						unlockBucket(&rootb.topHashMutex)
						return zeroedV, false
					}
					var wv interface{} = newValue
					// First we update the value, then the key.
					// This is important for atomic snapshot states.
					topHashes = atomic.LoadUint64(&emptyb.topHashMutex)
					atomic.StoreUint64(&emptyb.topHashMutex, storeTopHash(hash, topHashes, emptyidx))
					atomic.StorePointer(&emptyb.values[emptyidx], unsafe.Pointer(&wv))
					atomic.StorePointer(&emptyb.keys[emptyidx], unsafe.Pointer(&key))
					unlockBucket(&rootb.topHashMutex)
					table.addSize(bidx, 1)
					return newValue, computeOnly
				}
				growThreshold := float64(tableLen) * entriesPerMapBucket * mapLoadFactor
				if table.sumSize() > int64(growThreshold) {
					// Need to grow the table. Then go for another attempt.
					unlockBucket(&rootb.topHashMutex)
					m.resize(table, mapGrowHint)
					goto compute_attempt
				}
				// Insertion into a new bucket.
				var zeroedV V
				newValue, del := valueFn(zeroedV, false)
				if del {
					unlockBucket(&rootb.topHashMutex)
					return newValue, false
				}
				var wv interface{} = newValue
				// Create and append the bucket.
				newb := new(bucketPadded)
				newb.keys[0] = unsafe.Pointer(&key)
				newb.values[0] = unsafe.Pointer(&wv)
				newb.topHashMutex = storeTopHash(hash, newb.topHashMutex, 0)
				atomic.StorePointer(&b.next, unsafe.Pointer(newb))
				unlockBucket(&rootb.topHashMutex)
				table.addSize(bidx, 1)
				return newValue, computeOnly
			}
			b = (*bucketPadded)(b.next)
		}
	}
}

func (m *MapOf[K, V]) newerTableExists(table *mapTable) bool {
	curTablePtr := atomic.LoadPointer(&m.table)
	return uintptr(curTablePtr) != uintptr(unsafe.Pointer(table))
}

func (m *MapOf[K, V]) resizeInProgress() bool {
	return atomic.LoadInt64(&m.resizing) == 1
}

func (m *MapOf[K, V]) waitForResize() {
	m.resizeMu.Lock()
	for m.resizeInProgress() {
		m.resizeCond.Wait()
	}
	m.resizeMu.Unlock()
}

func (m *MapOf[K, V]) resize(table *mapTable, hint mapResizeHint) {
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
	case mapClearHint:
		newTable = newMapTable(minMapTableLen)
	default:
		panic(fmt.Sprintf("unexpected resize hint: %d", hint))
	}
	// Copy the data only if we're not clearing the map.
	if hint != mapClearHint {
		for i := 0; i < tableLen; i++ {
			copied := copyBucketOf(&table.buckets[i], newTable, m.hasher)
			newTable.addSizePlain(uint64(i), copied)
		}
	}
	// Publish the new table and wake up all waiters.
	atomic.StorePointer(&m.table, unsafe.Pointer(newTable))
	m.resizeMu.Lock()
	atomic.StoreInt64(&m.resizing, 0)
	m.resizeCond.Broadcast()
	m.resizeMu.Unlock()
}

func copyBucketOf[K comparable](b *bucketPadded, destTable *mapTable, hasher func(maphash.Seed, K) uint64) (copied int) {
	rootb := b
	lockBucket(&rootb.topHashMutex)
	for {
		for i := 0; i < entriesPerMapBucket; i++ {
			if b.keys[i] != nil {
				hash := hasher(destTable.seed, derefTypedKey[K](b.keys[i]))
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
func (m *MapOf[K, V]) Range(f func(key K, value V) bool) {
	var bentries [entriesPerMapBucket]rangeEntry
	tablep := atomic.LoadPointer(&m.table)
	table := *(*mapTable)(tablep)
	for i := range table.buckets {
		b := &table.buckets[i]
		for {
			n := copyRangeEntries(b, &bentries)
			for j := 0; j < n; j++ {
				k := derefTypedKey[K](bentries[j].key)
				v := derefTypedValue[V](bentries[j].value)
				if !f(k, v) {
					return
				}
			}
			bptr := atomic.LoadPointer(&b.next)
			if bptr == nil {
				break
			}
			b = (*bucketPadded)(bptr)
		}
	}
}

// Clear deletes all keys and values currently stored in the map.
func (m *MapOf[K, V]) Clear() {
	table := (*mapTable)(atomic.LoadPointer(&m.table))
	m.resize(table, mapClearHint)
}

// Size returns current size of the map.
func (m *MapOf[K, V]) Size() int {
	table := (*mapTable)(atomic.LoadPointer(&m.table))
	return int(table.sumSize())
}

func derefTypedKey[K comparable](keyPtr unsafe.Pointer) (key K) {
	return *(*K)(keyPtr)
}

func derefTypedValue[V any](valuePtr unsafe.Pointer) (val V) {
	return (*(*interface{})(valuePtr)).(V)
}

// O(N) operation; use for debug purposes only
func (m *MapOf[K, V]) stats() mapStats {
	stats := mapStats{
		TotalGrowths: atomic.LoadInt64(&m.totalGrowths),
		TotalShrinks: atomic.LoadInt64(&m.totalShrinks),
		MinEntries:   math.MaxInt32,
	}
	table := (*mapTable)(atomic.LoadPointer(&m.table))
	stats.RootBuckets = len(table.buckets)
	stats.Counter = int(table.sumSize())
	stats.CounterLen = len(table.size)
	for i := range table.buckets {
		nentries := 0
		b := &table.buckets[i]
		stats.TotalBuckets++
		for {
			nentriesLocal := 0
			stats.Capacity += entriesPerMapBucket
			for i := 0; i < entriesPerMapBucket; i++ {
				if atomic.LoadPointer(&b.keys[i]) != nil {
					stats.Size++
					nentriesLocal++
				}
			}
			nentries += nentriesLocal
			if nentriesLocal == 0 {
				stats.EmptyBuckets++
			}
			if b.next == nil {
				break
			}
			b = (*bucketPadded)(b.next)
			stats.TotalBuckets++
		}
		if nentries < stats.MinEntries {
			stats.MinEntries = nentries
		}
		if nentries > stats.MaxEntries {
			stats.MaxEntries = nentries
		}
	}
	return stats
}
