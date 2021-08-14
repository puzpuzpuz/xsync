package xsync

import (
	"sync"
	"sync/atomic"
	"unsafe"
)

const (
	// number of entries per bucket; 3 entries lead to size of 64B
	// (cache line) on 64-bit machines
	entriesPerMapBucket = 3
	// max number of linked buckets (chain size) to trigger a table
	// resize during insertion; thus, each chain holds up to
	// resizeMapThreshold+1 buckets
	resizeMapThreshold = 2
	// initial table size, i.e. number of buckets; thus, initial
	// capacity can be calculated as entriesPerMapBucket*initialMapTableLen
	initialMapTableLen = 32
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
	table      unsafe.Pointer // []bucket
	resizing   uint64         // resize in progress flag; updated atomically
	resizeMu   sync.Mutex     // only used along with resizeCond
	resizeCond sync.Cond      // used for resize waiters (concurrent modifications)
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
	table := make([]bucket, initialMapTableLen)
	atomic.StorePointer(&m.table, unsafe.Pointer(&table))
	return m
}

// Load returns the value stored in the map for a key, or nil if no
// value is present.
// The ok result indicates whether value was found in the map.
func (m *Map) Load(key string) (value interface{}, ok bool) {
	hash := fnv32(key)
	tablep := atomic.LoadPointer(&m.table)
	table := *(*[]bucket)(tablep)
	b := &table[uint32(len(table)-1)&hash]
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
	hash := fnv32(key)
	for {
	store_attempt:
		var emptykp, emptyvp *unsafe.Pointer
		tablep := atomic.LoadPointer(&m.table)
		table := *(*[]bucket)(tablep)
		rootb := &table[uint32(len(table)-1)&hash]
		b := rootb
		chainLen := 0
		rootb.mu.Lock()
		if m.newerTableExists(tablep) {
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
					return
				}
				if chainLen == resizeMapThreshold {
					// Need to resize the table. Then go for another attempt.
					rootb.mu.Unlock()
					m.resize(tablep)
					goto store_attempt
				}
				// Create and append a new bucket.
				newb := new(bucket)
				newb.keys[0] = unsafe.Pointer(&key)
				newb.values[0] = unsafe.Pointer(&value)
				atomic.StorePointer(&b.next, unsafe.Pointer(newb))
				rootb.mu.Unlock()
				return
			}
			b = (*bucket)(b.next)
			chainLen++
		}
	}
}

func (m *Map) newerTableExists(tablePtr unsafe.Pointer) bool {
	curTablePtr := atomic.LoadPointer(&m.table)
	return uintptr(curTablePtr) != uintptr(tablePtr)
}

func (m *Map) resizeInProgress() bool {
	return atomic.LoadUint64(&m.resizing) == 1
}

func (m *Map) waitForResize() {
	m.resizeMu.Lock()
	for m.resizeInProgress() {
		m.resizeCond.Wait()
	}
	m.resizeMu.Unlock()
}

func (m *Map) resize(tablePtr unsafe.Pointer) {
	if !atomic.CompareAndSwapUint64(&m.resizing, 0, 1) {
		// Someone else started resize. Wait for it to finish.
		m.waitForResize()
		return
	}
	table := *(*[]bucket)(tablePtr)
	// Grow the table with factor of 2.
	newTable := make([]bucket, len(table)<<1)
	for i := 0; i < len(table); i++ {
		copyBucket(&table[i], newTable)
	}
	// Publish the new table and wake up all waiters.
	atomic.StorePointer(&m.table, unsafe.Pointer(&newTable))
	m.resizeMu.Lock()
	atomic.StoreUint64(&m.resizing, 0)
	m.resizeCond.Broadcast()
	m.resizeMu.Unlock()
}

func copyBucket(b *bucket, table []bucket) {
	rootb := b
	rootb.mu.Lock()
	for {
		for i := 0; i < entriesPerMapBucket; i++ {
			if b.keys[i] != nil {
				k := derefKey(b.keys[i])
				hash := fnv32(k)
				destb := &table[uint32(len(table)-1)&hash]
				appendToBucket(destb, b.keys[i], b.values[i])
			}
		}
		if b.next == nil {
			rootb.mu.Unlock()
			return
		}
		b = (*bucket)(b.next)
	}
}

func appendToBucket(b *bucket, keyPtr, valPtr unsafe.Pointer) {
	for {
		for i := 0; i < entriesPerMapBucket; i++ {
			if b.keys[i] == nil {
				b.keys[i] = keyPtr
				b.values[i] = valPtr
				return
			}
		}
		if b.next == nil {
			newb := new(bucket)
			newb.keys[0] = keyPtr
			newb.values[0] = valPtr
			b.next = unsafe.Pointer(newb)
			return
		}
		b = (*bucket)(b.next)
	}
}

// LoadAndDelete deletes the value for a key, returning the previous
// value if any. The loaded result reports whether the key was
// present.
func (m *Map) LoadAndDelete(key string) (value interface{}, loaded bool) {
	hash := fnv32(key)
	for {
		tablep := atomic.LoadPointer(&m.table)
		table := *(*[]bucket)(tablep)
		rootb := &table[uint32(len(table)-1)&hash]
		b := rootb
		rootb.mu.Lock()
		if m.newerTableExists(tablep) {
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
						rootb.mu.Unlock()
						return derefValue(vp), true
					}
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
	table := *(*[]bucket)(tablep)
	bentries := make([]rangeEntry, 0, entriesPerMapBucket*(resizeMapThreshold+1))
	for i := range table {
		copyRangeEntries(&bentries, &table[i])
		for j := range bentries {
			if bentries[j].key == nil {
				break
			}
			k := derefKey(bentries[j].key)
			v := derefValue(bentries[j].value)
			if !f(k, v) {
				return
			}
		}
	}
}

func copyRangeEntries(bentries *[]rangeEntry, b *bucket) {
	// Clean up the slice.
	for i := range *bentries {
		(*bentries)[i] = rangeEntry{}
	}
	*bentries = (*bentries)[:0]
	// Make a copy.
	idx := 0
	rootb := b
	rootb.mu.Lock()
	for {
		for i := 0; i < entriesPerMapBucket; i++ {
			if b.keys[i] != nil {
				*bentries = append(*bentries, rangeEntry{
					key:   b.keys[i],
					value: b.values[i],
				})
				idx++
			}
		}
		if b.next == nil {
			rootb.mu.Unlock()
			return
		}
		b = (*bucket)(b.next)
	}
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
