package xsync

import (
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"unsafe"
)

const (
	// number of CowMapOf entries per bucket; 4 entries lead to size of 64B
	// (one cache line) on 64-bit machines
	entriesPerCowMapOfBucket        = 4
	cowMetaMask              uint64 = 0xffffffff
	defaultCowMetaMasked     uint64 = defaultMeta & cowMetaMask
)

// CowMapOf is like MapOf, but has method Copy with copy-on-write semantics.
// See Copy method for more info.
type CowMapOf[K comparable, V any] struct {
	totalCopies  int64
	totalGrowths int64
	totalShrinks int64
	resizing     int64          // resize in progress flag; updated atomically
	resizeMu     sync.Mutex     // only used along with resizeCond
	resizeCond   sync.Cond      // used to wake up resize waiters (concurrent modifications)
	table        unsafe.Pointer // *cowMapOfTable
	hasher       func(K, uint64) uint64
	minTableLen  int
	growOnly     bool
}

type cowMapOfTable[K comparable, V any] struct {
	buckets []cowBucketOfPadded
	// striped counter for number of table entries;
	// used to determine if a table shrinking is needed
	// occupies min(buckets_memory/1024, 64KB) of memory
	size        []counterStripe
	seed        uint64
	isoId       uint32 // map isolation id (generation)
	copyOnWrite bool   // flag if copy required before write
}

// cowBucketOfPadded is a CL-sized map bucket holding up to
// entriesPerCowMapOfBucket entries.
type cowBucketOfPadded struct {
	//lint:ignore U1000 ensure each bucket takes two cache lines on both 32 and 64-bit archs
	pad [cacheLineSize - unsafe.Sizeof(cowBucketOf{})]byte
	cowBucketOf
}

type cowBucketOf struct {
	isoId   uint32 // bucket isolation id (generation)
	meta    uint64
	entries [entriesPerCowMapOfBucket]unsafe.Pointer // *entryOf
	next    unsafe.Pointer                           // *cowBucketOfPadded
	mu      sync.Mutex
}

// NewCowMapOf creates a new CowMapOf instance configured with the given
// options.
func NewCowMapOf[K comparable, V any](options ...func(*MapConfig)) *CowMapOf[K, V] {
	return NewCowMapOfWithHasher[K, V](defaultHasher[K](), options...)
}

// NewCowMapOfWithHasher creates a new CowMapOf instance configured with
// the given hasher and options. The hash function is used instead
// of the built-in hash function configured when a map is created
// with the NewCowMapOf function.
func NewCowMapOfWithHasher[K comparable, V any](
	hasher func(K, uint64) uint64,
	options ...func(*MapConfig),
) *CowMapOf[K, V] {
	c := &MapConfig{
		sizeHint: defaultMinMapTableLen * entriesPerCowMapOfBucket,
	}
	for _, o := range options {
		o(c)
	}

	m := &CowMapOf[K, V]{}
	m.resizeCond = sync.Cond{L: &m.resizeMu}
	m.hasher = hasher
	var table *cowMapOfTable[K, V]
	if c.sizeHint <= defaultMinMapTableLen*entriesPerCowMapOfBucket {
		table = newCowMapOfTable[K, V](defaultMinMapTableLen, newIsoId())
	} else {
		tableLen := nextPowOf2(uint32((float64(c.sizeHint) / entriesPerCowMapOfBucket) / mapLoadFactor))
		table = newCowMapOfTable[K, V](int(tableLen), newIsoId())
	}
	m.minTableLen = len(table.buckets)
	m.growOnly = c.growOnly
	m.table = unsafe.Pointer(table)
	return m
}

func newCowMapOfTable[K comparable, V any](minTableLen int, isoId uint32) *cowMapOfTable[K, V] {
	buckets := make([]cowBucketOfPadded, minTableLen)
	for i := range buckets {
		buckets[i].meta = defaultMeta
		buckets[i].isoId = isoId
	}
	counterLen := minTableLen >> 10
	if counterLen < minMapCounterLen {
		counterLen = minMapCounterLen
	} else if counterLen > maxMapCounterLen {
		counterLen = maxMapCounterLen
	}
	counter := make([]counterStripe, counterLen)
	t := &cowMapOfTable[K, V]{
		buckets: buckets,
		size:    counter,
		seed:    makeSeed(),
		isoId:   isoId,
	}
	return t
}

// ToPlainCowMapOf returns a native map with a copy of xsync CowMapOf's
// contents. The copied xsync CowMapOf should not be modified while
// this call is made. If the copied CowMapOf is modified, the copying
// behavior is the same as in the Range method.
//
// See Copy method if you need xsync CowMapOf's content snapshot.
func ToPlainCowMapOf[K comparable, V any](m *CowMapOf[K, V]) map[K]V {
	pm := make(map[K]V)
	if m != nil {
		m.Range(func(key K, value V) bool {
			pm[key] = value
			return true
		})
	}
	return pm
}

// Load returns the value stored in the map for a key, or zero value
// of type V if no value is present.
// The ok result indicates whether value was found in the map.
func (m *CowMapOf[K, V]) Load(key K) (value V, ok bool) {
	table := (*cowMapOfTable[K, V])(atomic.LoadPointer(&m.table))
	hash := m.hasher(key, table.seed)
	h1 := h1(hash)
	h2w := broadcast(h2(hash))
	bidx := uint64(len(table.buckets)-1) & h1
	b := &table.buckets[bidx]
	for {
		metaw := atomic.LoadUint64(&b.meta)
		markedw := markZeroBytes(metaw^h2w) & cowMetaMask
		for markedw != 0 {
			idx := firstMarkedByteIndex(markedw)
			eptr := atomic.LoadPointer(&b.entries[idx])
			if eptr != nil {
				e := (*entryOf[K, V])(eptr)
				if e.key == key {
					return e.value, true
				}
			}
			markedw &= markedw - 1
		}
		bptr := atomic.LoadPointer(&b.next)
		if bptr == nil {
			return
		}
		b = (*cowBucketOfPadded)(bptr)
	}
}

// Store sets the value for a key.
func (m *CowMapOf[K, V]) Store(key K, value V) {
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
func (m *CowMapOf[K, V]) LoadOrStore(key K, value V) (actual V, loaded bool) {
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
func (m *CowMapOf[K, V]) LoadAndStore(key K, value V) (actual V, loaded bool) {
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
// Otherwise, it computes the value using the provided function, and
// then stores and returns the computed value. The loaded result is
// true if the value was loaded, false if computed.
//
// This call locks a hash table bucket while the compute function
// is executed. It means that modifications on other entries in
// the bucket will be blocked until the valueFn executes. Consider
// this when the function includes long-running operations.
func (m *CowMapOf[K, V]) LoadOrCompute(key K, valueFn func() V) (actual V, loaded bool) {
	return m.doCompute(
		key,
		func(V, bool) (V, bool) {
			return valueFn(), false
		},
		true,
		false,
	)
}

// LoadOrTryCompute returns the existing value for the key if present.
// Otherwise, it tries to compute the value using the provided function
// and, if successful, stores and returns the computed value. The loaded
// result is true if the value was loaded, or false if computed (whether
// successfully or not). If the compute attempt was cancelled (due to an
// error, for example), a zero value of type V will be returned.
//
// This call locks a hash table bucket while the compute function
// is executed. It means that modifications on other entries in
// the bucket will be blocked until the valueFn executes. Consider
// this when the function includes long-running operations.
func (m *CowMapOf[K, V]) LoadOrTryCompute(
	key K,
	valueFn func() (newValue V, cancel bool),
) (value V, loaded bool) {
	return m.doCompute(
		key,
		func(V, bool) (V, bool) {
			nv, c := valueFn()
			if !c {
				return nv, false
			}
			return nv, true // nv is ignored
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
//
// This call locks a hash table bucket while the compute function
// is executed. It means that modifications on other entries in
// the bucket will be blocked until the valueFn executes. Consider
// this when the function includes long-running operations.
func (m *CowMapOf[K, V]) Compute(
	key K,
	valueFn func(oldValue V, loaded bool) (newValue V, delete bool),
) (actual V, ok bool) {
	return m.doCompute(key, valueFn, false, true)
}

// LoadAndDelete deletes the value for a key, returning the previous
// value if any. The loaded result reports whether the key was
// present.
func (m *CowMapOf[K, V]) LoadAndDelete(key K) (value V, loaded bool) {
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
func (m *CowMapOf[K, V]) Delete(key K) {
	m.doCompute(
		key,
		func(value V, loaded bool) (V, bool) {
			return value, true
		},
		false,
		false,
	)
}

func (m *CowMapOf[K, V]) doCompute(
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
			emptyb   *cowBucketOfPadded
			emptyidx int
		)
		table := (*cowMapOfTable[K, V])(atomic.LoadPointer(&m.table))
		tableLen := len(table.buckets)
		hash := m.hasher(key, table.seed)
		h1 := h1(hash)
		h2 := h2(hash)
		h2w := broadcast(h2)
		bidx := uint64(len(table.buckets)-1) & h1
		rootb := &table.buckets[bidx]
		rootb.mu.Lock()
		// The following two checks must go in reverse to what's
		// in the resize method.
		if m.resizeInProgress() {
			// Resize is in progress. Wait, then go for another attempt.
			rootb.mu.Unlock()
			m.waitForResize()
			goto compute_attempt
		}
		if m.newerTableExists(table) {
			// Someone resized the table. Go for another attempt.
			rootb.mu.Unlock()
			goto compute_attempt
		}
		// The following two checks ensures that data was transferred from
		// shared table after Copy according to copy-on-write semantics.
		if table.copyOnWrite {
			rootb.mu.Unlock()
			m.resize(table, mapCopyHint)
			goto compute_attempt
		}
		if rootb.isoId != table.isoId {
			// Bucket points to other table after map was copied.
			// Update bucket references and resume processing.
			updateCowBucketOfPaddedRefs(rootb, table.isoId)
		}
		b := rootb
		for {
			metaw := b.meta
			markedw := markZeroBytes(metaw^h2w) & cowMetaMask
			for markedw != 0 {
				idx := firstMarkedByteIndex(markedw)
				eptr := b.entries[idx]
				if eptr != nil {
					e := (*entryOf[K, V])(eptr)
					if e.key == key {
						if loadIfExists {
							rootb.mu.Unlock()
							return e.value, !computeOnly
						}
						// In-place update/delete.
						// We get a copy of the value via an interface{} on each call,
						// thus the live value pointers are unique. Otherwise atomic
						// snapshot won't be correct in case of multiple Store calls
						// using the same value.
						oldv := e.value
						newv, del := valueFn(oldv, true)
						if del {
							// Deletion.
							// First we update the hash, then the entry.
							newmetaw := setByte(metaw, emptyMetaSlot, idx)
							atomic.StoreUint64(&b.meta, newmetaw)
							atomic.StorePointer(&b.entries[idx], nil)
							rootb.mu.Unlock()
							table.addSize(bidx, -1)
							// Might need to shrink the table if we left bucket empty.
							if newmetaw == defaultMeta {
								m.resize(table, mapShrinkHint)
							}
							return oldv, !computeOnly
						}
						newe := new(entryOf[K, V])
						newe.key = key
						newe.value = newv
						atomic.StorePointer(&b.entries[idx], unsafe.Pointer(newe))
						rootb.mu.Unlock()
						if computeOnly {
							// Compute expects the new value to be returned.
							return newv, true
						}
						// LoadAndStore expects the old value to be returned.
						return oldv, true
					}
				}
				markedw &= markedw - 1
			}
			if emptyb == nil {
				// Search for empty entries (up to 4 per bucket).
				emptyw := metaw & defaultCowMetaMasked
				if emptyw != 0 {
					idx := firstMarkedByteIndex(emptyw)
					emptyb = b
					emptyidx = idx
				}
			}
			if b.next == nil {
				if emptyb != nil {
					// Insertion into an existing bucket.
					var zeroV V
					newValue, del := valueFn(zeroV, false)
					if del {
						rootb.mu.Unlock()
						return zeroV, false
					}
					newe := new(entryOf[K, V])
					newe.key = key
					newe.value = newValue
					// First we update meta, then the entry.
					atomic.StoreUint64(&emptyb.meta, setByte(emptyb.meta, h2, emptyidx))
					atomic.StorePointer(&emptyb.entries[emptyidx], unsafe.Pointer(newe))
					rootb.mu.Unlock()
					table.addSize(bidx, 1)
					return newValue, computeOnly
				}
				growThreshold := float64(tableLen) * entriesPerCowMapOfBucket * mapLoadFactor
				if table.sumSize() > int64(growThreshold) {
					// Need to grow the table. Then go for another attempt.
					rootb.mu.Unlock()
					m.resize(table, mapGrowHint)
					goto compute_attempt
				}
				// Insertion into a new bucket.
				var zeroV V
				newValue, del := valueFn(zeroV, false)
				if del {
					rootb.mu.Unlock()
					return newValue, false
				}
				// Create and append a bucket.
				newb := new(cowBucketOfPadded)
				newb.meta = setByte(defaultMeta, h2, 0)
				newe := new(entryOf[K, V])
				newe.key = key
				newe.value = newValue
				newb.entries[0] = unsafe.Pointer(newe)
				atomic.StorePointer(&b.next, unsafe.Pointer(newb))
				rootb.mu.Unlock()
				table.addSize(bidx, 1)
				return newValue, computeOnly
			}
			b = (*cowBucketOfPadded)(b.next)
		}
	}
}

func (m *CowMapOf[K, V]) newerTableExists(table *cowMapOfTable[K, V]) bool {
	curTablePtr := atomic.LoadPointer(&m.table)
	return uintptr(curTablePtr) != uintptr(unsafe.Pointer(table))
}

func (m *CowMapOf[K, V]) resizeInProgress() bool {
	return atomic.LoadInt64(&m.resizing) == 1
}

func (m *CowMapOf[K, V]) waitForResize() {
	m.resizeMu.Lock()
	for m.resizeInProgress() {
		m.resizeCond.Wait()
	}
	m.resizeMu.Unlock()
}

func (m *CowMapOf[K, V]) resize(knownTable *cowMapOfTable[K, V], hint mapResizeHint) bool {
	knownTableLen := len(knownTable.buckets)
	// Fast path for shrink attempts.
	if hint == mapShrinkHint {
		if m.growOnly ||
			m.minTableLen == knownTableLen ||
			knownTable.sumSize() > int64((knownTableLen*entriesPerCowMapOfBucket)/mapShrinkFraction) {
			return false
		}
	}
	// Slow path.
	if !atomic.CompareAndSwapInt64(&m.resizing, 0, 1) {
		// Someone else started resize. Wait for it to finish.
		m.waitForResize()
		return false
	}
	var newTable *cowMapOfTable[K, V]
	table := (*cowMapOfTable[K, V])(atomic.LoadPointer(&m.table))
	tableLen := len(table.buckets)
	switch hint {
	case mapGrowHint:
		// Grow the table with factor of 2.
		atomic.AddInt64(&m.totalGrowths, 1)
		newTable = newCowMapOfTable[K, V](tableLen<<1, table.isoId)
	case mapShrinkHint:
		shrinkThreshold := int64((tableLen * entriesPerCowMapOfBucket) / mapShrinkFraction)
		if tableLen > m.minTableLen && table.sumSize() <= shrinkThreshold {
			// Shrink the table with factor of 2.
			atomic.AddInt64(&m.totalShrinks, 1)
			newTable = newCowMapOfTable[K, V](tableLen>>1, table.isoId)
		} else {
			// No need to shrink. Wake up all waiters and give up.
			m.resizeMu.Lock()
			atomic.StoreInt64(&m.resizing, 0)
			m.resizeCond.Broadcast()
			m.resizeMu.Unlock()
			return false
		}
	case mapClearHint:
		newTable = newCowMapOfTable[K, V](m.minTableLen, table.isoId)
	case mapCopyHint:
		// Shallow copy table except copying bucket mutexes.
		atomic.AddInt64(&m.totalCopies, 1)
		newTable = &cowMapOfTable[K, V]{
			buckets: make([]cowBucketOfPadded, len(table.buckets)),
			size:    make([]counterStripe, len(table.size)),
			seed:    table.seed,
			isoId:   table.isoId,
		}
		for bIdx := range newTable.buckets {
			table.buckets[bIdx].mu.Lock()
			newTable.buckets[bIdx].isoId = table.buckets[bIdx].isoId
			newTable.buckets[bIdx].meta = table.buckets[bIdx].meta
			newTable.buckets[bIdx].entries = table.buckets[bIdx].entries
			newTable.buckets[bIdx].next = table.buckets[bIdx].next
			cidx := uint64((len(newTable.size) - 1) & bIdx)
			newTable.size[cidx].c += cowBucketOfSize(&newTable.buckets[bIdx])
			table.buckets[bIdx].mu.Unlock()
		}
	default:
		panic(fmt.Sprintf("unexpected resize hint: %d", hint))
	}
	// Copy the data only if we're growing or shrinking the map.
	if hint == mapGrowHint || hint == mapShrinkHint {
		for i := 0; i < tableLen; i++ {
			copied := copyCowBucketOf(&table.buckets[i], newTable, m.hasher)
			newTable.addSizePlain(uint64(i), copied)
		}
	}
	// Publish the new table and wake up all waiters.
	atomic.StorePointer(&m.table, unsafe.Pointer(newTable))
	m.resizeMu.Lock()
	atomic.StoreInt64(&m.resizing, 0)
	m.resizeCond.Broadcast()
	m.resizeMu.Unlock()
	return true
}

func copyCowBucketOf[K comparable, V any](
	b *cowBucketOfPadded,
	destTable *cowMapOfTable[K, V],
	hasher func(K, uint64) uint64,
) (copied int) {
	rootb := b
	rootb.mu.Lock()
	for {
		for i := 0; i < entriesPerCowMapOfBucket; i++ {
			if b.entries[i] != nil {
				e := (*entryOf[K, V])(b.entries[i])
				hash := hasher(e.key, destTable.seed)
				bidx := uint64(len(destTable.buckets)-1) & h1(hash)
				destb := &destTable.buckets[bidx]
				appendToCowBucketOf(h2(hash), b.entries[i], destb)
				copied++
			}
		}
		if b.next == nil {
			rootb.mu.Unlock()
			return
		}
		b = (*cowBucketOfPadded)(b.next)
	}
}

func cowBucketOfSize(b *cowBucketOfPadded) (size int64) {
	for {
		for i := 0; i < entriesPerCowMapOfBucket; i++ {
			if b.entries[i] != nil {
				size++
			}
		}
		if b.next == nil {
			return
		}
		b = (*cowBucketOfPadded)(b.next)
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
// It is safe to modify the map while iterating it, including entry
// creation, modification and deletion. However, the concurrent
// modification rule apply, i.e. the changes may be not reflected
// in the subsequently iterated entries.
//
// Use Range on copy provided by Copy method to prevent concurrent
// modifications affecting iteration.
func (m *CowMapOf[K, V]) Range(f func(key K, value V) bool) {
	var zeroPtr unsafe.Pointer
	// Pre-allocate array big enough to fit entries for most hash tables.
	bentries := make([]unsafe.Pointer, 0, 16*entriesPerCowMapOfBucket)
	tablep := atomic.LoadPointer(&m.table)
	table := *(*cowMapOfTable[K, V])(tablep)
	for i := range table.buckets {
		rootb := &table.buckets[i]
		b := rootb
		// Prevent concurrent modifications and copy all entries into
		// the intermediate slice.
		rootb.mu.Lock()
		for {
			for i := 0; i < entriesPerCowMapOfBucket; i++ {
				if b.entries[i] != nil {
					bentries = append(bentries, b.entries[i])
				}
			}
			if b.next == nil {
				rootb.mu.Unlock()
				break
			}
			b = (*cowBucketOfPadded)(b.next)
		}
		// Call the function for all copied entries.
		for j := range bentries {
			entry := (*entryOf[K, V])(bentries[j])
			if !f(entry.key, entry.value) {
				return
			}
			// Remove the reference to avoid preventing the copied
			// entries from being GCed until this method finishes.
			bentries[j] = zeroPtr
		}
		bentries = bentries[:0]
	}
}

// Clear deletes all keys and values currently stored in the map.
func (m *CowMapOf[K, V]) Clear() {
	table := (*cowMapOfTable[K, V])(atomic.LoadPointer(&m.table))
	m.resize(table, mapClearHint)
}

// Size returns current size of the map.
func (m *CowMapOf[K, V]) Size() int {
	table := (*cowMapOfTable[K, V])(atomic.LoadPointer(&m.table))
	return int(table.sumSize())
}

func appendToCowBucketOf(h2 uint8, entryPtr unsafe.Pointer, b *cowBucketOfPadded) {
	for {
		for i := 0; i < entriesPerCowMapOfBucket; i++ {
			if b.entries[i] == nil {
				b.meta = setByte(b.meta, h2, i)
				b.entries[i] = entryPtr
				return
			}
		}
		if b.next == nil {
			newb := new(cowBucketOfPadded)
			newb.meta = setByte(defaultMeta, h2, 0)
			newb.entries[0] = entryPtr
			b.next = unsafe.Pointer(newb)
			return
		}
		b = (*cowBucketOfPadded)(b.next)
	}
}

func (table *cowMapOfTable[K, V]) addSize(bucketIdx uint64, delta int) {
	cidx := uint64(len(table.size)-1) & bucketIdx
	atomic.AddInt64(&table.size[cidx].c, int64(delta))
}

func (table *cowMapOfTable[K, V]) addSizePlain(bucketIdx uint64, delta int) {
	cidx := uint64(len(table.size)-1) & bucketIdx
	table.size[cidx].c += int64(delta)
}

func (table *cowMapOfTable[K, V]) sumSize() int64 {
	sum := int64(0)
	for i := range table.size {
		sum += atomic.LoadInt64(&table.size[i].c)
	}
	return sum
}

// Stats returns statistics for the CowMapOf. Just like other map
// methods, this one is thread-safe. Yet it's an O(N) operation,
// so it should be used only for diagnostics or debugging purposes.
func (m *CowMapOf[K, V]) Stats() MapStats {
	stats := MapStats{
		TotalGrowths: atomic.LoadInt64(&m.totalGrowths),
		TotalShrinks: atomic.LoadInt64(&m.totalShrinks),
		TotalCopies:  atomic.LoadInt64(&m.totalCopies),
		MinEntries:   math.MaxInt32,
	}
	table := (*cowMapOfTable[K, V])(atomic.LoadPointer(&m.table))
	stats.RootBuckets = len(table.buckets)
	stats.Counter = int(table.sumSize())
	stats.CounterLen = len(table.size)
	for i := range table.buckets {
		nentries := 0
		b := &table.buckets[i]
		stats.TotalBuckets++
		for {
			nentriesLocal := 0
			stats.Capacity += entriesPerCowMapOfBucket
			for i := 0; i < entriesPerCowMapOfBucket; i++ {
				if atomic.LoadPointer(&b.entries[i]) != nil {
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
			b = (*cowBucketOfPadded)(atomic.LoadPointer(&b.next))
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

var gIsoId uint32

func newIsoId() uint32 {
	return atomic.AddUint32(&gIsoId, 1)
}

// Copy creates shallow copy of map, no actual copying is happening there.
// Actual copying would be performed on first write to original map or copy.
// Original map modifications doesn't affect copied one and visa versa.
// It's safe to call Copy concurrently.
func (m *CowMapOf[K, V]) Copy() *CowMapOf[K, V] {
	// Mark map's table as copy-on-write under resizing condition
	for !atomic.CompareAndSwapInt64(&m.resizing, 0, 1) {
		m.waitForResize()
	}
	t := (*cowMapOfTable[K, V])(atomic.LoadPointer(&m.table))
	t1, t2 := *t, *t
	t1.isoId = newIsoId()
	t1.copyOnWrite = true
	atomic.StorePointer(&m.table, unsafe.Pointer(&t1))
	m.resizeMu.Lock()
	atomic.StoreInt64(&m.resizing, 0)
	m.resizeCond.Broadcast()
	m.resizeMu.Unlock()
	// Create another map with same table marked as copy-on-write
	t2.isoId = newIsoId()
	t2.copyOnWrite = true
	m2 := CowMapOf[K, V]{
		table:       unsafe.Pointer(&t2),
		hasher:      m.hasher,
		minTableLen: m.minTableLen,
		growOnly:    m.growOnly,
	}
	m2.resizeCond = sync.Cond{L: &m2.resizeMu}
	return &m2
}

// updateCowBucketOfPaddedRefs updates bucket references to make sure
// it doesn't refer other table's data (entries or next).
func updateCowBucketOfPaddedRefs(bExt *cowBucketOfPadded, isoId uint32) {
	bInt := bExt
	for {
		bInt.isoId = isoId
		bInt.meta = bExt.meta
		bExtEntries := bExt.entries
		bInt.entries = [entriesPerCowMapOfBucket]unsafe.Pointer{}
		// Shallow copy entries; deep copy is unnecessary
		// since every write operation replaces addresses anyway.
		copy(bInt.entries[:], bExtEntries[:])
		if bExt.next == nil {
			return
		}
		bExt = (*cowBucketOfPadded)(bExt.next)
		bIntNext := new(cowBucketOfPadded)
		bInt.next = unsafe.Pointer(bIntNext)
		bInt = bIntNext
	}
}
