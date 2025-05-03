package xsync

import (
	"fmt"
	"hash/maphash"
	"math"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"unsafe"
)

const (
	// number of Map entries per bucket; 5 entries lead to size of 64B
	// (one cache line) on 64-bit machines
	entriesPerMapBucket = 5
	// threshold fraction of table occupation to start a table shrinking
	// when deleting the last entry in a bucket chain
	mapShrinkFraction = 128
	// map load factor to trigger a table resize during insertion;
	// a map holds up to mapLoadFactor*entriesPerMapBucket*mapTableLen
	// key-value pairs (this is a soft limit)
	mapLoadFactor = 0.75
	// minimal table size, i.e. number of buckets; thus, minimal map
	// capacity can be calculated as entriesPerMapBucket*defaultMinMapTableLen
	defaultMinMapTableLen = 32
	// minimum counter stripes to use
	minMapCounterLen = 8
	// maximum counter stripes to use; stands for around 4KB of memory
	maxMapCounterLen         = 32
	defaultMeta       uint64 = 0x8080808080808080
	metaMask          uint64 = 0xffffffffff
	defaultMetaMasked uint64 = defaultMeta & metaMask
	emptyMetaSlot     uint8  = 0x80
	// minimum buckets per goroutine during parallel resize
	minBucketsPerGoroutine = 64
)

type mapResizeHint int

const (
	mapGrowHint   mapResizeHint = 0
	mapShrinkHint mapResizeHint = 1
	mapClearHint  mapResizeHint = 2
)

type ComputeOp int

const (
	// CancelOp signals to Compute to not do anything as a result
	// of executing the lambda. If the entry was not present in
	// the map, nothing happens, and if it was present, the
	// returned value is ignored.
	CancelOp ComputeOp = iota
	// UpdateOp signals to Compute to update the entry to the
	// value returned by the lambda, creating it if necessary.
	UpdateOp
	// DeleteOp signals to Compute to always delete the entry
	// from the map.
	DeleteOp
)

type loadOp int

const (
	noLoadOp loadOp = iota
	loadOrComputeOp
	loadAndDeleteOp
)

// Deprecated: use [Map]
type MapOf[K comparable, V any] = Map[K, V]

// Map is like a Go map[K]V but is safe for concurrent
// use by multiple goroutines without additional locking or
// coordination. It follows the interface of sync.Map with
// a number of valuable extensions like Compute or Size.
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
// Map also borrows ideas from Java's j.u.c.ConcurrentHashMap
// (immutable K/V pair structs instead of atomic snapshots)
// and C++'s absl::flat_hash_map (meta memory and SWAR-based
// lookups).
type Map[K comparable, V any] struct {
	totalGrowths atomic.Int64
	totalShrinks atomic.Int64
	resizing     atomic.Bool // resize in progress flag
	resizeMu     sync.Mutex  // only used along with resizeCond
	resizeCond   sync.Cond   // used to wake up resize waiters (concurrent modifications)
	table        atomic.Pointer[mapTable[K, V]]
	minTableLen  int
	growOnly     bool
	serialResize bool
}

type mapTable[K comparable, V any] struct {
	buckets []bucketPadded[K, V]
	// striped counter for number of table entries;
	// used to determine if a table shrinking is needed
	// occupies min(buckets_memory/1024, 64KB) of memory
	size []counterStripe
	seed maphash.Seed
}

type counterStripe struct {
	c int64
	// Padding to prevent false sharing.
	_ [cacheLineSize - 8]byte
}

// bucketPadded is a CL-sized map bucket holding up to
// entriesPerMapBucket entries.
type bucketPadded[K comparable, V any] struct {
	//lint:ignore U1000 ensure each bucket takes two cache lines on both 32 and 64-bit archs
	pad [cacheLineSize - unsafe.Sizeof(bucket[K, V]{})]byte
	bucket[K, V]
}

type bucket[K comparable, V any] struct {
	meta    atomic.Uint64
	entries [entriesPerMapBucket]atomic.Pointer[entry[K, V]] // *entry
	next    atomic.Pointer[bucketPadded[K, V]]               // *bucketPadded
	mu      sync.Mutex
}

// entry is an immutable map entry.
type entry[K comparable, V any] struct {
	key   K
	value V
}

// MapConfig defines configurable Map options.
type MapConfig struct {
	sizeHint     int
	growOnly     bool
	serialResize bool
}

// WithPresize configures new Map instance with capacity enough
// to hold sizeHint entries. The capacity is treated as the minimal
// capacity meaning that the underlying hash table will never shrink
// to a smaller capacity. If sizeHint is zero or negative, the value
// is ignored.
func WithPresize(sizeHint int) func(*MapConfig) {
	return func(c *MapConfig) {
		c.sizeHint = sizeHint
	}
}

// WithGrowOnly configures new Map instance to be grow-only.
// This means that the underlying hash table grows in capacity when
// new keys are added, but does not shrink when keys are deleted.
// The only exception to this rule is the Clear method which
// shrinks the hash table back to the initial capacity.
func WithGrowOnly() func(*MapConfig) {
	return func(c *MapConfig) {
		c.growOnly = true
	}
}

// WithSerialResize enables serial resizing mode, matching the behavior of
// older versions. With this setting, Map will no longer spawn additional
// goroutines when resizing. Use in resource-constrained environments, while
// parallel resizing (default) provides higher throughput.
func WithSerialResize() func(*MapConfig) {
	return func(c *MapConfig) {
		c.serialResize = true
	}
}

// Deprecated: use [NewMap].
func NewMapOf[K comparable, V any](options ...func(*MapConfig)) *Map[K, V] {
	return NewMap[K, V](options...)
}

// NewMap creates a new Map instance configured with the given
// options.
func NewMap[K comparable, V any](options ...func(*MapConfig)) *Map[K, V] {
	c := &MapConfig{
		sizeHint: defaultMinMapTableLen * entriesPerMapBucket,
	}
	for _, o := range options {
		o(c)
	}

	m := &Map[K, V]{}
	m.resizeCond = *sync.NewCond(&m.resizeMu)
	var table *mapTable[K, V]
	if c.sizeHint <= defaultMinMapTableLen*entriesPerMapBucket {
		table = newMapTable[K, V](defaultMinMapTableLen)
	} else {
		tableLen := nextPowOf2(uint32((float64(c.sizeHint) / entriesPerMapBucket) / mapLoadFactor))
		table = newMapTable[K, V](int(tableLen))
	}
	m.minTableLen = len(table.buckets)
	m.growOnly = c.growOnly
	m.serialResize = c.serialResize
	m.table.Store(table)
	return m
}

func newMapTable[K comparable, V any](minTableLen int) *mapTable[K, V] {
	buckets := make([]bucketPadded[K, V], minTableLen)
	for i := range buckets {
		buckets[i].meta.Store(defaultMeta)
	}
	counterLen := minTableLen >> 10
	if counterLen < minMapCounterLen {
		counterLen = minMapCounterLen
	} else if counterLen > maxMapCounterLen {
		counterLen = maxMapCounterLen
	}
	counter := make([]counterStripe, counterLen)
	t := &mapTable[K, V]{
		buckets: buckets,
		size:    counter,
		seed:    maphash.MakeSeed(),
	}
	return t
}

// ToPlainMap returns a native map with a copy of xsync Map's
// contents. The copied xsync Map should not be modified while
// this call is made. If the copied Map is modified, the copying
// behavior is the same as in the Range method.
func ToPlainMap[K comparable, V any](m *Map[K, V]) map[K]V {
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
func (m *Map[K, V]) Load(key K) (value V, ok bool) {
	table := m.table.Load()
	hash := maphash.Comparable(table.seed, key)
	h1 := h1(hash)
	h2w := broadcast(h2(hash))
	bidx := uint64(len(table.buckets)-1) & h1
	b := &table.buckets[bidx]
	for {
		metaw := b.meta.Load()
		markedw := markZeroBytes(metaw^h2w) & metaMask
		for markedw != 0 {
			idx := firstMarkedByteIndex(markedw)
			e := b.entries[idx].Load()
			if e != nil {
				if e.key == key {
					return e.value, true
				}
			}
			markedw &= markedw - 1
		}
		b = b.next.Load()
		if b == nil {
			return
		}
	}
}

// Store sets the value for a key.
func (m *Map[K, V]) Store(key K, value V) {
	m.doCompute(
		key,
		func(V, bool) (V, ComputeOp) {
			return value, UpdateOp
		},
		noLoadOp,
		false,
	)
}

// LoadOrStore returns the existing value for the key if present.
// Otherwise, it stores and returns the given value.
// The loaded result is true if the value was loaded, false if stored.
func (m *Map[K, V]) LoadOrStore(key K, value V) (actual V, loaded bool) {
	return m.doCompute(
		key,
		func(oldValue V, loaded bool) (V, ComputeOp) {
			if loaded {
				return oldValue, CancelOp
			}
			return value, UpdateOp
		},
		loadOrComputeOp,
		false,
	)
}

// LoadAndStore returns the existing value for the key if present,
// while setting the new value for the key.
// It stores the new value and returns the existing one, if present.
// The loaded result is true if the existing value was loaded,
// false otherwise.
func (m *Map[K, V]) LoadAndStore(key K, value V) (actual V, loaded bool) {
	return m.doCompute(
		key,
		func(V, bool) (V, ComputeOp) {
			return value, UpdateOp
		},
		noLoadOp,
		false,
	)
}

// LoadOrCompute returns the existing value for the key if
// present. Otherwise, it tries to compute the value using the
// provided function and, if successful, stores and returns
// the computed value. The loaded result is true if the value was
// loaded, or false if computed. If valueFn returns true as the
// cancel value, the computation is cancelled and the zero value
// for type V is returned.
//
// This call locks a hash table bucket while the compute function
// is executed. It means that modifications on other entries in
// the bucket will be blocked until the valueFn executes. Consider
// this when the function includes long-running operations.
func (m *Map[K, V]) LoadOrCompute(
	key K,
	valueFn func() (newValue V, cancel bool),
) (value V, loaded bool) {
	return m.doCompute(
		key,
		func(oldValue V, loaded bool) (V, ComputeOp) {
			if loaded {
				return oldValue, CancelOp
			}
			newValue, c := valueFn()
			if !c {
				return newValue, UpdateOp
			}
			return oldValue, CancelOp
		},
		loadOrComputeOp,
		false,
	)
}

// Compute either sets the computed new value for the key,
// deletes the value for the key, or does nothing, based on
// the returned [ComputeOp]. When the op returned by valueFn
// is [UpdateOp], the value is updated to the new value. If
// it is [DeleteOp], the entry is removed from the map
// altogether. And finally, if the op is [CancelOp] then the
// entry is left as-is. In other words, if it did not already
// exist, it is not created, and if it did exist, it is not
// updated. This is useful to synchronously execute some
// operation on the value without incurring the cost of
// updating the map every time. The ok result indicates
// whether the entry is present in the map after the compute
// operation. The actual result contains the value of the map
// if a corresponding entry is present, or the zero value
// otherwise. See the example for a few use cases.
//
// This call locks a hash table bucket while the compute function
// is executed. It means that modifications on other entries in
// the bucket will be blocked until the valueFn executes. Consider
// this when the function includes long-running operations.
func (m *Map[K, V]) Compute(
	key K,
	valueFn func(oldValue V, loaded bool) (newValue V, op ComputeOp),
) (actual V, ok bool) {
	return m.doCompute(key, valueFn, noLoadOp, true)
}

// LoadAndDelete deletes the value for a key, returning the previous
// value if any. The loaded result reports whether the key was
// present.
func (m *Map[K, V]) LoadAndDelete(key K) (value V, loaded bool) {
	return m.doCompute(
		key,
		func(value V, loaded bool) (V, ComputeOp) {
			return value, DeleteOp
		},
		loadAndDeleteOp,
		false,
	)
}

// Delete deletes the value for a key.
func (m *Map[K, V]) Delete(key K) {
	m.LoadAndDelete(key)
}

func (m *Map[K, V]) doCompute(
	key K,
	valueFn func(oldValue V, loaded bool) (V, ComputeOp),
	loadOp loadOp,
	computeOnly bool,
) (V, bool) {
	for {
	compute_attempt:
		var (
			emptyb   *bucketPadded[K, V]
			emptyidx int
		)
		table := m.table.Load()
		tableLen := len(table.buckets)
		hash := maphash.Comparable(table.seed, key)
		h1 := h1(hash)
		h2 := h2(hash)
		h2w := broadcast(h2)
		bidx := uint64(len(table.buckets)-1) & h1
		rootb := &table.buckets[bidx]

		if loadOp != noLoadOp {
			b := rootb
		load:
			for {
				metaw := b.meta.Load()
				markedw := markZeroBytes(metaw^h2w) & metaMask
				for markedw != 0 {
					idx := firstMarkedByteIndex(markedw)
					e := b.entries[idx].Load()
					if e != nil {
						if e.key == key {
							if loadOp == loadOrComputeOp {
								return e.value, true
							}
							break load
						}
					}
					markedw &= markedw - 1
				}
				b = b.next.Load()
				if b == nil {
					if loadOp == loadAndDeleteOp {
						return *new(V), false
					}
					break load
				}
			}
		}

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
		b := rootb
		for {
			metaw := b.meta.Load()
			markedw := markZeroBytes(metaw^h2w) & metaMask
			for markedw != 0 {
				idx := firstMarkedByteIndex(markedw)
				e := b.entries[idx].Load()
				if e != nil {
					if e.key == key {
						// In-place update/delete.
						// We get a copy of the value via an interface{} on each call,
						// thus the live value pointers are unique. Otherwise atomic
						// snapshot won't be correct in case of multiple Store calls
						// using the same value.
						oldv := e.value
						newv, op := valueFn(oldv, true)
						switch op {
						case DeleteOp:
							// Deletion.
							// First we update the hash, then the entry.
							newmetaw := setByte(metaw, emptyMetaSlot, idx)
							b.meta.Store(newmetaw)
							b.entries[idx].Store(nil)
							rootb.mu.Unlock()
							table.addSize(bidx, -1)
							// Might need to shrink the table if we left bucket empty.
							if newmetaw == defaultMeta {
								m.resize(table, mapShrinkHint)
							}
							return oldv, !computeOnly
						case UpdateOp:
							newe := new(entry[K, V])
							newe.key = key
							newe.value = newv
							b.entries[idx].Store(newe)
						case CancelOp:
							newv = oldv
						}
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
				// Search for empty entries (up to 5 per bucket).
				emptyw := metaw & defaultMetaMasked
				if emptyw != 0 {
					idx := firstMarkedByteIndex(emptyw)
					emptyb = b
					emptyidx = idx
				}
			}
			if b.next.Load() == nil {
				if emptyb != nil {
					// Insertion into an existing bucket.
					var zeroV V
					newValue, op := valueFn(zeroV, false)
					switch op {
					case DeleteOp, CancelOp:
						rootb.mu.Unlock()
						return zeroV, false
					default:
						newe := new(entry[K, V])
						newe.key = key
						newe.value = newValue
						// First we update meta, then the entry.
						emptyb.meta.Store(setByte(emptyb.meta.Load(), h2, emptyidx))
						emptyb.entries[emptyidx].Store(newe)
						rootb.mu.Unlock()
						table.addSize(bidx, 1)
						return newValue, computeOnly
					}
				}
				growThreshold := float64(tableLen) * entriesPerMapBucket * mapLoadFactor
				if table.sumSize() > int64(growThreshold) {
					// Need to grow the table. Then go for another attempt.
					rootb.mu.Unlock()
					m.resize(table, mapGrowHint)
					goto compute_attempt
				}
				// Insertion into a new bucket.
				var zeroV V
				newValue, op := valueFn(zeroV, false)
				switch op {
				case DeleteOp, CancelOp:
					rootb.mu.Unlock()
					return newValue, false
				default:
					// Create and append a bucket.
					newb := new(bucketPadded[K, V])
					newb.meta.Store(setByte(defaultMeta, h2, 0))
					newe := new(entry[K, V])
					newe.key = key
					newe.value = newValue
					newb.entries[0].Store(newe)
					b.next.Store(newb)
					rootb.mu.Unlock()
					table.addSize(bidx, 1)
					return newValue, computeOnly
				}
			}
			b = b.next.Load()
		}
	}
}

func (m *Map[K, V]) newerTableExists(table *mapTable[K, V]) bool {
	return table != m.table.Load()
}

func (m *Map[K, V]) resizeInProgress() bool {
	return m.resizing.Load()
}

func (m *Map[K, V]) waitForResize() {
	m.resizeMu.Lock()
	for m.resizeInProgress() {
		m.resizeCond.Wait()
	}
	m.resizeMu.Unlock()
}

func (m *Map[K, V]) resize(knownTable *mapTable[K, V], hint mapResizeHint) {
	knownTableLen := len(knownTable.buckets)
	// Fast path for shrink attempts.
	if hint == mapShrinkHint {
		if m.growOnly ||
			m.minTableLen == knownTableLen ||
			knownTable.sumSize() > int64((knownTableLen*entriesPerMapBucket)/mapShrinkFraction) {
			return
		}
	}
	// Slow path.
	if !m.resizing.CompareAndSwap(false, true) {
		// Someone else started resize. Wait for it to finish.
		m.waitForResize()
		return
	}
	var newTable *mapTable[K, V]
	table := m.table.Load()
	tableLen := len(table.buckets)
	switch hint {
	case mapGrowHint:
		// Grow the table with factor of 2.
		m.totalGrowths.Add(1)
		newTable = newMapTable[K, V](tableLen << 1)
	case mapShrinkHint:
		shrinkThreshold := int64((tableLen * entriesPerMapBucket) / mapShrinkFraction)
		if tableLen > m.minTableLen && table.sumSize() <= shrinkThreshold {
			// Shrink the table with factor of 2.
			m.totalShrinks.Add(1)
			newTable = newMapTable[K, V](tableLen >> 1)
		} else {
			// No need to shrink. Wake up all waiters and give up.
			m.resizeMu.Lock()
			m.resizing.Store(false)
			m.resizeCond.Broadcast()
			m.resizeMu.Unlock()
			return
		}
	case mapClearHint:
		newTable = newMapTable[K, V](m.minTableLen)
	default:
		panic(fmt.Sprintf("unexpected resize hint: %d", hint))
	}
	// Copy the data only if we're not clearing the map.
	if hint != mapClearHint {
		// Enable parallel resizing when serialResize is false and table is large enough.
		// Calculate optimal goroutine count based on table size and available CPUs
		chunks := 1
		if !m.serialResize && tableLen >= minBucketsPerGoroutine*2 {
			chunks = min(tableLen/minBucketsPerGoroutine, runtime.GOMAXPROCS(0))
			chunks = max(chunks, 1)
		}
		if chunks > 1 {
			var copyWg sync.WaitGroup
			chunkSize := (tableLen + chunks - 1) / chunks
			for c := 0; c < chunks; c++ {
				copyWg.Add(1)
				go func(start, end int) {
					for i := start; i < end; i++ {
						copied := copyBucketWithDestLock[K, V](&table.buckets[i], newTable)
						if copied > 0 {
							newTable.addSize(uint64(i), copied)
						}
					}
					copyWg.Done()
				}(c*chunkSize, min((c+1)*chunkSize, tableLen))
			}
			copyWg.Wait()
		} else {
			for i := 0; i < tableLen; i++ {
				copied := copyBucket(&table.buckets[i], newTable)
				newTable.addSizePlain(uint64(i), copied)
			}
		}
	}
	// Publish the new table and wake up all waiters.
	m.table.Store(newTable)
	m.resizeMu.Lock()
	m.resizing.Store(false)
	m.resizeCond.Broadcast()
	m.resizeMu.Unlock()
}

func copyBucketWithDestLock[K comparable, V any](
	b *bucketPadded[K, V],
	destTable *mapTable[K, V],
) (copied int) {
	rootb := b
	rootb.mu.Lock()
	for {
		for i := 0; i < entriesPerMapBucket; i++ {
			if e := b.entries[i].Load(); e != nil {
				hash := maphash.Comparable(destTable.seed, e.key)
				bidx := uint64(len(destTable.buckets)-1) & h1(hash)
				destb := &destTable.buckets[bidx]
				destb.mu.Lock()
				appendToBucket(h2(hash), b.entries[i].Load(), destb)
				destb.mu.Unlock()
				copied++
			}
		}
		if next := b.next.Load(); next == nil {
			rootb.mu.Unlock()
			return
		} else {
			b = next
		}
	}
}

func copyBucket[K comparable, V any](
	b *bucketPadded[K, V],
	destTable *mapTable[K, V],
) (copied int) {
	rootb := b
	rootb.mu.Lock()
	for {
		for i := 0; i < entriesPerMapBucket; i++ {
			if e := b.entries[i].Load(); e != nil {
				hash := maphash.Comparable(destTable.seed, e.key)
				bidx := uint64(len(destTable.buckets)-1) & h1(hash)
				destb := &destTable.buckets[bidx]
				appendToBucket(h2(hash), b.entries[i].Load(), destb)
				copied++
			}
		}
		if next := b.next.Load(); next == nil {
			rootb.mu.Unlock()
			return
		} else {
			b = next
		}
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
func (m *Map[K, V]) Range(f func(key K, value V) bool) {
	// Pre-allocate array big enough to fit entries for most hash tables.
	bentries := make([]*entry[K, V], 0, 16*entriesPerMapBucket)
	table := m.table.Load()
	for i := range table.buckets {
		rootb := &table.buckets[i]
		b := rootb
		// Prevent concurrent modifications and copy all entries into
		// the intermediate slice.
		rootb.mu.Lock()
		for {
			for i := 0; i < entriesPerMapBucket; i++ {
				if entry := b.entries[i].Load(); entry != nil {
					bentries = append(bentries, entry)
				}
			}
			if next := b.next.Load(); next == nil {
				rootb.mu.Unlock()
				break
			} else {
				b = next
			}
		}
		// Call the function for all copied entries.
		for j, e := range bentries {
			if !f(e.key, e.value) {
				return
			}
			// Remove the reference to avoid preventing the copied
			// entries from being GCed until this method finishes.
			bentries[j] = nil
		}
		bentries = bentries[:0]
	}
}

// Clear deletes all keys and values currently stored in the map.
func (m *Map[K, V]) Clear() {
	m.resize(m.table.Load(), mapClearHint)
}

// Size returns current size of the map.
func (m *Map[K, V]) Size() int {
	return int(m.table.Load().sumSize())
}

func appendToBucket[K comparable, V any](h2 uint8, e *entry[K, V], b *bucketPadded[K, V]) {
	for {
		for i := 0; i < entriesPerMapBucket; i++ {
			if b.entries[i].Load() == nil {
				b.meta.Store(setByte(b.meta.Load(), h2, i))
				b.entries[i].Store(e)
				return
			}
		}
		if next := b.next.Load(); next == nil {
			newb := new(bucketPadded[K, V])
			newb.meta.Store(setByte(defaultMeta, h2, 0))
			newb.entries[0].Store(e)
			b.next.Store(newb)
			return
		} else {
			b = next
		}
	}
}

func (table *mapTable[K, V]) addSize(bucketIdx uint64, delta int) {
	cidx := uint64(len(table.size)-1) & bucketIdx
	atomic.AddInt64(&table.size[cidx].c, int64(delta))
}

func (table *mapTable[K, V]) addSizePlain(bucketIdx uint64, delta int) {
	cidx := uint64(len(table.size)-1) & bucketIdx
	table.size[cidx].c += int64(delta)
}

func (table *mapTable[K, V]) sumSize() int64 {
	sum := int64(0)
	for i := range table.size {
		sum += atomic.LoadInt64(&table.size[i].c)
	}
	return sum
}

func h1(h uint64) uint64 {
	return h >> 7
}

func h2(h uint64) uint8 {
	return uint8(h & 0x7f)
}

// MapStats is Map statistics.
//
// Warning: map statistics are intented to be used for diagnostic
// purposes, not for production code. This means that breaking changes
// may be introduced into this struct even between minor releases.
type MapStats struct {
	// RootBuckets is the number of root buckets in the hash table.
	// Each bucket holds a few entries.
	RootBuckets int
	// TotalBuckets is the total number of buckets in the hash table,
	// including root and their chained buckets. Each bucket holds
	// a few entries.
	TotalBuckets int
	// EmptyBuckets is the number of buckets that hold no entries.
	EmptyBuckets int
	// Capacity is the Map capacity, i.e. the total number of
	// entries that all buckets can physically hold. This number
	// does not consider the load factor.
	Capacity int
	// Size is the exact number of entries stored in the map.
	Size int
	// Counter is the number of entries stored in the map according
	// to the internal atomic counter. In case of concurrent map
	// modifications this number may be different from Size.
	Counter int
	// CounterLen is the number of internal atomic counter stripes.
	// This number may grow with the map capacity to improve
	// multithreaded scalability.
	CounterLen int
	// MinEntries is the minimum number of entries per a chain of
	// buckets, i.e. a root bucket and its chained buckets.
	MinEntries int
	// MinEntries is the maximum number of entries per a chain of
	// buckets, i.e. a root bucket and its chained buckets.
	MaxEntries int
	// TotalGrowths is the number of times the hash table grew.
	TotalGrowths int64
	// TotalGrowths is the number of times the hash table shrinked.
	TotalShrinks int64
}

// ToString returns string representation of map stats.
func (s *MapStats) ToString() string {
	var sb strings.Builder
	sb.WriteString("MapStats{\n")
	sb.WriteString(fmt.Sprintf("RootBuckets:  %d\n", s.RootBuckets))
	sb.WriteString(fmt.Sprintf("TotalBuckets: %d\n", s.TotalBuckets))
	sb.WriteString(fmt.Sprintf("EmptyBuckets: %d\n", s.EmptyBuckets))
	sb.WriteString(fmt.Sprintf("Capacity:     %d\n", s.Capacity))
	sb.WriteString(fmt.Sprintf("Size:         %d\n", s.Size))
	sb.WriteString(fmt.Sprintf("Counter:      %d\n", s.Counter))
	sb.WriteString(fmt.Sprintf("CounterLen:   %d\n", s.CounterLen))
	sb.WriteString(fmt.Sprintf("MinEntries:   %d\n", s.MinEntries))
	sb.WriteString(fmt.Sprintf("MaxEntries:   %d\n", s.MaxEntries))
	sb.WriteString(fmt.Sprintf("TotalGrowths: %d\n", s.TotalGrowths))
	sb.WriteString(fmt.Sprintf("TotalShrinks: %d\n", s.TotalShrinks))
	sb.WriteString("}\n")
	return sb.String()
}

// Stats returns statistics for the Map. Just like other map
// methods, this one is thread-safe. Yet it's an O(N) operation,
// so it should be used only for diagnostics or debugging purposes.
func (m *Map[K, V]) Stats() MapStats {
	stats := MapStats{
		TotalGrowths: m.totalGrowths.Load(),
		TotalShrinks: m.totalShrinks.Load(),
		MinEntries:   math.MaxInt32,
	}
	table := m.table.Load()
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
				if b.entries[i].Load() != nil {
					stats.Size++
					nentriesLocal++
				}
			}
			nentries += nentriesLocal
			if nentriesLocal == 0 {
				stats.EmptyBuckets++
			}
			if next := b.next.Load(); next == nil {
				break
			} else {
				b = next
			}
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
