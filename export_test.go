package xsync

import "sync/atomic"

const (
	EntriesPerMapBucket           = entriesPerMapBucket
	MapLoadFactor                 = mapLoadFactor
	DefaultMinMapTableLen         = defaultMinMapTableLen
	DefaultMinMapTableCap         = defaultMinMapTableLen * entriesPerMapBucket
	MaxMapCounterLen              = maxMapCounterLen
	MpmcQueueMaxRequestedCapacity = mpmcQueueMaxRequestedCapacity
)

type (
	BucketPadded = bucketPadded
)

func EnableAssertions() {
	assertionsEnabled = true
}

func DisableAssertions() {
	assertionsEnabled = false
}

func Cheaprand() uint32 {
	return runtime_cheaprand()
}

func Broadcast(b uint8) uint64 {
	return broadcast(b)
}

func FirstMarkedByteIndex(w uint64) int {
	return firstMarkedByteIndex(w)
}

func MarkZeroBytes(w uint64) uint64 {
	return markZeroBytes(w)
}

func SetByte(w uint64, b uint8, idx int) uint64 {
	return setByte(w, b, idx)
}

func NextPowOf2(v uint64) uint64 {
	return nextPowOf2(v)
}

func HashUint64(seed, v uint64) uint64 {
	return hashUint64(seed, v)
}

func MapIntKey[K comparable, V any](m *Map[K, V]) bool {
	return m.intKey
}

// MapKeysInRootBucket returns first n non-negative int keys that hash
// into root bucket 0 of current table.
func MapKeysInRootBucket[V any](m *Map[int, V], n int) []int {
	table := m.table.Load()
	mask := uint64(len(table.buckets) - 1)
	keys := make([]int, 0, n)
	for k := 0; len(keys) < n; k++ {
		if h1(hashUint64(table.intSeed, toUint64(k)))&mask == 0 {
			keys = append(keys, k)
		}
	}
	return keys
}

// MapChainLen returns number of buckets in chain of root bucket 0.
func MapChainLen[K comparable, V any](m *Map[K, V]) int {
	l := 0
	for b := &m.table.Load().buckets[0]; b != nil; b = (*bucketPadded)(atomic.LoadPointer(&b.next)) {
		l++
	}
	return l
}

func LoadMapTable[K comparable, V any](m *Map[K, V]) any {
	return m.table.Load()
}

func GrowMap[K comparable, V any](m *Map[K, V], knownTable any) {
	m.resize(knownTable.(*mapTable[K, V]), mapGrowHint)
}
