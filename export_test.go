package xsync

const (
	EntriesPerMapBucket   = entriesPerMapBucket
	MapLoadFactor         = mapLoadFactor
	DefaultMinMapTableLen = defaultMinMapTableLen
	DefaultMinMapTableCap = defaultMinMapTableLen * entriesPerMapBucket
	MaxMapCounterLen      = maxMapCounterLen
)

type (
	BucketPadded[K comparable, V any] = bucketPadded[K, V]
)

func EnableAssertions() {
	assertionsEnabled = true
}

func DisableAssertions() {
	assertionsEnabled = false
}

func Fastrand() uint32 {
	return runtime_fastrand()
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

func NextPowOf2(v uint32) uint32 {
	return nextPowOf2(v)
}
