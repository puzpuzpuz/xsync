package xsync

const (
	EntriesPerMapOfBucket   = entriesPerMapOfBucket
	MapLoadFactor           = mapLoadFactor
	DefaultMinMapTableLen   = defaultMinMapTableLen
	DefaultMinMapOfTableCap = defaultMinMapTableLen * entriesPerMapOfBucket
	MaxMapCounterLen        = maxMapCounterLen
)

type (
	BucketOfPadded = bucketOfPadded
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
