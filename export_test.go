package xsync

const (
	EntriesPerMapBucket     = entriesPerMapBucket
	EntriesPerMapOfBucket   = entriesPerMapOfBucket
	MapLoadFactor           = mapLoadFactor
	DefaultMinMapTableLen   = defaultMinMapTableLen
	DefaultMinMapTableCap   = defaultMinMapTableLen * entriesPerMapBucket
	DefaultMinMapOfTableCap = defaultMinMapTableLen * entriesPerMapOfBucket
	MaxMapCounterLen        = maxMapCounterLen
)

type (
	BucketPadded   = bucketPadded
	BucketOfPadded = bucketOfPadded
)

func LockBucket(mu *uint64) {
	lockBucket(mu)
}

func UnlockBucket(mu *uint64) {
	unlockBucket(mu)
}

func TopHashMatch(hash, topHashes uint64, idx int) bool {
	return topHashMatch(hash, topHashes, idx)
}

func StoreTopHash(hash, topHashes uint64, idx int) uint64 {
	return storeTopHash(hash, topHashes, idx)
}

func EraseTopHash(topHashes uint64, idx int) uint64 {
	return eraseTopHash(topHashes, idx)
}

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

func MakeSeed() uint64 {
	return makeSeed()
}

func HashString(s string, seed uint64) uint64 {
	return hashString(s, seed)
}

func DefaultHasher[T comparable]() func(T, uint64) uint64 {
	return defaultHasher[T]()
}
