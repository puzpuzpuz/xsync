package xsync

const (
	EntriesPerMapBucket   = entriesPerMapBucket
	MapLoadFactor         = mapLoadFactor
	DefaultMinMapTableLen = defaultMinMapTableLen
	DefaultMinMapTableCap = defaultMinMapTableLen * entriesPerMapBucket
	MaxMapCounterLen      = maxMapCounterLen
)

type (
	BucketPadded   = bucketPadded
	BucketOfPadded = bucketOfPadded
)

type MapStats struct {
	mapStats
}

func CollectMapStats(m *Map) MapStats {
	return MapStats{m.stats()}
}

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

func NextPowOf2(v uint32) uint32 {
	return nextPowOf2(v)
}

func MakeSeed() uint64 {
	return makeSeed()
}

func HashString(s string, seed uint64) uint64 {
	return hashString(s, seed)
}

func MakeHasher[T comparable]() func(T, uint64) uint64 {
	return makeHasher[T]()
}

func CollectMapOfStats[K comparable, V any](m *MapOf[K, V]) MapStats {
	return MapStats{m.stats()}
}

func NewMapOfPresizedWithHasher[K comparable, V any](
	hasher func(K, uint64) uint64,
	sizeHint int,
) *MapOf[K, V] {
	return newMapOfPresized[K, V](hasher, sizeHint)
}
