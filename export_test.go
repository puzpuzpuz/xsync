package xsync

const (
	EntriesPerMapBucket = entriesPerMapBucket
	MapLoadFactor       = mapLoadFactor
	MinMapTableLen      = minMapTableLen
	MinMapTableCap      = minMapTableCap
	MaxMapCounterLen    = maxMapCounterLen
)

type (
	BucketPadded = bucketPadded
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
	return fastrand()
}
