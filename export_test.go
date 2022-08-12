package xsync

const (
	EntriesPerMapBucket = entriesPerMapBucket
	MinMapTableLen      = minMapTableLen
	MaxMapCounterLen    = maxMapCounterLen
)

type (
	Bucket = bucket
)

type MapStats struct {
	mapStats
}

func CollectMapStats(m *Map) MapStats {
	return MapStats{m.stats()}
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
