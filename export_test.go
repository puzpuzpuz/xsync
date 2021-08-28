package xsync

import "unsafe"

const (
	EntriesPerMapBucket = entriesPerMapBucket
	ResizeMapThreshold  = resizeMapThreshold
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

func MapSize(m *Map) int {
	return m.size()
}

func EntryHashMatch(keyPtr, valuePtr unsafe.Pointer, hash uint64) bool {
	return entryHashMatch(keyPtr, valuePtr, hash)
}

func TagKeyPtr(key *string, hash uint64) unsafe.Pointer {
	return tagKeyPtr(key, hash)
}

func TagValuePtr(value *interface{}, hash uint64) unsafe.Pointer {
	return tagValuePtr(value, hash)
}

func DerefKeyPtr(keyPtr unsafe.Pointer) string {
	return derefKeyPtr(keyPtr)
}

func DerefValuePtr(valuePtr unsafe.Pointer) interface{} {
	return derefValuePtr(valuePtr)
}
