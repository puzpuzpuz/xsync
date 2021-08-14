package xsync

const (
	EntriesPerMapBucket = entriesPerMapBucket
	ResizeMapThreshold  = resizeMapThreshold
	MinMapTableLen      = minMapTableLen
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
