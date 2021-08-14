package xsync

const (
	EntriesPerMapBucket = entriesPerMapBucket
	ResizeMapThreshold  = resizeMapThreshold
	MinMapTableLen      = minMapTableLen
)

type (
	Bucket   = bucket
	MapStats = mapStats
)

func CollectMapStats(m *Map) MapStats {
	return m.stats()
}

func MapSize(m *Map) int {
	return m.size()
}
