//go:build go1.18
// +build go1.18

package xsync

func CollectMapOfStats[K, V any](m *MapOf[K, V]) MapStats {
	return MapStats{m.stats()}
}
