//go:build go1.18
// +build go1.18

package xsync

func CollectMapOfStats[V any](m *MapOf[V]) MapStats {
	return MapStats{m.stats()}
}
