//go:build go1.18
// +build go1.18

package xsync

type (
	BucketOfPadded = bucketOfPadded
)

func CollectMapOfStats[K comparable, V any](m *MapOf[K, V]) MapStats {
	return MapStats{m.stats()}
}
