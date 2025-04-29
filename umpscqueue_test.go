package xsync

import (
	"fmt"
	"testing"
	"time"
)

func TestUMPSCQueue(t *testing.T) {
	for _, goroutines := range []int{1, 4, 16} {
		t.Run(fmt.Sprintf("goroutines=%d", goroutines), func(t *testing.T) {
			q := NewUMPSCQueue[int]()

			const count = 100 * segmentSize
			for mod := range goroutines {
				go func() {
					for i := range count {
						if i%goroutines == mod {
							q.Add(i)
						}
					}
				}()
			}

			values := make(map[int]struct{}, count)
			for range count {
				actual := q.Take()
				if _, ok := values[actual]; !ok {
					values[actual] = struct{}{}
				} else {
					t.Fatalf("got duplicate value: %q", actual)
				}
			}
			if len(values) != count {
				t.Fatalf("got %d values, expected %d", len(values), count)
			}
		})
	}
}

// This benchmarks the performance of the [UMPSCQueue] vs using a normal channel. As we can see from
// the results, channels get slower as the parallelism goes up due to contention on the lock which is
// acquired every time an element is added. By contrast, the queue actually gets faster. This is
// expected, as [testing.B.RunParallel] executes N operations with G goroutines, so it should take
// less time overall. The overall memory cost is negligible, especially since the allocation is not
// per-operation, it's per-segment, meaning it is amortized by the size of the segment. Additionally,
// segments are reused when possible, further decreasing the cost.
//
//	goos: linux
//	goarch: amd64
//	pkg: github.com/puzpuzpuz/xsync/v4
//	cpu: AMD EPYC 7V13 64-Core Processor
//	                        │    queue     │                  chan                   │
//	                        │    sec/op    │    sec/op      vs base                  │
//	ChanVsUMPSCQueue       15.91n ±  1%    39.00n ±  3%   +145.02% (p=0.000 n=10)
//	ChanVsUMPSCQueue-8     92.45n ± 13%   101.17n ±  4%          ~ (p=0.052 n=10)
//	ChanVsUMPSCQueue-16    58.84n ±  3%   118.85n ±  5%   +102.01% (p=0.000 n=10)
//	ChanVsUMPSCQueue-64    32.63n ±  2%   191.35n ±  7%   +486.33% (p=0.000 n=10)
//	ChanVsUMPSCQueue-128   30.58n ±  2%   329.95n ±  6%   +979.15% (p=0.000 n=10)
//	ChanVsUMPSCQueue-256   29.52n ±  4%   702.15n ± 14%  +2278.15% (p=0.000 n=10)
//	ChanVsUMPSCQueue-512   28.49n ±  2%   826.25n ±  8%  +2800.65% (p=0.000 n=10)
//	geomean                   35.61n          208.6n         +485.69%
//
//	                        │     queue     │                   chan                   │
//	                        │     B/op      │    B/op      vs base                     │
//	ChanVsUMPSCQueue        0.000 ±  ?      0.000 ± 0%         ~ (p=0.474 n=10)
//	ChanVsUMPSCQueue-8      0.000 ± 0%      0.000 ± 0%         ~ (p=1.000 n=10) ¹
//	ChanVsUMPSCQueue-16     0.000 ± 0%      0.000 ± 0%         ~ (p=1.000 n=10) ¹
//	ChanVsUMPSCQueue-64     0.000 ± 0%      0.000 ± 0%         ~ (p=1.000 n=10) ¹
//	ChanVsUMPSCQueue-128    0.000 ± 0%      0.000 ± 0%         ~ (p=1.000 n=10) ¹
//	ChanVsUMPSCQueue-256    0.000 ± 0%      0.000 ± 0%         ~ (p=1.000 n=10)
//	ChanVsUMPSCQueue-512   0.5000 ±  ?     0.0000 ± 0%  -100.00% (p=0.033 n=10)
//	geomean                               ²                ?                       ² ³
//	¹ all samples are equal
//	² summaries must be >0 to compute geomean
//	³ ratios must be >0 to compute geomean
//
//	                        │    queue     │                chan                 │
//	                        │  allocs/op   │ allocs/op   vs base                 │
//	ChanVsUMPSCQueue       0.000 ± 0%     0.000 ± 0%       ~ (p=1.000 n=10) ¹
//	ChanVsUMPSCQueue-8     0.000 ± 0%     0.000 ± 0%       ~ (p=1.000 n=10) ¹
//	ChanVsUMPSCQueue-16    0.000 ± 0%     0.000 ± 0%       ~ (p=1.000 n=10) ¹
//	ChanVsUMPSCQueue-64    0.000 ± 0%     0.000 ± 0%       ~ (p=1.000 n=10) ¹
//	ChanVsUMPSCQueue-128   0.000 ± 0%     0.000 ± 0%       ~ (p=1.000 n=10) ¹
//	ChanVsUMPSCQueue-256   0.000 ± 0%     0.000 ± 0%       ~ (p=1.000 n=10) ¹
//	ChanVsUMPSCQueue-512   0.000 ± 0%     0.000 ± 0%       ~ (p=1.000 n=10) ¹
//	geomean                              ²               +0.00%                ²
//	¹ all samples are equal
//	² summaries must be >0 to compute geomean
func BenchmarkChanVsUMPSCQueue(b *testing.B) {
	b.Run("method=queue", func(b *testing.B) {
		q := NewUMPSCQueue[int]()
		done := make(chan struct{})
		go func() {
			defer close(done)
			for range b.N {
				q.Take()
			}
		}()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				q.Add(0)
			}
		})
		<-done
	})

	b.Run("method=chan", func(b *testing.B) {
		ch := make(chan time.Duration, segmentSize)
		done := make(chan struct{})
		go func() {
			defer close(done)
			var received int
			for range ch {
				received++
				if received == b.N {
					break
				}
			}
		}()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				ch <- 0
			}
		})
		<-done
	})
}
