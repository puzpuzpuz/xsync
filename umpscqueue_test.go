package xsync

import (
	"fmt"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"
)

func TestUMPSCQueueEnqueueDequeueInt(t *testing.T) {
	q := NewUMPSCQueue[int]()
	for i := range 10000 {
		q.Enqueue(i)
	}
	for i := range 10000 {
		if got := q.Dequeue(); got != i {
			t.Fatalf("got %v, want %d", got, i)
		}
	}
}

func TestUMPSCQueueEnqueueDequeueString(t *testing.T) {
	q := NewUMPSCQueue[string]()
	for i := range 100 {
		q.Enqueue(strconv.Itoa(i))
	}
	for i := range 100 {
		if got := q.Dequeue(); got != strconv.Itoa(i) {
			t.Fatalf("got %v, want %d", got, i)
		}
	}
}

func TestUMPSCQueueEnqueueDequeueStruct(t *testing.T) {
	type foo struct {
		bar int
		baz int
	}
	q := NewUMPSCQueue[foo]()
	for i := range 100 {
		q.Enqueue(foo{i, i})
	}
	for i := range 100 {
		if got := q.Dequeue(); got.bar != i || got.baz != i {
			t.Fatalf("got %v, want %d", got, i)
		}
	}
}

func TestUMPSCQueueEnqueueDequeueStructRef(t *testing.T) {
	type foo struct {
		bar int
		baz int
	}
	q := NewUMPSCQueue[*foo]()
	for i := range 100 {
		q.Enqueue(&foo{i, i})
	}
	q.Enqueue(nil)
	for i := range 100 {
		if got := q.Dequeue(); got.bar != i || got.baz != i {
			t.Fatalf("got %v, want %d", got, i)
		}
	}
	if last := q.Dequeue(); last != nil {
		t.Fatalf("got %v, want nil", last)
	}
}

func TestUMPSCQueue(t *testing.T) {
	for _, goroutines := range []int{1, 4, 16} {
		t.Run(fmt.Sprintf("goroutines=%d", goroutines), func(t *testing.T) {
			q := NewUMPSCQueue[int]()

			const count = 100 * segmentSize
			for mod := range goroutines {
				go func() {
					for i := range count {
						if i%goroutines == mod {
							q.Enqueue(i)
						}
					}
				}()
			}

			values := make(map[int]struct{}, count)
			for range count {
				actual := q.Dequeue()
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

func hammerUMPSCQueueBlockingCalls(t *testing.T, gomaxprocs, numOps, numThreads int) {
	runtime.GOMAXPROCS(gomaxprocs)
	q := NewUMPSCQueue[int]()
	startwg := sync.WaitGroup{}
	startwg.Add(1)
	csum := make(chan int, 1)
	// Start producers.
	for i := range numThreads {
		go func(n int) {
			startwg.Wait()
			for j := n; j < numOps; j += numThreads {
				q.Enqueue(j)
			}
		}(i)
	}
	// Start consumer.
	go func() {
		startwg.Wait()
		sum := 0
		for range numOps {
			item := q.Dequeue()
			sum += item
		}
		csum <- sum
	}()
	startwg.Done()
	// Wait for the sum from the consumer.
	sum := <-csum
	// Assert the sum.
	expectedSum := numOps * (numOps - 1) / 2
	if sum != expectedSum {
		t.Fatalf("sums don't match for %d num ops, %d num threads: got %d, want %d",
			numOps, numThreads, sum, expectedSum)
	}
}

func TestUMPSCQueueBlockingCalls(t *testing.T) {
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(-1))
	n := 10
	if testing.Short() {
		n = 1
	}
	hammerUMPSCQueueBlockingCalls(t, 1, n, n)
	hammerUMPSCQueueBlockingCalls(t, 2, 10*n, 2*n)
	hammerUMPSCQueueBlockingCalls(t, 4, 100*n, 4*n)
}

// This benchmarks the performance of the [UMPSCQueue] vs using a normal channel. In the results,
// channels should get slower as the parallelism goes up due to contention on the lock which is
// acquired every time an element is added. By contrast, the queue actually should get faster. This
// is expected, as [testing.B.RunParallel] executes N operations with G goroutines, so it should
// take less time overall. The overall memory cost is negligible, especially since the allocation is not
// per-operation, it's per-segment, meaning it is amortized by the size of the segment. Additionally,
// segments are reused when possible, further decreasing the cost.
func BenchmarkChanVsUMPSCQueue(b *testing.B) {
	b.Run("method=queue", func(b *testing.B) {
		q := NewUMPSCQueue[int]()
		done := make(chan struct{})
		go func() {
			defer close(done)
			for b.Loop() {
				q.Dequeue()
			}
		}()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				q.Enqueue(0)
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
