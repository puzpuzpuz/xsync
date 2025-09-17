// Copyright notice. The following tests are partially based on
// the following file from the Go Programming Language core repo:
// https://github.com/golang/go/blob/831f9376d8d730b16fb33dfd775618dffe13ce7a/src/runtime/chan_test.go

package xsync_test

import (
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	. "github.com/puzpuzpuz/xsync/v4"
)

func TestMPMCQueue_InvalidSize(t *testing.T) {
	defer func() { recover() }()
	NewMPMCQueue[int](0)
	t.Fatal("no panic detected")
}

func TestMPMCQueueEnqueueDequeueInt(t *testing.T) {
	q := NewMPMCQueue[int](10)
	for i := range 10 {
		if !q.TryEnqueue(i) {
			t.Fatalf("failed to enqueue for %d", i)
		}
	}
	for i := range 10 {
		if got, ok := q.TryDequeue(); !ok || got != i {
			t.Fatalf("%v got %v, want %d", ok, got, i)
		}
	}
}

func TestMPMCQueueEnqueueDequeueString(t *testing.T) {
	q := NewMPMCQueue[string](10)
	for i := range 10 {
		if !q.TryEnqueue(strconv.Itoa(i)) {
			t.Fatalf("failed to enqueue for %d", i)
		}
	}
	for i := range 10 {
		if got, ok := q.TryDequeue(); !ok || got != strconv.Itoa(i) {
			t.Fatalf("%v got %v, want %d", ok, got, i)
		}
	}
}

func TestMPMCQueueEnqueueDequeueStruct(t *testing.T) {
	type foo struct {
		bar int
		baz int
	}
	q := NewMPMCQueue[foo](10)
	for i := range 10 {
		if !q.TryEnqueue(foo{i, i}) {
			t.Fatalf("failed to enqueue for %d", i)
		}
	}
	for i := range 10 {
		if got, ok := q.TryDequeue(); !ok || got.bar != i || got.baz != i {
			t.Fatalf("%v got %v, want %d", ok, got, i)
		}
	}
}

func TestMPMCQueueEnqueueDequeueStructRef(t *testing.T) {
	type foo struct {
		bar int
		baz int
	}
	q := NewMPMCQueue[*foo](11)
	for i := range 10 {
		if !q.TryEnqueue(&foo{i, i}) {
			t.Fatalf("failed to enqueue for %d", i)
		}
	}
	if !q.TryEnqueue(nil) {
		t.Fatal("failed to enqueue for nil")
	}
	for i := range 10 {
		if got, ok := q.TryDequeue(); !ok || got.bar != i || got.baz != i {
			t.Fatalf("%v got %v, want %d", ok, got, i)
		}
	}
	if last, ok := q.TryDequeue(); !ok || last != nil {
		t.Fatalf("%v got %v, want nil", ok, last)
	}
}

func TestMPMCQueueTryEnqueueDequeue(t *testing.T) {
	q := NewMPMCQueue[int](10)
	for i := range 10 {
		if !q.TryEnqueue(i) {
			t.Fatalf("failed to enqueue for %d", i)
		}
	}
	for i := range 10 {
		if got, ok := q.TryDequeue(); !ok || got != i {
			t.Fatalf("got %v, want %d, for status %v", got, i, ok)
		}
	}
}

func TestMPMCQueueTryEnqueueOnFull(t *testing.T) {
	q := NewMPMCQueue[string](1)
	if !q.TryEnqueue("foo") {
		t.Error("failed to enqueue initial item")
	}
	if q.TryEnqueue("bar") {
		t.Error("got success for enqueue on full queue")
	}
}

func TestMPMCQueueTryDequeueOnEmpty(t *testing.T) {
	q := NewMPMCQueue[int](2)
	if _, ok := q.TryDequeue(); ok {
		t.Error("got success for enqueue on empty queue")
	}
}

func hammerMPMCQueueNonBlockingCalls(t *testing.T, gomaxprocs, numOps, numThreads int) {
	runtime.GOMAXPROCS(gomaxprocs)
	q := NewMPMCQueue[int](numThreads)
	startwg := sync.WaitGroup{}
	startwg.Add(1)
	csum := make(chan int, numThreads)
	// Start producers.
	for i := range numThreads {
		go func(n int) {
			startwg.Wait()
			for j := n; j < numOps; j += numThreads {
				for !q.TryEnqueue(j) {
					// busy spin until success
				}
			}
		}(i)
	}
	// Start consumers.
	for i := range numThreads {
		go func(n int) {
			startwg.Wait()
			sum := 0
			for j := n; j < numOps; j += numThreads {
				var (
					item int
					ok   bool
				)
				for {
					// busy spin until success
					if item, ok = q.TryDequeue(); ok {
						sum += item
						break
					}
				}
			}
			csum <- sum
		}(i)
	}
	startwg.Done()
	// Wait for all the sums from consumers.
	sum := 0
	for range numThreads {
		s := <-csum
		sum += s
	}
	// Assert the total sum.
	expectedSum := numOps * (numOps - 1) / 2
	if sum != expectedSum {
		t.Fatalf("sums don't match for %d num ops, %d num threads: got %d, want %d",
			numOps, numThreads, sum, expectedSum)
	}
}

func TestMPMCQueueNonBlockingCalls(t *testing.T) {
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(-1))
	n := 10
	if testing.Short() {
		n = 1
	}
	hammerMPMCQueueNonBlockingCalls(t, 1, n, n)
	hammerMPMCQueueNonBlockingCalls(t, 2, 10*n, 2*n)
	hammerMPMCQueueNonBlockingCalls(t, 4, 100*n, 4*n)
}

func benchmarkMPMCQueue(b *testing.B, queueSize, localWork int) {
	callsPerSched := queueSize
	procs := runtime.GOMAXPROCS(-1) / 2
	if procs == 0 {
		procs = 1
	}
	N := int32(b.N / callsPerSched)
	c := make(chan bool, 2*procs)
	q := NewMPMCQueue[int](queueSize)
	for p := 0; p < procs; p++ {
		go func() {
			foo := 0
			for atomic.AddInt32(&N, -1) >= 0 {
				for range callsPerSched {
					for range localWork {
						foo *= 2
						foo /= 2
					}
					for !q.TryEnqueue(1) {
						runtime.Gosched()
					}
				}
			}
			for !q.TryEnqueue(0) {
				runtime.Gosched()
			}
			c <- foo == 42
		}()
		go func() {
			foo := 0
			for {
				var (
					v  int
					ok bool
				)
				for {
					if v, ok = q.TryDequeue(); !ok {
						runtime.Gosched()
					} else {
						break
					}
				}
				if v == 0 {
					break
				}
				for range localWork {
					foo *= 2
					foo /= 2
				}
			}
			c <- foo == 42
		}()
	}
	for p := 0; p < procs; p++ {
		<-c
		<-c
	}
}

func BenchmarkMPMCQueue(b *testing.B) {
	benchmarkMPMCQueue(b, 1000, 0)
}

func BenchmarkMPMCQueueWork100(b *testing.B) {
	benchmarkMPMCQueue(b, 1000, 100)
}
