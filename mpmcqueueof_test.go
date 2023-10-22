//go:build go1.19
// +build go1.19

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
	"time"

	. "github.com/puzpuzpuz/xsync/v3"
)

func TestQueueOf_InvalidSize(t *testing.T) {
	defer func() { recover() }()
	NewMPMCQueueOf[int](0)
	t.Fatal("no panic detected")
}

func TestQueueOfEnqueueDequeueInt(t *testing.T) {
	q := NewMPMCQueueOf[int](10)
	for i := 0; i < 10; i++ {
		q.Enqueue(i)
	}
	for i := 0; i < 10; i++ {
		if got := q.Dequeue(); got != i {
			t.Fatalf("got %v, want %d", got, i)
		}
	}
}

func TestQueueOfEnqueueDequeueString(t *testing.T) {
	q := NewMPMCQueueOf[string](10)
	for i := 0; i < 10; i++ {
		q.Enqueue(strconv.Itoa(i))
	}
	for i := 0; i < 10; i++ {
		if got := q.Dequeue(); got != strconv.Itoa(i) {
			t.Fatalf("got %v, want %d", got, i)
		}
	}
}

func TestQueueOfEnqueueDequeueStruct(t *testing.T) {
	type foo struct {
		bar int
		baz int
	}
	q := NewMPMCQueueOf[foo](10)
	for i := 0; i < 10; i++ {
		q.Enqueue(foo{i, i})
	}
	for i := 0; i < 10; i++ {
		if got := q.Dequeue(); got.bar != i || got.baz != i {
			t.Fatalf("got %v, want %d", got, i)
		}
	}
}

func TestQueueOfEnqueueDequeueStructRef(t *testing.T) {
	type foo struct {
		bar int
		baz int
	}
	q := NewMPMCQueueOf[*foo](11)
	for i := 0; i < 10; i++ {
		q.Enqueue(&foo{i, i})
	}
	q.Enqueue(nil)
	for i := 0; i < 10; i++ {
		if got := q.Dequeue(); got.bar != i || got.baz != i {
			t.Fatalf("got %v, want %d", got, i)
		}
	}
	if last := q.Dequeue(); last != nil {
		t.Fatalf("got %v, want nil", last)
	}
}

func TestQueueOfEnqueueBlocksOnFull(t *testing.T) {
	q := NewMPMCQueueOf[string](1)
	q.Enqueue("foo")
	cdone := make(chan bool)
	flag := int32(0)
	go func() {
		q.Enqueue("bar")
		if atomic.LoadInt32(&flag) == 0 {
			t.Error("enqueue on full queue didn't wait for dequeue")
		}
		cdone <- true
	}()
	time.Sleep(50 * time.Millisecond)
	atomic.StoreInt32(&flag, 1)
	if got := q.Dequeue(); got != "foo" {
		t.Fatalf("got %v, want foo", got)
	}
	<-cdone
}

func TestQueueOfDequeueBlocksOnEmpty(t *testing.T) {
	q := NewMPMCQueueOf[string](2)
	cdone := make(chan bool)
	flag := int32(0)
	go func() {
		q.Dequeue()
		if atomic.LoadInt32(&flag) == 0 {
			t.Error("dequeue on empty queue didn't wait for enqueue")
		}
		cdone <- true
	}()
	time.Sleep(50 * time.Millisecond)
	atomic.StoreInt32(&flag, 1)
	q.Enqueue("foobar")
	<-cdone
}

func TestQueueOfTryEnqueueDequeue(t *testing.T) {
	q := NewMPMCQueueOf[int](10)
	for i := 0; i < 10; i++ {
		if !q.TryEnqueue(i) {
			t.Fatalf("failed to enqueue for %d", i)
		}
	}
	for i := 0; i < 10; i++ {
		if got, ok := q.TryDequeue(); !ok || got != i {
			t.Fatalf("got %v, want %d, for status %v", got, i, ok)
		}
	}
}

func TestQueueOfTryEnqueueOnFull(t *testing.T) {
	q := NewMPMCQueueOf[string](1)
	if !q.TryEnqueue("foo") {
		t.Error("failed to enqueue initial item")
	}
	if q.TryEnqueue("bar") {
		t.Error("got success for enqueue on full queue")
	}
}

func TestQueueOfTryDequeueBlocksOnEmpty(t *testing.T) {
	q := NewMPMCQueueOf[int](2)
	if _, ok := q.TryDequeue(); ok {
		t.Error("got success for enqueue on empty queue")
	}
}

func hammerQueueOfBlockingCalls(t *testing.T, gomaxprocs, numOps, numThreads int) {
	runtime.GOMAXPROCS(gomaxprocs)
	q := NewMPMCQueueOf[int](numThreads)
	startwg := sync.WaitGroup{}
	startwg.Add(1)
	csum := make(chan int, numThreads)
	// Start producers.
	for i := 0; i < numThreads; i++ {
		go func(n int) {
			startwg.Wait()
			for j := n; j < numOps; j += numThreads {
				q.Enqueue(j)
			}
		}(i)
	}
	// Start consumers.
	for i := 0; i < numThreads; i++ {
		go func(n int) {
			startwg.Wait()
			sum := 0
			for j := n; j < numOps; j += numThreads {
				item := q.Dequeue()
				sum += item
			}
			csum <- sum
		}(i)
	}
	startwg.Done()
	// Wait for all the sums from producers.
	sum := 0
	for i := 0; i < numThreads; i++ {
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

func TestQueueOfBlockingCalls(t *testing.T) {
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(-1))
	n := 100
	if testing.Short() {
		n = 10
	}
	hammerQueueOfBlockingCalls(t, 1, 100*n, n)
	hammerQueueOfBlockingCalls(t, 1, 1000*n, 10*n)
	hammerQueueOfBlockingCalls(t, 4, 100*n, n)
	hammerQueueOfBlockingCalls(t, 4, 1000*n, 10*n)
	hammerQueueOfBlockingCalls(t, 8, 100*n, n)
	hammerQueueOfBlockingCalls(t, 8, 1000*n, 10*n)
}

func hammerQueueOfNonBlockingCalls(t *testing.T, gomaxprocs, numOps, numThreads int) {
	runtime.GOMAXPROCS(gomaxprocs)
	q := NewMPMCQueueOf[int](numThreads)
	startwg := sync.WaitGroup{}
	startwg.Add(1)
	csum := make(chan int, numThreads)
	// Start producers.
	for i := 0; i < numThreads; i++ {
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
	for i := 0; i < numThreads; i++ {
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
	// Wait for all the sums from producers.
	sum := 0
	for i := 0; i < numThreads; i++ {
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

func TestQueueOfNonBlockingCalls(t *testing.T) {
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(-1))
	n := 10
	if testing.Short() {
		n = 1
	}
	hammerQueueOfNonBlockingCalls(t, 1, n, n)
	hammerQueueOfNonBlockingCalls(t, 2, 10*n, 2*n)
	hammerQueueOfNonBlockingCalls(t, 4, 100*n, 4*n)
}

func benchmarkQueueOfProdCons(b *testing.B, queueSize, localWork int) {
	callsPerSched := queueSize
	procs := runtime.GOMAXPROCS(-1) / 2
	if procs == 0 {
		procs = 1
	}
	N := int32(b.N / callsPerSched)
	c := make(chan bool, 2*procs)
	q := NewMPMCQueueOf[int](queueSize)
	for p := 0; p < procs; p++ {
		go func() {
			foo := 0
			for atomic.AddInt32(&N, -1) >= 0 {
				for g := 0; g < callsPerSched; g++ {
					for i := 0; i < localWork; i++ {
						foo *= 2
						foo /= 2
					}
					q.Enqueue(1)
				}
			}
			q.Enqueue(0)
			c <- foo == 42
		}()
		go func() {
			foo := 0
			for {
				v := q.Dequeue()
				if v == 0 {
					break
				}
				for i := 0; i < localWork; i++ {
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

func BenchmarkQueueOfProdCons(b *testing.B) {
	benchmarkQueueOfProdCons(b, 1000, 0)
}

func BenchmarkOfQueueProdConsWork100(b *testing.B) {
	benchmarkQueueOfProdCons(b, 1000, 100)
}
