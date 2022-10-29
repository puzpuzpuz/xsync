// Copyright notice. The following tests are partially based on
// the following file from the Go Programming Language core repo:
// https://github.com/golang/go/blob/831f9376d8d730b16fb33dfd775618dffe13ce7a/src/runtime/chan_test.go

package xsync_test

import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	. "github.com/puzpuzpuz/xsync/v2"
)

func TestQueue_InvalidSize(t *testing.T) {
	defer func() { recover() }()
	NewMPMCQueue(0)
	t.Fatal("no panic detected")
}

func TestQueueEnqueueDequeue(t *testing.T) {
	q := NewMPMCQueue(10)
	for i := 0; i < 10; i++ {
		q.Enqueue(i)
	}
	for i := 0; i < 10; i++ {
		if got := q.Dequeue(); got != i {
			t.Fatalf("got %v, want %d", got, i)
		}
	}
}

func TestQueueEnqueueBlocksOnFull(t *testing.T) {
	q := NewMPMCQueue(1)
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

func TestQueueDequeueBlocksOnEmpty(t *testing.T) {
	q := NewMPMCQueue(2)
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

func TestQueueTryEnqueueDequeue(t *testing.T) {
	q := NewMPMCQueue(10)
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

func TestQueueTryEnqueueOnFull(t *testing.T) {
	q := NewMPMCQueue(1)
	if !q.TryEnqueue("foo") {
		t.Error("failed to enqueue initial item")
	}
	if q.TryEnqueue("bar") {
		t.Error("got success for enqueue on full queue")
	}
}

func TestQueueTryDequeueBlocksOnEmpty(t *testing.T) {
	q := NewMPMCQueue(2)
	if _, ok := q.TryDequeue(); ok {
		t.Error("got success for enqueue on empty queue")
	}
}

func hammerQueueBlockingCalls(t *testing.T, gomaxprocs, numOps, numThreads int) {
	runtime.GOMAXPROCS(gomaxprocs)
	q := NewMPMCQueue(numThreads)
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
				sum += item.(int)
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

func TestQueueBlockingCalls(t *testing.T) {
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(-1))
	n := 100
	if testing.Short() {
		n = 10
	}
	hammerQueueBlockingCalls(t, 1, 100*n, n)
	hammerQueueBlockingCalls(t, 1, 1000*n, 10*n)
	hammerQueueBlockingCalls(t, 4, 100*n, n)
	hammerQueueBlockingCalls(t, 4, 1000*n, 10*n)
	hammerQueueBlockingCalls(t, 8, 100*n, n)
	hammerQueueBlockingCalls(t, 8, 1000*n, 10*n)
}

func hammerQueueNonBlockingCalls(t *testing.T, gomaxprocs, numOps, numThreads int) {
	runtime.GOMAXPROCS(gomaxprocs)
	q := NewMPMCQueue(numThreads)
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
					item interface{}
					ok   bool
				)
				for {
					// busy spin until success
					if item, ok = q.TryDequeue(); ok {
						sum += item.(int)
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

func TestQueueNonBlockingCalls(t *testing.T) {
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(-1))
	n := 10
	if testing.Short() {
		n = 1
	}
	hammerQueueNonBlockingCalls(t, 1, n, n)
	hammerQueueNonBlockingCalls(t, 2, 10*n, 2*n)
	hammerQueueNonBlockingCalls(t, 4, 100*n, 4*n)
}

func benchmarkQueueProdCons(b *testing.B, queueSize, localWork int) {
	callsPerSched := queueSize
	procs := runtime.GOMAXPROCS(-1) / 2
	if procs == 0 {
		procs = 1
	}
	N := int32(b.N / callsPerSched)
	c := make(chan bool, 2*procs)
	q := NewMPMCQueue(queueSize)
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
				v := q.Dequeue().(int)
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

func BenchmarkQueueProdCons(b *testing.B) {
	benchmarkQueueProdCons(b, 1000, 0)
}

func BenchmarkQueueProdConsWork100(b *testing.B) {
	benchmarkQueueProdCons(b, 1000, 100)
}

func benchmarkChanProdCons(b *testing.B, chanSize, localWork int) {
	callsPerSched := chanSize
	procs := runtime.GOMAXPROCS(-1) / 2
	if procs == 0 {
		procs = 1
	}
	N := int32(b.N / callsPerSched)
	c := make(chan bool, 2*procs)
	myc := make(chan int, chanSize)
	for p := 0; p < procs; p++ {
		go func() {
			foo := 0
			for atomic.AddInt32(&N, -1) >= 0 {
				for g := 0; g < callsPerSched; g++ {
					for i := 0; i < localWork; i++ {
						foo *= 2
						foo /= 2
					}
					myc <- 1
				}
			}
			myc <- 0
			c <- foo == 42
		}()
		go func() {
			foo := 0
			for {
				v := <-myc
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

func BenchmarkChanProdCons(b *testing.B) {
	benchmarkChanProdCons(b, 1000, 0)
}

func BenchmarkChanProdConsWork100(b *testing.B) {
	benchmarkChanProdCons(b, 1000, 100)
}
