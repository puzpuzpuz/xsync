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

func TestDeprecatedSPSCQueueOf(t *testing.T) {
	q := NewSPSCQueueOf[int](5)
	if !q.TryEnqueue(1) {
		t.Fatal("enqueue failed")
	}
	if v, ok := q.TryDequeue(); !ok || v != 1 {
		t.Fatalf("got %v/%v, want 1/true", v, ok)
	}
}

func TestSPSCQueueInvalidSize(t *testing.T) {
	defer func() { recover() }()
	NewSPSCQueue[int](0)
	t.Fatal("no panic detected")
}

func TestSPSCQueueWraparound(t *testing.T) {
	const capacity = 3
	const cycles = 5
	q := NewSPSCQueue[int](capacity)
	// Cycle through the queue multiple times to test index wraparound
	for cycle := range cycles {
		for i := range capacity {
			if !q.TryEnqueue(cycle*capacity + i) {
				t.Fatalf("cycle %d: enqueue %d failed", cycle, i)
			}
		}
		for i := range capacity {
			v, ok := q.TryDequeue()
			if !ok || v != cycle*capacity+i {
				t.Fatalf("cycle %d: got %v/%v, want %d/true", cycle, v, ok, cycle*capacity+i)
			}
		}
	}
}

func TestSPSCQueueConsumerCacheInvalidation(t *testing.T) {
	// Tests ccachedIdx invalidation in TryEnqueue.
	// When the producer's cached consumer index is stale, it must be refreshed.
	q := NewSPSCQueue[int](2)

	// Fill the queue to capacity
	if !q.TryEnqueue(1) {
		t.Fatal("first enqueue failed")
	}
	if !q.TryEnqueue(2) {
		t.Fatal("second enqueue failed")
	}

	// Queue is full, enqueue should fail
	if q.TryEnqueue(3) {
		t.Fatal("enqueue on full queue should fail")
	}

	// Dequeue one item - this updates cidx but producer's ccachedIdx is stale
	if v, ok := q.TryDequeue(); !ok || v != 1 {
		t.Fatalf("dequeue: got %v/%v, want 1/true", v, ok)
	}

	// Now enqueue should succeed after cache invalidation
	if !q.TryEnqueue(3) {
		t.Fatal("enqueue after dequeue failed - cache invalidation issue")
	}

	// Verify queue contents
	if v, ok := q.TryDequeue(); !ok || v != 2 {
		t.Fatalf("got %v/%v, want 2/true", v, ok)
	}
	if v, ok := q.TryDequeue(); !ok || v != 3 {
		t.Fatalf("got %v/%v, want 3/true", v, ok)
	}
}

func TestSPSCQueueProducerCacheInvalidation(t *testing.T) {
	// Tests pcachedIdx invalidation in TryDequeue.
	// When the consumer's cached producer index is stale, it must be refreshed.
	q := NewSPSCQueue[int](2)

	// Queue is empty, dequeue should fail
	if _, ok := q.TryDequeue(); ok {
		t.Fatal("dequeue on empty queue should fail")
	}

	// Enqueue an item - this updates pidx but consumer's pcachedIdx is stale
	if !q.TryEnqueue(1) {
		t.Fatal("enqueue failed")
	}

	// Now dequeue should succeed after cache invalidation
	if v, ok := q.TryDequeue(); !ok || v != 1 {
		t.Fatalf("dequeue after enqueue failed - got %v/%v, want 1/true", v, ok)
	}

	// Verify queue is empty again
	if _, ok := q.TryDequeue(); ok {
		t.Fatal("dequeue on empty queue should fail")
	}
}

func TestSPSCQueueEnqueueDequeueInt(t *testing.T) {
	q := NewSPSCQueue[int](10)
	for i := range 10 {
		if !q.TryEnqueue(i) {
			t.Fatal("TryEnqueue failed")
		}
	}
	for i := range 10 {
		if got, ok := q.TryDequeue(); !ok || got != i {
			t.Fatalf("%v: got %v, want %d", ok, got, i)
		}
	}
}

func TestSPSCQueueEnqueueDequeueString(t *testing.T) {
	q := NewSPSCQueue[string](10)
	for i := range 10 {
		if !q.TryEnqueue(strconv.Itoa(i)) {
			t.Fatal("TryEnqueue failed")
		}
	}
	for i := range 10 {
		if got, ok := q.TryDequeue(); !ok || got != strconv.Itoa(i) {
			t.Fatalf("%v: got %v, want %d", ok, got, i)
		}
	}
}

func TestSPSCQueueEnqueueDequeueStruct(t *testing.T) {
	type foo struct {
		bar int
		baz int
	}
	q := NewSPSCQueue[foo](10)
	for i := range 10 {
		if !q.TryEnqueue(foo{i, i}) {
			t.Fatal("TryEnqueue failed")
		}
	}
	for i := range 10 {
		if got, ok := q.TryDequeue(); !ok || got.bar != i || got.baz != i {
			t.Fatalf("%v: got %v, want %d", ok, got, i)
		}
	}
}

func TestSPSCQueueEnqueueDequeueStructRef(t *testing.T) {
	type foo struct {
		bar int
		baz int
	}
	q := NewSPSCQueue[*foo](11)
	for i := range 10 {
		if !q.TryEnqueue(&foo{i, i}) {
			t.Fatal("TryEnqueue failed")
		}
	}
	if !q.TryEnqueue(nil) {
		t.Fatal("TryEnqueue with nil failed")
	}
	for i := range 10 {
		if got, ok := q.TryDequeue(); !ok || got.bar != i || got.baz != i {
			t.Fatalf("%v: got %v, want %d", ok, got, i)
		}
	}
	if last, ok := q.TryDequeue(); !ok || last != nil {
		t.Fatalf("%v: got %v, want nil", ok, last)
	}
}

func TestSPSCQueueTryEnqueueDequeue(t *testing.T) {
	q := NewSPSCQueue[int](10)
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

func TestSPSCQueueTryEnqueueOnFull(t *testing.T) {
	q := NewSPSCQueue[string](1)
	if !q.TryEnqueue("foo") {
		t.Error("failed to enqueue initial item")
	}
	if q.TryEnqueue("bar") {
		t.Error("got success for enqueue on full queue")
	}
}

func TestSPSCQueueTryDequeueOnEmpty(t *testing.T) {
	q := NewSPSCQueue[int](2)
	if _, ok := q.TryDequeue(); ok {
		t.Error("got success for enqueue on empty queue")
	}
}

func hammerSPSCQueueNonBlockingCalls(t *testing.T, cap, numOps int) {
	q := NewSPSCQueue[int](cap)
	startwg := sync.WaitGroup{}
	startwg.Add(1)
	csum := make(chan int, 2)
	// Start producer.
	go func() {
		startwg.Wait()
		for j := range numOps {
			for !q.TryEnqueue(j) {
				// busy spin until success
			}
		}
	}()
	// Start consumer.
	go func() {
		startwg.Wait()
		sum := 0
		for range numOps {
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
	}()
	startwg.Done()
	// Wait for all the sum from the producer.
	sum := <-csum
	// Assert the total sum.
	expectedSum := numOps * (numOps - 1) / 2
	if sum != expectedSum {
		t.Fatalf("sums don't match for %d num ops: got %d, want %d",
			numOps, sum, expectedSum)
	}
}

func TestSPSCQueueNonBlockingCalls(t *testing.T) {
	n := 10
	if testing.Short() {
		n = 1
	}
	hammerSPSCQueueNonBlockingCalls(t, 1, n)
	hammerSPSCQueueNonBlockingCalls(t, 2, 2*n)
	hammerSPSCQueueNonBlockingCalls(t, 4, 4*n)
}

func benchmarkSPSCQueueProdCons(b *testing.B, queueSize, localWork int) {
	callsPerSched := queueSize
	N := int32(b.N / callsPerSched)
	c := make(chan bool, 2)
	q := NewSPSCQueue[int](queueSize)

	go func() {
		foo := 0
		for atomic.AddInt32(&N, -1) >= 0 {
			for range callsPerSched {
				for range localWork {
					foo *= 2
					foo /= 2
				}
				if !q.TryEnqueue(1) {
					runtime.Gosched()
				}
			}
		}
		q.TryEnqueue(0)
		c <- foo == 42
	}()

	go func() {
		foo := 0
		for {
			v, ok := q.TryDequeue()
			if ok {
				if v == 0 {
					break
				}
				for range localWork {
					foo *= 2
					foo /= 2
				}
			} else {
				runtime.Gosched()
			}
		}
		c <- foo == 42
	}()

	<-c
	<-c
}

func BenchmarkSPSCQueueProdCons(b *testing.B) {
	benchmarkSPSCQueueProdCons(b, 1000, 0)
}

func BenchmarkSPSCQueueProdConsWork100(b *testing.B) {
	benchmarkSPSCQueueProdCons(b, 1000, 100)
}

func benchmarkSPSCChan(b *testing.B, chanSize, localWork int) {
	callsPerSched := chanSize
	N := int32(b.N / callsPerSched)
	c := make(chan bool, 2)
	myc := make(chan int, chanSize)

	go func() {
		foo := 0
		for atomic.AddInt32(&N, -1) >= 0 {
			for range callsPerSched {
				for range localWork {
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
			for range localWork {
				foo *= 2
				foo /= 2
			}
		}
		c <- foo == 42
	}()

	<-c
	<-c
}

func BenchmarkSPSCChan(b *testing.B) {
	benchmarkSPSCChan(b, 1000, 0)
}

func BenchmarkSPSCChanWork100(b *testing.B) {
	benchmarkSPSCChan(b, 1000, 100)
}
