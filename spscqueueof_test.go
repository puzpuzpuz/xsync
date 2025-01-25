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

	. "github.com/puzpuzpuz/xsync/v3"
)

func TestSPSCQueueOf_InvalidSize(t *testing.T) {
	defer func() { recover() }()
	NewSPSCQueueOf[int](0)
	t.Fatal("no panic detected")
}

func TestSPSCQueueOfTryEnqueueDequeueInt(t *testing.T) {
	q := NewSPSCQueueOf[int](10)
	for i := 0; i < 10; i++ {
		if !q.TryEnqueue(i) {
			t.Fatal("TryEnqueue failed")
		}
	}
	for i := 0; i < 10; i++ {
		if got, ok := q.TryDequeue(); !ok || got != i {
			t.Fatalf("%v: got %v, want %d", ok, got, i)
		}
	}
}

func TestSPSCQueueOfTryEnqueueDequeueString(t *testing.T) {
	q := NewSPSCQueueOf[string](10)
	for i := 0; i < 10; i++ {
		if !q.TryEnqueue(strconv.Itoa(i)) {
			t.Fatal("TryEnqueue failed")
		}
	}
	for i := 0; i < 10; i++ {
		if got, ok := q.TryDequeue(); !ok || got != strconv.Itoa(i) {
			t.Fatalf("%v: got %v, want %d", ok, got, i)
		}
	}
}

func TestSPSCQueueOfTryEnqueueDequeueStruct(t *testing.T) {
	type foo struct {
		bar int
		baz int
	}
	q := NewSPSCQueueOf[foo](10)
	for i := 0; i < 10; i++ {
		if !q.TryEnqueue(foo{i, i}) {
			t.Fatal("TryEnqueue failed")
		}
	}
	for i := 0; i < 10; i++ {
		if got, ok := q.TryDequeue(); !ok || got.bar != i || got.baz != i {
			t.Fatalf("%v: got %v, want %d", ok, got, i)
		}
	}
}

func TestSPSCQueueOfTryEnqueueDequeueStructRef(t *testing.T) {
	type foo struct {
		bar int
		baz int
	}
	q := NewSPSCQueueOf[*foo](11)
	for i := 0; i < 10; i++ {
		if !q.TryEnqueue(&foo{i, i}) {
			t.Fatal("TryEnqueue failed")
		}
	}
	if !q.TryEnqueue(nil) {
		t.Fatal("TryEnqueue with nil failed")
	}
	for i := 0; i < 10; i++ {
		if got, ok := q.TryDequeue(); !ok || got.bar != i || got.baz != i {
			t.Fatalf("%v: got %v, want %d", ok, got, i)
		}
	}
	if last, ok := q.TryDequeue(); !ok || last != nil {
		t.Fatalf("%v: got %v, want nil", ok, last)
	}
}

func TestSPSCQueueOfTryEnqueueDequeue(t *testing.T) {
	q := NewSPSCQueueOf[int](10)
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

func TestSPSCQueueOfTryEnqueueOnFull(t *testing.T) {
	q := NewSPSCQueueOf[string](1)
	if !q.TryEnqueue("foo") {
		t.Error("failed to enqueue initial item")
	}
	if q.TryEnqueue("bar") {
		t.Error("got success for enqueue on full queue")
	}
}

func TestSPSCQueueOfTryDequeueOnEmpty(t *testing.T) {
	q := NewSPSCQueueOf[int](2)
	if _, ok := q.TryDequeue(); ok {
		t.Error("got success for enqueue on empty queue")
	}
}

func hammerSPSCQueueOfNonBlockingCalls(t *testing.T, cap, numOps int) {
	q := NewSPSCQueueOf[int](cap)
	startwg := sync.WaitGroup{}
	startwg.Add(1)
	csum := make(chan int, 2)
	// Start producer.
	go func() {
		startwg.Wait()
		for j := 0; j < numOps; j++ {
			for !q.TryEnqueue(j) {
				// busy spin until success
			}
		}
	}()
	// Start consumer.
	go func() {
		startwg.Wait()
		sum := 0
		for j := 0; j < numOps; j++ {
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

func TestSPSCQueueOfNonBlockingCalls(t *testing.T) {
	n := 10
	if testing.Short() {
		n = 1
	}
	hammerSPSCQueueOfNonBlockingCalls(t, 1, n)
	hammerSPSCQueueOfNonBlockingCalls(t, 2, 2*n)
	hammerSPSCQueueOfNonBlockingCalls(t, 4, 4*n)
}

func benchmarkSPSCQueueOfProdCons(b *testing.B, queueSize, localWork int) {
	callsPerSched := queueSize
	N := int32(b.N / callsPerSched)
	c := make(chan bool, 2)
	q := NewSPSCQueueOf[int](queueSize)

	go func() {
		foo := 0
		for atomic.AddInt32(&N, -1) >= 0 {
			for g := 0; g < callsPerSched; g++ {
				for i := 0; i < localWork; i++ {
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
				for i := 0; i < localWork; i++ {
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

func BenchmarkSPSCQueueOfProdCons(b *testing.B) {
	benchmarkSPSCQueueOfProdCons(b, 1000, 0)
}

func BenchmarkSPSCQueueOfProdConsWork100(b *testing.B) {
	benchmarkSPSCQueueOfProdCons(b, 1000, 100)
}

func benchmarkSPSCChan(b *testing.B, chanSize, localWork int) {
	callsPerSched := chanSize
	N := int32(b.N / callsPerSched)
	c := make(chan bool, 2)
	myc := make(chan int, chanSize)

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

	<-c
	<-c
}

func BenchmarkSPSCChan(b *testing.B) {
	benchmarkSPSCChan(b, 1000, 0)
}

func BenchmarkSPSCChanWork100(b *testing.B) {
	benchmarkSPSCChan(b, 1000, 100)
}
