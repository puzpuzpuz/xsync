// Copyright notice. The following tests are partially based on
// the following file from the Go Programming Language core repo:
// https://github.com/golang/go/blob/831f9376d8d730b16fb33dfd775618dffe13ce7a/src/runtime/chan_test.go

package xsync_test

import (
	"sync"
	"testing"

	. "github.com/puzpuzpuz/xsync/v3"
)

func TestSPSCQueue_InvalidSize(t *testing.T) {
	defer func() { recover() }()
	NewSPSCQueue(0)
	t.Fatal("no panic detected")
}

func TestSPSCQueueTryEnqueueDequeue(t *testing.T) {
	q := NewSPSCQueue(10)
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

func TestSPSCQueueTryEnqueueOnFull(t *testing.T) {
	q := NewSPSCQueue(1)
	if !q.TryEnqueue("foo") {
		t.Error("failed to enqueue initial item")
	}
	if q.TryEnqueue("bar") {
		t.Error("got success for enqueue on full queue")
	}
}

func TestSPSCQueueTryDequeueOnEmpty(t *testing.T) {
	q := NewSPSCQueue(2)
	if _, ok := q.TryDequeue(); ok {
		t.Error("got success for enqueue on empty queue")
	}
}

func hammerSPSCQueueNonBlockingCalls(t *testing.T, cap, numOps int) {
	q := NewSPSCQueue(cap)
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
