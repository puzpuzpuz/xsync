package xsync

import (
	"runtime"
	"sync/atomic"
	"unsafe"
)

const (
	cacheLineSize = 64
)

// A MPMCQueue is a bounded multi-producer multi-consumer concurrent queue.
//
// MPMCQueue instances must be created with NewMPMCQueue function.
// A MPMCQueue must not be copied after first use.
//
// Based on the algorithm from the following C++ library:
// https://github.com/rigtorp/MPMCQueue
type MPMCQueue struct {
	cap   uint64
	head  uint64
	hpad  [cacheLineSize - 8]byte
	tail  uint64
	tpad  [cacheLineSize - 8]byte
	slots []slot
}

type slot struct {
	slotInternal
	pad [cacheLineSize - unsafe.Sizeof(slotInternal{})]byte
}

type slotInternal struct {
	turn uint64
	item interface{}
}

// NewMPMCQueue creates a new MPMCQueue instance with the given capacity.
func NewMPMCQueue(capacity int) *MPMCQueue {
	if capacity < 1 {
		panic("capacity must be positive number")
	}
	return &MPMCQueue{
		cap:   uint64(capacity),
		slots: make([]slot, capacity),
	}
}

// Enqueue inserts the given item into the queue. Blocks, if the queue
// is full.
func (q *MPMCQueue) Enqueue(item interface{}) {
	head := atomic.AddUint64(&q.head, 1) - 1
	slot := &q.slots[q.idx(head)]
	turn := q.turn(head) * 2
	for atomic.LoadUint64(&slot.turn) != turn {
		runtime.Gosched()
	}
	slot.item = item
	atomic.StoreUint64(&slot.turn, turn+1)
}

// Dequeue retrieves and removes the head of the queue. Blocks, if
// the queue is empty.
func (q *MPMCQueue) Dequeue() interface{} {
	tail := atomic.AddUint64(&q.tail, 1) - 1
	slot := &q.slots[q.idx(tail)]
	turn := q.turn(tail)*2 + 1
	for atomic.LoadUint64(&slot.turn) != turn {
		runtime.Gosched()
	}
	item := slot.item
	slot.item = nil
	atomic.StoreUint64(&slot.turn, turn+1)
	return item
}

func (q *MPMCQueue) idx(i uint64) uint64 {
	return i % q.cap
}

func (q *MPMCQueue) turn(i uint64) uint64 {
	return i / q.cap
}
