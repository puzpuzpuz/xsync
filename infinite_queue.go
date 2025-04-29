package xsync

import (
	"math"
	"sync"
	"sync/atomic"
	"unsafe"
)

func NewInfiniteQueue[T any]() *InfiniteQueue[T] {
	q := &InfiniteQueue[T]{}
	q.readHead = q.newSegment()
	q.writeHead.Store(q.readHead)
	return q
}

// The InfiniteQueue is a replacement for a channel. However, crucially, it has infinite capacity.
// This is a very bad idea in many cases as it means that it never exhibits backpressure. In other
// words, if nothing is consuming elements from the queue, it will eventually consume all available
// memory and crash the process. However, there are also cases where this is desired behavior as it
// means the queue will dynamically allocate more memory to store temporary bursts, allowing
// producers to never block while the consumer catches up.
//
// The backing data structure is represented as a singly linked list of large segments. The size of
// the segments is determined empirically. Each segment is a slice of T along with a corresponding
// [sync.WaitGroup] for each index. Producers use an atomic counter to determine the unique index in
// the segment where they will write their value, and mark the corresponding wait group as done after
// having written the value. The consumer simply keeps track of the index it wants to read and waits
// for the corresponding wait group to complete. Neither operation acquires a lock and therefore
// performs quite well under highly contentious loads.
//
// Note however that because no locks are acquired, it is unsafe for multiple goroutines to consume
// from the queue. Consumers must explicitly synchronize between themselves. This allows setups with
// a single consumer to never acquire a lock, significantly speeding up consumption.
type InfiniteQueue[T any] struct {
	// Represents the current head of the queue. This is updated by writers as they materialize the
	// segments of the queue.
	writeHead atomic.Pointer[queueSegment[T]]
	// Padding to prevent false sharing.
	_ [cacheLineSize - unsafe.Sizeof(atomic.Pointer[queueSegment[T]]{})]byte

	// Used to pool slices of queueValue to relieve pressure on the garbage collector.
	segmentPool sync.Pool

	readHead *queueSegment[T]
	readIdx  int
}

// This value is chose arbitrarily, as increasing it gives diminishing returns. With some testing (on
// 64-core machines), when the segment size is smaller than 2^10, the queue becomes slower as
// parallelism increases, while there is no statistically significant difference beyond 2^12.
const segmentSize = 1 << 12

// key/value pair representing the [time.Duration] that should be inserted into the backing
// [metrics.TimeHistogram], and whether the value is ready to be read. The reading goroutine should
// not attempt to read the value until the ready [sync.WaitGroup] has been marked as done.
type queueValue[T any] struct {
	value T
	ready sync.WaitGroup
}

// init initializes the [sync.WaitGroup] so that get blocks until set is called.
func (hv *queueValue[T]) init() {
	hv.ready.Add(1)
}

// set sets the value and marks it as ready.
func (hv *queueValue[T]) set(value T) {
	hv.value = value
	hv.ready.Done()
}

// get waits for the value to be ready, then reads it.
func (hv *queueValue[T]) get() T {
	hv.ready.Wait()
	return hv.value
}

type queueSegment[T any] struct {
	// Incremented every time a writer wants to write to this segment, and prevents multiple writers from
	// attempting to write to the same index. If the index is greater than the size of the segment,
	// pending writers should try again in the next segment.
	idx atomic.Uint64
	// Padding to prevent false sharing.
	_ [cacheLineSize - unsafe.Sizeof(atomic.Uint64{})]byte
	// The set of values this segment.
	values []queueValue[T]
	// Synchronizes the creation of the next segment.
	nextOnce sync.Once
	next     *queueSegment[T]
}

// newSegment creates a new queueSegment and pre-allocates the value slice by either reusing one
// from the pool or creating a fresh one.
func (q *InfiniteQueue[T]) newSegment() *queueSegment[T] {
	values, ok := q.segmentPool.Get().([]queueValue[T])
	if !ok {
		values = make([]queueValue[T], segmentSize)
	}
	for i := range values {
		values[i].init()
	}

	s := &queueSegment[T]{
		values: values,
	}
	// Storing math.MaxUint64 means the first call to Add(1) will return 0, not 1! Otherwise, it becomes
	// very messy as the real intended index is actually s.idx.Add(1) - 1. Setting this once makes this
	// less error-prone.
	s.idx.Store(math.MaxUint64)
	return s
}

func (q *InfiniteQueue[T]) loadNext(s *queueSegment[T]) *queueSegment[T] {
	s.nextOnce.Do(func() {
		s.next = q.newSegment()
	})
	return s.next
}

// Take returns the next value in the queue, blocking if it is empty. It is not safe to invoke Take
// from multiple goroutines.
//
// As it completes reading a segment, it returns the backing value slice to the pool. The actual
// queueSegment itself cannot be reused as it contains the pointer to the next segment, which cannot
// safely be updated as it cannot be determined whether all writers have released all references to
// it.
func (q *InfiniteQueue[T]) Take() T {
	t := q.readHead.values[q.readIdx].get()
	q.readIdx++
	if q.readIdx == segmentSize {
		q.readIdx = 0
		q.segmentPool.Put(q.readHead.values)
		q.readHead = q.loadNext(q.readHead)
	}
	return t
}

// Add writes the given value to the queue. It never blocks and is safe to be called by multiple
// goroutines concurrently.
func (q *InfiniteQueue[T]) Add(value T) {
	var segment *queueSegment[T]
	for {
		segment = q.writeHead.Load()
		idx := segment.idx.Add(1)
		if idx < segmentSize {
			segment.values[idx].set(value)
			// Optimization: eagerly creating the next segment means less contention as it's unlikely that other
			// writers have already gotten to the end of the segment and are also invoking loadNext, which blocks
			// until the segment has been created.
			if idx == 0 {
				q.loadNext(segment)
			}
			return
		} else {
			var prev *queueSegment[T]
			prev, segment = segment, q.loadNext(segment)
			q.writeHead.CompareAndSwap(prev, segment)
		}
	}
}
