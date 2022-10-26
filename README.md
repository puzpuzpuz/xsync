[![GoDoc reference](https://img.shields.io/badge/godoc-reference-blue.svg)](https://pkg.go.dev/github.com/puzpuzpuz/xsync/v2)
[![GoReport](https://goreportcard.com/badge/github.com/puzpuzpuz/xsync/v2)](https://goreportcard.com/report/github.com/puzpuzpuz/xsync/v2)
[![codecov](https://codecov.io/gh/puzpuzpuz/xsync/branch/main/graph/badge.svg)](https://codecov.io/gh/puzpuzpuz/xsync)

# xsync

Concurrent data structures for Go. Aims to provide more scalable alternatives for some of the data structures from the standard `sync` package, but not only.

### Benchmarks

Benchmark results may be found [here](BENCHMARKS.md).

## Counter

A `Counter` is a striped `int64` counter inspired by the j.u.c.a.LongAdder class from Java standard library.

```go
var c xsync.Counter
// increment and decrement the counter
c.Inc()
c.Dec()
// read the current value 
v := c.Value()
```

Works better in comparison with a single atomically updated int64 counter in high contention scenarios.

## Map

A `Map` is like a concurrent hash table based map. It follows the interface of `sync.Map` with a few extensions, like `LoadOrCompute` or `Size` methods.

```go
m := xsync.NewMap()
m.Store("foo", "bar")
v, ok := m.Load("foo")
s := m.Size()
```

`Map` uses a modified version of Cache-Line Hash Table (CLHT) data structure: https://github.com/LPD-EPFL/CLHT

CLHT is built around idea to organize the hash table in cache-line-sized buckets, so that on all modern CPUs update operations complete with minimal cache-line transfer. Also, `Get`, `Range` and other read-only operations are obstruction-free and involve no writes to shared memory, hence no mutexes or any other sort of locks. Due to this design, in all considered scenarios `Map` outperforms `sync.Map`.

One important difference with `sync.Map` is that only string keys are supported. That's because Golang standard library does not expose the built-in hash functions for `interface{}` values.

`MapOf[V]` is an implementation with parametrized value type. It is available for Go 1.18 or later.

```go
m := xsync.NewMapOf[string]()
m.Store("foo", "bar")
v, ok := m.Load("foo")
```

One important difference with `Map` is that `MapOf` supports arbitrary `comparable` key types:

```go
type Point struct {
	x int32
	y int32
}
m := NewTypedMapOf[Point, int](func(seed maphash.Seed, p Point) uint64 {
	// provide a hash function when creating the MapOf;
	// we recommend using the hash/maphash package for the function
	var h maphash.Hash
	h.SetSeed(seed)
	binary.Write(&h, binary.LittleEndian, p.x)
	hash := h.Sum64()
	h.Reset()
	binary.Write(&h, binary.LittleEndian, p.y)
	return 31*hash + h.Sum64()
})
m.Store(Point{42, 42}, 42)
v, ok := m.Load(point{42, 42})
```

## MPMCQueue

A `MPMCQeueue` is a bounded multi-producer multi-consumer concurrent queue.

```go
q := xsync.NewMPMCQueue(1024)
// producer inserts an item into the queue
q.Enqueue("foo")
// optimistic insertion attempt; doesn't block
inserted := q.TryEnqueue("bar")
// consumer obtains an item from the queue
item := q.Dequeue()
// optimistic obtain attempt; doesn't block
item, ok := q.TryDequeue()
```

Based on the algorithm from the [MPMCQueue](https://github.com/rigtorp/MPMCQueue) C++ library which in its turn references D.Vyukov's [MPMC queue](https://www.1024cores.net/home/lock-free-algorithms/queues/bounded-mpmc-queue). According to the following [classification](https://www.1024cores.net/home/lock-free-algorithms/queues), the queue is array-based, fails on overflow, provides causal FIFO, has blocking producers and consumers.

The idea of the algorithm is to allow parallelism for concurrent producers and consumers by introducing the notion of tickets, i.e. values of two counters, one per producers/consumers. An atomic increment of one of those counters is the only noticeable contention point in queue operations. The rest of the operation avoids contention on writes thanks to the turn-based read/write access for each of the queue items.

In essence, `MPMCQueue` is a specialized queue for scenarios where there are multiple concurrent producers and consumers of a single queue running on a large multicore machine.

To get the optimal performance, you may want to set the queue size to be large enough, say, an order of magnitude greater than the number of producers/consumers, to allow producers and consumers to progress with their queue operations in parallel most of the time.

## RBMutex

A `RBMutex` is a reader biased reader/writer mutual exclusion lock. The lock can be held by an many readers or a single writer.

```go
var m xsync.RBMutex
// reader lock calls return a token
t := m.RLock()
// the token must be later used to unlock the mutex
m.RUnlock(t)
// writer locks are the same as in sync.RWMutex
m.Lock()
m.Unlock()
```

`RBMutex` is based on the BRAVO (Biased Locking for Reader-Writer Locks) algorithm: https://arxiv.org/pdf/1810.01553.pdf

The idea of the algorithm is to build on top of an existing reader-writer mutex and introduce a fast path for readers. On the fast path, reader lock attempts are sharded over an internal array based on the reader identity (a token in case of Golang). This means that readers do not contend over a single atomic counter like it's done in, say, `sync.RWMutex` allowing for better scalability in terms of cores.

Hence, by the design `RBMutex` is a specialized mutex for scenarios, such as caches, where the vast majority of locks are acquired by readers and write lock acquire attempts are infrequent. In such scenarios, `RBMutex` should perform better than the `sync.RWMutex` on large multicore machines.

`RBMutex` extends `sync.RWMutex` internally and uses it as the "reader bias disabled" fallback, so the same semantics apply. The only noticeable difference is in the reader tokens returned from the `RLock`/`RUnlock` methods.

## License

Licensed under MIT.
