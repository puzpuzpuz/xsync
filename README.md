[![GoDoc reference](https://img.shields.io/badge/godoc-reference-blue.svg)](https://pkg.go.dev/github.com/puzpuzpuz/xsync)
[![GoReport](https://goreportcard.com/badge/github.com/puzpuzpuz/xsync)](https://goreportcard.com/report/github.com/puzpuzpuz/xsync)

# xsync

Concurrent data structures for Go. An extension for the standard `sync` package.

This library should be considered experimental, so make sure to run tests and benchmarks for your use cases before adding it to your application.

*Important note*. Only 64-bit builds are officially supported at the moment. If you need to run a 32-bit build, make sure to test it and open a GH issue in case of any problems.

### Benchmarks

Benchmark results may be found [here](BENCHMARKS.md).

## Counter

A `Counter` is a striped int64 counter inspired by the j.u.c.a.LongAdder class from Java standard library.

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

A `Map` is like a concurrent hash table based map. It follows the interface of sync.Map.

```go
m := xsync.NewMap()
m.Store("foo", "bar")
v, ok := m.Load("foo")
```

`Map` uses a modified version of Cache-Line Hash Table (CLHT) data structure: https://github.com/LPD-EPFL/CLHT

CLHT is built around idea to organize the hash table in cache-line-sized buckets, so that on all modern CPUs update operations complete with at most one cache-line transfer. Also, Get operations involve no write to memory, as well as no mutexes or any other sort of locks. Due to this design, in all considered scenarios Map outperforms sync.Map.

One important difference with sync.Map is that only string keys are supported. That's because Golang standard library does not expose the built-in hash functions for `interface{}` values.

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
