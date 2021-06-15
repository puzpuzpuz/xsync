# xsync

Concurrent data structures for Go. An extension for the standard `sync` package.

This library should be considered experimental, so make sure to run benchmarks for your use cases before starting to use it in your application.

## RBMutex

A `RBMutex` is a reader biased reader/writer mutual exclusion lock. The lock can be held by an many readers or a single writer.

```go
var m RBMutex
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

### RBMutex vs. sync.RWMutex

The following results were obtained on a GCP e2-highcpu-32 VM with 32 vCPUs (Intel Haswell), 32 GB memory, Ubuntu 20.04, Go 1.16.5.

<figure>
  <img src="./images/rb-mutex-read-only-no-work-chart.svg" alt="Reader locks only, no work in the critical section" />
  <figcaption>Reader locks only, no work in the critical section</figcaption>
</figure>

<br/><br/>

<figure>
  <img src="./images/rb-mutex-read-only-work-chart.svg" alt="Reader locks only, a loop spin in the critical section" />
  <figcaption>Reader locks only, some work in the critical section</figcaption>
</figure>

<br/><br/>

<figure>
  <img src="./images/rb-mutex-write-10000-chart.svg" alt="Writer locks on each 10,000 iteration, both no work and a loop spin in the critical section" />
  <figcaption>Writer locks on each 10,000 iteration, both no work and some work in the critical section</figcaption>
</figure>

## MPMCQueue

A `MPMCQeueue` is a bounded multi-producer multi-consumer concurrent queue.

```go
q := NewMPMCQueue(1024)
// producer inserts an item into the queue
q.Enqueue("foo")
// optimistic insertion attempt; doesn't block
inserted := q.TryEnqueue("bar")
// consumer obtains an item from the queue
item := q.Dequeue()
// optimistic obtain attempt; doesn't block
item, ok := q.Dequeue()
```

Based on the algorithm from the [MPMCQueue](https://github.com/rigtorp/MPMCQueue) C++ library which in its turn references D.Vyukov's [MPMC queue](https://www.1024cores.net/home/lock-free-algorithms/queues/bounded-mpmc-queue). According to the following [classification](https://www.1024cores.net/home/lock-free-algorithms/queues), the queue is array-based, fails on overflow, provides causal FIFO, has blocking producers and consumers.

The idea of the algorithm is to allow parallelism for concurrent producers and consumers by introducing the notion of tickets, i.e. values of two counters, one per producers/consumers. An atomic increment of one of those counters is the only noticeable contention point in queue operations. The rest of the operation avoids contention on writes thanks to the turn-based read/write access for each of the queue items.

In essence, `MPMCQueue` is a specialized queue for scenarios where there are multiple concurrent producers and consumers of a single queue running on a large multicore machine.

To get the optimal performance, you may want to set the queue size to be large enough, say, an order of magnitude greater than the number of producers/consumers, to allow producers and consumers to progress with their queue operations in parallel most of the time.

### MPMCQueue vs. Go channels

The following results were obtained on a GCP e2-highcpu-32 VM with 32 vCPUs (Intel Haswell), 32 GB memory, Ubuntu 20.04, Go 1.16.5.

<figure>
  <img src="./images/mpmcqueue-no-work-chart.svg" alt="Concurrent producers and consumers (1:1), queue/channel size 1,000, no work" />
  <figcaption>Concurrent producers and consumers (1:1), queue/channel size 1,000, no work</figcaption>
</figure>

<br/><br/>

<figure>
  <img src="./images/mpmcqueue-work-chart.svg" alt="Concurrent producers and consumers (1:1), queue/channel size 1,000, some work" />
  <figcaption>Concurrent producers and consumers (1:1), queue/channel size 1,000, some work</figcaption>
</figure>

## License

Licensed under MIT.
