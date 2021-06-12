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

`RBMutex` is a specialized mutex for scenarios, such as caches, where the vast majority of locks are acquired by readers and write lock acquire attempts are infrequent. In such scenarios, `RBMutex` should perform better than the sync.RWMutex on large multicore machines.

`RBMutex` extends `sync.RWMutex` internally and uses it as the "reader bias disabled" fallback, so the same semantics apply. The only noticeable difference is in reader tokens returned from the `RLock`/`RUnlock` methods.

### RBMutex vs. sync.RWMutex

The following results were obtained on a GCP e2-highcpu-32 VM with 32 vCPUs (Intel Haswell), 32 GB memory, Ubuntu 20.04, Go 1.16.5.

<figure>
  <img src="./images/read-only-no-work-chart.svg" alt="Reader locks only, no work in the critical section" />
  <figcaption>Reader locks only, no work in the critical section</figcaption>
</figure>

<br/><br/>

<figure>
  <img src="./images/read-only-work-chart.svg" alt="Reader locks only, a loop spin in the critical section" />
  <figcaption>Reader locks only, a loop spin in the critical section</figcaption>
</figure>

<br/><br/>

<figure>
  <img src="./images/write-10000-chart.svg" alt="Writer locks on each 10,000 iteration, both no work and a loop spin in the critical section" />
  <figcaption>Writer locks on each 10,000 iteration, both no work and a loop spin in the critical section</figcaption>
</figure>

## MPMCQueue

A `MPMCQeueue` is a bounded multi-producer multi-consumer concurrent queue.

```go
q := NewMPMCQueue(1024)
// producer inserts an item into the queue
q.Enqueue("hello world")
// consumer obtains an item from the queue
item := q.Dequeue()
```

Based on the algorithm from the [MPMCQueue](https://github.com/rigtorp/MPMCQueue) C++ library which in its turn references D.Vyukov's [MPMC queue](https://www.1024cores.net/home/lock-free-algorithms/queues/bounded-mpmc-queue).

### MPMCQueue vs. Go channels

```
BenchmarkQueueProdCons-8          	25243560	        52.01 ns/op
BenchmarkQueueProdConsWork100-8   	20317573	        58.89 ns/op
BenchmarkChanProdCons-8           	14297372	        81.96 ns/op
BenchmarkChanProdConsWork100-8    	 5424766	       230.8 ns/op
```

## License

Licensed under MIT.
