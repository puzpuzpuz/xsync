# xsync

Concurrent data structures for Go. An extension for the standard `sync` package.

## RBMutex

A `RBMutex` is a reader biased reader/writer mutual exclusion lock.
The lock can be held by an many readers or a single writer.

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

`RBMutex` is based on the BRAVO (Biased Locking for Reader-Writer Locks)
algorithm: https://arxiv.org/pdf/1810.01553.pdf

`RBMutex` is a specialized mutex for scenarios, such as caches, where
the vast majority of locks are acquired by readers and write lock
acquire attempts are infrequent. In such scenarios, `RBMutex` performs
better than the sync.RWMutex on large multicore machines.

`RBMutex` extends `sync.RWMutex` internally and uses it as the "reader
bias disabled" fallback, so the same semantics apply. The only
noticeable difference is in reader tokens returned from the
`RLock`/`RUnlock` methods.

### RBMutex vs. sync.RWMutex

```
goos: linux
goarch: amd64
pkg: github.com/puzpuzpuz/xsync
cpu: Intel(R) Core(TM) i5-8300H CPU @ 2.30GHz
BenchmarkRBMutexReadOnly-8         	212651030	         5.917 ns/op
BenchmarkRBMutexWrite10000-8       	100000000	        10.35 ns/op
BenchmarkRBMutexWrite1000-8        	36253170	        32.56 ns/op
BenchmarkRBMutexWrite100-8         	18382934	        64.92 ns/op
BenchmarkRBMutexWorkReadOnly-8     	57881797	        20.72 ns/op
BenchmarkRBMutexWorkWrite10000-8   	40274763	        27.70 ns/op
BenchmarkRBMutexWorkWrite1000-8    	20605999	        57.56 ns/op
BenchmarkRBMutexWorkWrite100-8     	 8106751	       147.3 ns/op
BenchmarkRWMutexReadOnly-8         	40822070	        29.32 ns/op
BenchmarkRWMutexWrite10000-8       	40643188	        29.36 ns/op
BenchmarkRWMutexWrite1000-8        	45810360	        26.89 ns/op
BenchmarkRWMutexWrite100-8         	43401741	        26.95 ns/op
BenchmarkRWMutexWorkReadOnly-8     	40501375	        29.39 ns/op
BenchmarkRWMutexWorkWrite10000-8   	35851208	        32.52 ns/op
BenchmarkRWMutexWorkWrite1000-8    	24899078	        48.26 ns/op
BenchmarkRWMutexWorkWrite100-8     	11526358	       104.2 ns/op
```

## License

Licensed under MIT.
