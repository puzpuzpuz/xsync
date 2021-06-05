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
BenchmarkRBMutexReadOnly-8        	212948089	         5.945 ns/op	       0 B/op	       0 allocs/op
BenchmarkRBMutexWrite1000-8       	36328396	        33.42 ns/op	       0 B/op	       0 allocs/op
BenchmarkRBMutexWrite100-8        	18402147	        65.85 ns/op	       0 B/op	       0 allocs/op
BenchmarkRBMutexWrite10-8         	16644835	        71.08 ns/op	       0 B/op	       0 allocs/op
BenchmarkRBMutexWorkReadOnly-8    	59884281	        20.01 ns/op	       0 B/op	       0 allocs/op
BenchmarkRBMutexWorkWrite1000-8   	20774445	        57.45 ns/op	       0 B/op	       0 allocs/op
BenchmarkRBMutexWorkWrite100-8    	 8182573	       146.9 ns/op	       0 B/op	       0 allocs/op
BenchmarkRBMutexWorkWrite10-8     	 6925441	       174.1 ns/op	       0 B/op	       0 allocs/op
BenchmarkRWMutexReadOnly-8        	31383505	        38.05 ns/op	       0 B/op	       0 allocs/op
BenchmarkRWMutexWrite1000-8       	38046286	        31.49 ns/op	       0 B/op	       0 allocs/op
BenchmarkRWMutexWrite100-8        	43489144	        27.57 ns/op	       0 B/op	       0 allocs/op
BenchmarkRWMutexWrite10-8         	60075478	        19.95 ns/op	       0 B/op	       0 allocs/op
BenchmarkRWMutexWorkReadOnly-8    	31480654	        37.88 ns/op	       0 B/op	       0 allocs/op
BenchmarkRWMutexWorkWrite1000-8   	23722382	        51.50 ns/op	       0 B/op	       0 allocs/op
BenchmarkRWMutexWorkWrite100-8    	11507290	       104.7 ns/op	       0 B/op	       0 allocs/op
BenchmarkRWMutexWorkWrite10-8     	 6894904	       173.8 ns/op	       0 B/op	       0 allocs/op
```

## License

Licensed under MIT.
