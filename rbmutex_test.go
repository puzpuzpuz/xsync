// Copyright notice. Initial version of the following tests was based on
// the following file from the Go Programming Language core repo:
// https://github.com/golang/go/blob/831f9376d8d730b16fb33dfd775618dffe13ce7a/src/sync/rwmutex_test.go

package xsync_test

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"

	. "github.com/puzpuzpuz/xsync/v3"
)

func TestRBMutexSerialReader(t *testing.T) {
	const numCalls = 10
	mu := NewRBMutex()
	for i := 0; i < 3; i++ {
		var rtokens [numCalls]*RToken
		for j := 0; j < numCalls; j++ {
			rtokens[j] = mu.RLock()
		}
		for j := 0; j < numCalls; j++ {
			mu.RUnlock(rtokens[j])
		}
	}
}

func TestRBMutexSerialOptimisticReader(t *testing.T) {
	const numCalls = 10
	mu := NewRBMutex()
	for i := 0; i < 3; i++ {
		var rtokens [numCalls]*RToken
		for j := 0; j < numCalls; j++ {
			ok, rt := mu.TryRLock()
			if !ok {
				t.Fatalf("TryRLock failed for %d", j)
			}
			if rt == nil {
				t.Fatalf("nil reader token for %d", j)
			}
			rtokens[j] = rt
		}
		for j := 0; j < numCalls; j++ {
			mu.RUnlock(rtokens[j])
		}
	}
}

func TestRBMutexSerialOptimisticWriter(t *testing.T) {
	mu := NewRBMutex()
	for i := 0; i < 3; i++ {
		if !mu.TryLock() {
			t.Fatal("TryLock failed")
		}
		mu.Unlock()
	}
}

func parallelReader(mu *RBMutex, clocked, cunlock, cdone chan bool) {
	t := mu.RLock()
	clocked <- true
	<-cunlock
	mu.RUnlock(t)
	cdone <- true
}

func doTestParallelReaders(numReaders, gomaxprocs int) {
	runtime.GOMAXPROCS(gomaxprocs)
	mu := NewRBMutex()
	clocked := make(chan bool)
	cunlock := make(chan bool)
	cdone := make(chan bool)
	for i := 0; i < numReaders; i++ {
		go parallelReader(mu, clocked, cunlock, cdone)
	}
	// Wait for all parallel RLock()s to succeed.
	for i := 0; i < numReaders; i++ {
		<-clocked
	}
	for i := 0; i < numReaders; i++ {
		cunlock <- true
	}
	// Wait for the goroutines to finish.
	for i := 0; i < numReaders; i++ {
		<-cdone
	}
}

func TestRBMutexParallelReaders(t *testing.T) {
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(0))
	doTestParallelReaders(1, 4)
	doTestParallelReaders(3, 4)
	doTestParallelReaders(4, 2)
}

func reader(mu *RBMutex, numIterations int, activity *int32, cdone chan bool) {
	for i := 0; i < numIterations; i++ {
		t := mu.RLock()
		n := atomic.AddInt32(activity, 1)
		if n < 1 || n >= 10000 {
			mu.RUnlock(t)
			panic(fmt.Sprintf("rlock(%d)\n", n))
		}
		for i := 0; i < 100; i++ {
		}
		atomic.AddInt32(activity, -1)
		mu.RUnlock(t)
	}
	cdone <- true
}

func writer(mu *RBMutex, numIterations int, activity *int32, cdone chan bool) {
	for i := 0; i < numIterations; i++ {
		mu.Lock()
		n := atomic.AddInt32(activity, 10000)
		if n != 10000 {
			mu.Unlock()
			panic(fmt.Sprintf("wlock(%d)\n", n))
		}
		for i := 0; i < 100; i++ {
		}
		atomic.AddInt32(activity, -10000)
		mu.Unlock()
	}
	cdone <- true
}

func hammerRBMutex(gomaxprocs, numReaders, numIterations int) {
	runtime.GOMAXPROCS(gomaxprocs)
	// Number of active readers + 10000 * number of active writers.
	var activity int32
	mu := NewRBMutex()
	cdone := make(chan bool)
	go writer(mu, numIterations, &activity, cdone)
	var i int
	for i = 0; i < numReaders/2; i++ {
		go reader(mu, numIterations, &activity, cdone)
	}
	go writer(mu, numIterations, &activity, cdone)
	for ; i < numReaders; i++ {
		go reader(mu, numIterations, &activity, cdone)
	}
	// Wait for the 2 writers and all readers to finish.
	for i := 0; i < 2+numReaders; i++ {
		<-cdone
	}
}

func TestRBMutex(t *testing.T) {
	const n = 1000
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(0))
	hammerRBMutex(1, 1, n)
	hammerRBMutex(1, 3, n)
	hammerRBMutex(1, 10, n)
	hammerRBMutex(4, 1, n)
	hammerRBMutex(4, 3, n)
	hammerRBMutex(4, 10, n)
	hammerRBMutex(10, 1, n)
	hammerRBMutex(10, 3, n)
	hammerRBMutex(10, 10, n)
	hammerRBMutex(10, 5, n)
}

func optimisticReader(mu *RBMutex, numIterations int, activity *int32, cdone chan bool) {
	for i := 0; i < numIterations; i++ {
		if ok, t := mu.TryRLock(); ok {
			n := atomic.AddInt32(activity, 1)
			if n < 1 || n >= 10000 {
				mu.RUnlock(t)
				panic(fmt.Sprintf("rlock(%d)\n", n))
			}
			for i := 0; i < 100; i++ {
			}
			atomic.AddInt32(activity, -1)
			mu.RUnlock(t)
		}
	}
	cdone <- true
}

func optimisticWriter(mu *RBMutex, numIterations int, activity *int32, cdone chan bool) {
	for i := 0; i < numIterations; i++ {
		if mu.TryLock() {
			n := atomic.AddInt32(activity, 10000)
			if n != 10000 {
				mu.Unlock()
				panic(fmt.Sprintf("wlock(%d)\n", n))
			}
			for i := 0; i < 100; i++ {
			}
			atomic.AddInt32(activity, -10000)
			mu.Unlock()
		}
	}
	cdone <- true
}

func hammerOptimisticRBMutex(gomaxprocs, numReaders, numIterations int) {
	runtime.GOMAXPROCS(gomaxprocs)
	// Number of active readers + 10000 * number of active writers.
	var activity int32
	mu := NewRBMutex()
	cdone := make(chan bool)
	go optimisticWriter(mu, numIterations, &activity, cdone)
	var i int
	for i = 0; i < numReaders/2; i++ {
		go optimisticReader(mu, numIterations, &activity, cdone)
	}
	go optimisticWriter(mu, numIterations, &activity, cdone)
	for ; i < numReaders; i++ {
		go optimisticReader(mu, numIterations, &activity, cdone)
	}
	// Wait for the 2 writers and all readers to finish.
	for i := 0; i < 2+numReaders; i++ {
		<-cdone
	}
}

func TestRBMutex_Optimistic(t *testing.T) {
	const n = 1000
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(0))
	hammerOptimisticRBMutex(1, 1, n)
	hammerOptimisticRBMutex(1, 3, n)
	hammerOptimisticRBMutex(1, 10, n)
	hammerOptimisticRBMutex(4, 1, n)
	hammerOptimisticRBMutex(4, 3, n)
	hammerOptimisticRBMutex(4, 10, n)
	hammerOptimisticRBMutex(10, 1, n)
	hammerOptimisticRBMutex(10, 3, n)
	hammerOptimisticRBMutex(10, 10, n)
	hammerOptimisticRBMutex(10, 5, n)
}

func hammerMixedRBMutex(gomaxprocs, numReaders, numIterations int) {
	runtime.GOMAXPROCS(gomaxprocs)
	// Number of active readers + 10000 * number of active writers.
	var activity int32
	mu := NewRBMutex()
	cdone := make(chan bool)
	go writer(mu, numIterations, &activity, cdone)
	var i int
	for i = 0; i < numReaders/2; i++ {
		go reader(mu, numIterations, &activity, cdone)
	}
	go optimisticWriter(mu, numIterations, &activity, cdone)
	for ; i < numReaders; i++ {
		go optimisticReader(mu, numIterations, &activity, cdone)
	}
	// Wait for the 2 writers and all readers to finish.
	for i := 0; i < 2+numReaders; i++ {
		<-cdone
	}
}

func TestRBMutex_Mixed(t *testing.T) {
	const n = 1000
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(0))
	hammerMixedRBMutex(1, 1, n)
	hammerMixedRBMutex(1, 3, n)
	hammerMixedRBMutex(1, 10, n)
	hammerMixedRBMutex(4, 1, n)
	hammerMixedRBMutex(4, 3, n)
	hammerMixedRBMutex(4, 10, n)
	hammerMixedRBMutex(10, 1, n)
	hammerMixedRBMutex(10, 3, n)
	hammerMixedRBMutex(10, 10, n)
	hammerMixedRBMutex(10, 5, n)
}

func benchmarkRBMutex(b *testing.B, parallelism, localWork, writeRatio int) {
	mu := NewRBMutex()
	b.SetParallelism(parallelism)
	runParallel(b, func(pb *testing.PB) {
		foo := 0
		for pb.Next() {
			foo++
			if writeRatio > 0 && foo%writeRatio == 0 {
				mu.Lock()
				for i := 0; i != localWork; i += 1 {
					foo *= 2
					foo /= 2
				}
				mu.Unlock()
			} else {
				tk := mu.RLock()
				for i := 0; i != localWork; i += 1 {
					foo *= 2
					foo /= 2
				}
				mu.RUnlock(tk)
			}
		}
		_ = foo
	})
}

func BenchmarkRBMutexWorkReadOnly_HighParallelism(b *testing.B) {
	benchmarkRBMutex(b, 1024, 100, -1)
}

func BenchmarkRBMutexWorkReadOnly(b *testing.B) {
	benchmarkRBMutex(b, -1, 100, -1)
}

func BenchmarkRBMutexWorkWrite100000(b *testing.B) {
	benchmarkRBMutex(b, -1, 100, 100000)
}

func BenchmarkRBMutexWorkWrite1000(b *testing.B) {
	benchmarkRBMutex(b, -1, 100, 1000)
}

func benchmarkRWMutex(b *testing.B, parallelism, localWork, writeRatio int) {
	var mu sync.RWMutex
	b.SetParallelism(parallelism)
	runParallel(b, func(pb *testing.PB) {
		foo := 0
		for pb.Next() {
			foo++
			if writeRatio > 0 && foo%writeRatio == 0 {
				mu.Lock()
				for i := 0; i != localWork; i += 1 {
					foo *= 2
					foo /= 2
				}
				mu.Unlock()
			} else {
				mu.RLock()
				for i := 0; i != localWork; i += 1 {
					foo *= 2
					foo /= 2
				}
				mu.RUnlock()
			}
		}
		_ = foo
	})
}

func BenchmarkRWMutexWorkReadOnly_HighParallelism(b *testing.B) {
	benchmarkRWMutex(b, 1024, 100, -1)
}

func BenchmarkRWMutexWorkReadOnly(b *testing.B) {
	benchmarkRWMutex(b, -1, 100, -1)
}

func BenchmarkRWMutexWorkWrite100000(b *testing.B) {
	benchmarkRWMutex(b, -1, 100, 100000)
}

func BenchmarkRWMutexWorkWrite1000(b *testing.B) {
	benchmarkRWMutex(b, -1, 100, 1000)
}
