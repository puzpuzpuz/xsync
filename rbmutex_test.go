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

	. "github.com/puzpuzpuz/xsync/v2"
)

func TestRBMutexSerialReader(t *testing.T) {
	var m RBMutex
	for i := 0; i < 10; i++ {
		tk := m.RLock()
		m.RUnlock(tk)
	}
}

func parallelReader(m *RBMutex, clocked, cunlock, cdone chan bool) {
	tk := m.RLock()
	clocked <- true
	<-cunlock
	m.RUnlock(tk)
	cdone <- true
}

func doTestParallelReaders(numReaders, gomaxprocs int) {
	runtime.GOMAXPROCS(gomaxprocs)
	var m RBMutex
	clocked := make(chan bool)
	cunlock := make(chan bool)
	cdone := make(chan bool)
	for i := 0; i < numReaders; i++ {
		go parallelReader(&m, clocked, cunlock, cdone)
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
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(-1))
	doTestParallelReaders(1, 4)
	doTestParallelReaders(3, 4)
	doTestParallelReaders(4, 2)
}

func reader(m *RBMutex, numIterations int, activity *int32, cdone chan bool) {
	for i := 0; i < numIterations; i++ {
		tk := m.RLock()
		n := atomic.AddInt32(activity, 1)
		if n < 1 || n >= 10000 {
			m.RUnlock(tk)
			panic(fmt.Sprintf("rlock(%d)\n", n))
		}
		for i := 0; i < 100; i++ {
		}
		atomic.AddInt32(activity, -1)
		m.RUnlock(tk)
	}
	cdone <- true
}

func writer(m *RBMutex, numIterations int, activity *int32, cdone chan bool) {
	for i := 0; i < numIterations; i++ {
		m.Lock()
		n := atomic.AddInt32(activity, 10000)
		if n != 10000 {
			m.Unlock()
			panic(fmt.Sprintf("wlock(%d)\n", n))
		}
		for i := 0; i < 100; i++ {
		}
		atomic.AddInt32(activity, -10000)
		m.Unlock()
	}
	cdone <- true
}

func hammerRBMutex(gomaxprocs, numReaders, numIterations int) {
	runtime.GOMAXPROCS(gomaxprocs)
	var (
		// Number of active readers + 10000 * number of active writers.
		activity int32
		m        RBMutex
	)
	cdone := make(chan bool)
	go writer(&m, numIterations, &activity, cdone)
	var i int
	for i = 0; i < numReaders/2; i++ {
		go reader(&m, numIterations, &activity, cdone)
	}
	go writer(&m, numIterations, &activity, cdone)
	for ; i < numReaders; i++ {
		go reader(&m, numIterations, &activity, cdone)
	}
	// Wait for the 2 writers and all readers to finish.
	for i := 0; i < 2+numReaders; i++ {
		<-cdone
	}
}

func TestRBMutex(t *testing.T) {
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(-1))
	n := 1000
	if testing.Short() {
		n = 5
	}
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

func benchmarkRBMutex(b *testing.B, localWork, writeRatio int) {
	var m RBMutex
	b.RunParallel(func(pb *testing.PB) {
		foo := 0
		for pb.Next() {
			foo++
			if writeRatio > 0 && foo%writeRatio == 0 {
				m.Lock()
				//lint:ignore SA2001 critical section is empty in this benchmark
				m.Unlock()
			} else {
				tk := m.RLock()
				for i := 0; i != localWork; i += 1 {
					foo *= 2
					foo /= 2
				}
				m.RUnlock(tk)
			}
		}
		_ = foo
	})
}

func BenchmarkRBMutexReadOnly(b *testing.B) {
	benchmarkRBMutex(b, 0, -1)
}

func BenchmarkRBMutexWrite10000(b *testing.B) {
	benchmarkRBMutex(b, 0, 10000)
}

func BenchmarkRBMutexWrite1000(b *testing.B) {
	benchmarkRBMutex(b, 0, 1000)
}

func BenchmarkRBMutexWrite100(b *testing.B) {
	benchmarkRBMutex(b, 0, 100)
}

func BenchmarkRBMutexWorkReadOnly(b *testing.B) {
	benchmarkRBMutex(b, 100, -1)
}

func BenchmarkRBMutexWorkWrite10000(b *testing.B) {
	benchmarkRBMutex(b, 100, 10000)
}

func BenchmarkRBMutexWorkWrite1000(b *testing.B) {
	benchmarkRBMutex(b, 100, 1000)
}

func BenchmarkRBMutexWorkWrite100(b *testing.B) {
	benchmarkRBMutex(b, 100, 100)
}

func benchmarkRWMutex(b *testing.B, localWork, writeRatio int) {
	var m sync.RWMutex
	b.RunParallel(func(pb *testing.PB) {
		foo := 0
		for pb.Next() {
			foo++
			if writeRatio > 0 && foo%writeRatio == 0 {
				m.Lock()
				//lint:ignore SA2001 critical section is empty in this benchmark
				m.Unlock()
			} else {
				m.RLock()
				for i := 0; i != localWork; i += 1 {
					foo *= 2
					foo /= 2
				}
				m.RUnlock()
			}
		}
		_ = foo
	})
}

func BenchmarkRWMutexReadOnly(b *testing.B) {
	benchmarkRWMutex(b, 0, -1)
}

func BenchmarkRWMutexWrite10000(b *testing.B) {
	benchmarkRWMutex(b, 0, 10000)
}

func BenchmarkRWMutexWrite1000(b *testing.B) {
	benchmarkRWMutex(b, 0, 1000)
}

func BenchmarkRWMutexWrite100(b *testing.B) {
	benchmarkRWMutex(b, 0, 100)
}

func BenchmarkRWMutexWorkReadOnly(b *testing.B) {
	benchmarkRWMutex(b, 100, -1)
}

func BenchmarkRWMutexWorkWrite10000(b *testing.B) {
	benchmarkRWMutex(b, 100, 10000)
}

func BenchmarkRWMutexWorkWrite1000(b *testing.B) {
	benchmarkRWMutex(b, 100, 1000)
}

func BenchmarkRWMutexWorkWrite100(b *testing.B) {
	benchmarkRWMutex(b, 100, 100)
}
