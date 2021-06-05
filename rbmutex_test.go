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

	. "github.com/puzpuzpuz/xsync"
)

func TestSerialReader(t *testing.T) {
	var m RBMutex
	tk := m.RLock()
	m.RUnlock(tk)
	tk = m.RLock()
	m.RUnlock(tk)
}

func parallelReader(m *RBMutex, clocked, cunlock, cdone chan bool) {
	t := m.RLock()
	clocked <- true
	<-cunlock
	m.RUnlock(t)
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

func TestParallelReaders(t *testing.T) {
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(-1))
	doTestParallelReaders(1, 4)
	doTestParallelReaders(3, 4)
	doTestParallelReaders(4, 2)
}

func reader(shm *RBMutex, num_iterations int, activity *int32, cdone chan bool) {
	for i := 0; i < num_iterations; i++ {
		t := shm.RLock()
		n := atomic.AddInt32(activity, 1)
		if n < 1 || n >= 10000 {
			shm.RUnlock(t)
			panic(fmt.Sprintf("wlock(%d)\n", n))
		}
		for i := 0; i < 100; i++ {
		}
		atomic.AddInt32(activity, -1)
		shm.RUnlock(t)
	}
	cdone <- true
}

func writer(shm *RBMutex, num_iterations int, activity *int32, cdone chan bool) {
	for i := 0; i < num_iterations; i++ {
		shm.Lock()
		n := atomic.AddInt32(activity, 10000)
		if n != 10000 {
			shm.Unlock()
			panic(fmt.Sprintf("wlock(%d)\n", n))
		}
		for i := 0; i < 100; i++ {
		}
		atomic.AddInt32(activity, -10000)
		shm.Unlock()
	}
	cdone <- true
}

func HammerRBMutex(gomaxprocs, numReaders, num_iterations int) {
	runtime.GOMAXPROCS(gomaxprocs)
	// Number of active readers + 10000 * number of active writers.
	var activity int32
	var m RBMutex
	cdone := make(chan bool)
	go writer(&m, num_iterations, &activity, cdone)
	var i int
	for i = 0; i < numReaders/2; i++ {
		go reader(&m, num_iterations, &activity, cdone)
	}
	go writer(&m, num_iterations, &activity, cdone)
	for ; i < numReaders; i++ {
		go reader(&m, num_iterations, &activity, cdone)
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
	HammerRBMutex(1, 1, n)
	HammerRBMutex(1, 3, n)
	HammerRBMutex(1, 10, n)
	HammerRBMutex(4, 1, n)
	HammerRBMutex(4, 3, n)
	HammerRBMutex(4, 10, n)
	HammerRBMutex(10, 1, n)
	HammerRBMutex(10, 3, n)
	HammerRBMutex(10, 10, n)
	HammerRBMutex(10, 5, n)
}

func benchmarkRBMutex(b *testing.B, localWork, writeRatio int) {
	var m RBMutex
	b.RunParallel(func(pb *testing.PB) {
		foo := 0
		for pb.Next() {
			foo++
			if writeRatio > 0 && foo%writeRatio == 0 {
				m.Lock()
				m.Unlock()
			} else {
				t := m.RLock()
				for i := 0; i != localWork; i += 1 {
					foo *= 2
					foo /= 2
				}
				m.RUnlock(t)
			}
		}
		_ = foo
	})
}

func BenchmarkRBMutexReadOnly(b *testing.B) {
	benchmarkRBMutex(b, 0, -1)
}

func BenchmarkRBMutexWrite1000(b *testing.B) {
	benchmarkRBMutex(b, 0, 1000)
}

func BenchmarkRBMutexWrite100(b *testing.B) {
	benchmarkRBMutex(b, 0, 100)
}

func BenchmarkRBMutexWrite10(b *testing.B) {
	benchmarkRBMutex(b, 0, 10)
}

func BenchmarkRBMutexWorkReadOnly(b *testing.B) {
	benchmarkRBMutex(b, 100, -1)
}

func BenchmarkRBMutexWorkWrite1000(b *testing.B) {
	benchmarkRBMutex(b, 100, 1000)
}

func BenchmarkRBMutexWorkWrite100(b *testing.B) {
	benchmarkRBMutex(b, 100, 100)
}

func BenchmarkRBMutexWorkWrite10(b *testing.B) {
	benchmarkRBMutex(b, 100, 10)
}

func benchmarkRWMutex(b *testing.B, localWork, writeRatio int) {
	var m sync.RWMutex
	b.RunParallel(func(pb *testing.PB) {
		foo := 0
		for pb.Next() {
			foo++
			if writeRatio > 0 && foo%writeRatio == 0 {
				m.Lock()
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

func BenchmarkRWMutexWrite1000(b *testing.B) {
	benchmarkRWMutex(b, 0, 1000)
}

func BenchmarkRWMutexWrite100(b *testing.B) {
	benchmarkRWMutex(b, 0, 100)
}

func BenchmarkRWMutexWrite10(b *testing.B) {
	benchmarkRWMutex(b, 0, 10)
}

func BenchmarkRWMutexWorkReadOnly(b *testing.B) {
	benchmarkRWMutex(b, 100, -1)
}

func BenchmarkRWMutexWorkWrite1000(b *testing.B) {
	benchmarkRWMutex(b, 100, 1000)
}

func BenchmarkRWMutexWorkWrite100(b *testing.B) {
	benchmarkRWMutex(b, 100, 100)
}

func BenchmarkRWMutexWorkWrite10(b *testing.B) {
	benchmarkRBMutex(b, 100, 10)
}
