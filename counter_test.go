package xsync_test

import (
	"runtime"
	"sync/atomic"
	"testing"

	. "github.com/puzpuzpuz/xsync/v2"
)

func TestCounterInc(t *testing.T) {
	c := NewCounter()
	for i := 0; i < 100; i++ {
		if v := c.Value(); v != int64(i) {
			t.Fatalf("got %v, want %d", v, i)
		}
		c.Inc()
	}
}

func TestCounterDec(t *testing.T) {
	c := NewCounter()
	for i := 0; i < 100; i++ {
		if v := c.Value(); v != int64(-i) {
			t.Fatalf("got %v, want %d", v, -i)
		}
		c.Dec()
	}
}

func TestCounterAdd(t *testing.T) {
	c := NewCounter()
	for i := 0; i < 100; i++ {
		if v := c.Value(); v != int64(i*42) {
			t.Fatalf("got %v, want %d", v, i*42)
		}
		c.Add(42)
	}
}

func TestCounterReset(t *testing.T) {
	c := NewCounter()
	c.Add(42)
	if v := c.Value(); v != 42 {
		t.Fatalf("got %v, want %d", v, 42)
	}
	c.Reset()
	if v := c.Value(); v != 0 {
		t.Fatalf("got %v, want %d", v, 0)
	}
}

func parallelIncrementor(c *Counter, numIncs int, cdone chan bool) {
	for i := 0; i < numIncs; i++ {
		c.Inc()
	}
	cdone <- true
}

func doTestParallelIncrementors(t *testing.T, numModifiers, gomaxprocs int) {
	runtime.GOMAXPROCS(gomaxprocs)
	c := NewCounter()
	cdone := make(chan bool)
	numIncs := 10_000
	for i := 0; i < numModifiers; i++ {
		go parallelIncrementor(c, numIncs, cdone)
	}
	// Wait for the goroutines to finish.
	for i := 0; i < numModifiers; i++ {
		<-cdone
	}
	expected := int64(numModifiers * numIncs)
	if v := c.Value(); v != expected {
		t.Fatalf("got %d, want %d", v, expected)
	}
}

func TestCounterParallelIncrementors(t *testing.T) {
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(-1))
	doTestParallelIncrementors(t, 4, 2)
	doTestParallelIncrementors(t, 16, 4)
	doTestParallelIncrementors(t, 64, 8)
}

func benchmarkCounter(b *testing.B, writeRatio int) {
	c := NewCounter()
	runParallel(b, func(pb *testing.PB) {
		foo := 0
		for pb.Next() {
			foo++
			if writeRatio > 0 && foo%writeRatio == 0 {
				c.Value()
			} else {
				c.Inc()
			}
		}
		_ = foo
	})
}

func BenchmarkCounter(b *testing.B) {
	benchmarkCounter(b, 10000)
}

func benchmarkAtomicInt64(b *testing.B, writeRatio int) {
	var c int64
	runParallel(b, func(pb *testing.PB) {
		foo := 0
		for pb.Next() {
			foo++
			if writeRatio > 0 && foo%writeRatio == 0 {
				atomic.LoadInt64(&c)
			} else {
				atomic.AddInt64(&c, 1)
			}
		}
		_ = foo
	})
}

func BenchmarkAtomicInt64(b *testing.B) {
	benchmarkAtomicInt64(b, 10000)
}
