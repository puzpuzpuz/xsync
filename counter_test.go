package xsync_test

import (
	"runtime"
	"testing"

	. "github.com/puzpuzpuz/xsync"
)

func TestCounterInc(t *testing.T) {
	var c Counter
	for i := 0; i < 100; i++ {
		if v := c.Value(); v != int64(i) {
			t.Errorf("got %v, want %d", v, i)
		}
		c.Inc()
	}
}

func TestCounterDec(t *testing.T) {
	var c Counter
	for i := 0; i < 100; i++ {
		if v := c.Value(); v != int64(-i) {
			t.Errorf("got %v, want %d", v, -i)
		}
		c.Dec()
	}
}

func TestCounterAdd(t *testing.T) {
	var c Counter
	for i := 0; i < 100; i++ {
		if v := c.Value(); v != int64(i*42) {
			t.Errorf("got %v, want %d", v, i*42)
		}
		c.Add(42)
	}
}

func TestCounterReset(t *testing.T) {
	var c Counter
	c.Add(42)
	if v := c.Value(); v != 42 {
		t.Errorf("got %v, want %d", v, 42)
	}
	c.Reset()
	if v := c.Value(); v != 0 {
		t.Errorf("got %v, want %d", v, 0)
	}
}

func parallelModifier(c *Counter, cdone chan bool) {
	for i := 0; i < 10000; i++ {
		c.Inc()
		c.Dec()
	}
	cdone <- true
}

func doTestParallelModifiers(t *testing.T, numModifiers, gomaxprocs int) {
	runtime.GOMAXPROCS(gomaxprocs)
	var c Counter
	cdone := make(chan bool)
	for i := 0; i < numModifiers; i++ {
		go parallelModifier(&c, cdone)
	}
	// Wait for the goroutines to finish.
	for i := 0; i < numModifiers; i++ {
		<-cdone
	}
	if v := c.Value(); v != 0 {
		t.Errorf("got %v, want %d", v, 0)
	}
}

func TestCounterParallelModifiers(t *testing.T) {
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(-1))
	doTestParallelModifiers(t, 4, 2)
	doTestParallelModifiers(t, 16, 4)
	doTestParallelModifiers(t, 64, 8)
}
