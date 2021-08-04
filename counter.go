package xsync

import (
	"sync"
	"sync/atomic"
	"unsafe"
)

// number of counter stripes
const cstripes = 32

// pool for P tokens
var ptokenPool sync.Pool

// a P token is used to point at the current OS thread (P)
// on which the goroutine is run; exact identity of the thread,
// as well as P migration tolerance, is not important since
// it's used to as a best effort mechanism for assigning
// concurrent operations (goroutines) to different stripes of
// the counter
type ptoken struct {
	ptr *int64
}

// A Counter is a striped int64 counter.
//
// Should be preferred over a single atomically updated int64
// counter in high contention scenarios.
//
// A Counter must not be copied after first use.
type Counter struct {
	counters [cstripes]counter
}

type counter struct {
	c   int64
	pad [cacheLineSize - 8]byte
}

// Inc increments the counter by 1.
func (c *Counter) Inc() {
	c.Add(1)
}

// Dec decrements the counter by 1.
func (c *Counter) Dec() {
	c.Add(-1)
}

// Add adds the delta to the counter.
func (c *Counter) Add(delta int64) {
	t, ok := ptokenPool.Get().(*ptoken)
	if !ok {
		t = &ptoken{}
		idx := int(hash64(uintptr(unsafe.Pointer(t))) % cstripes)
		t.ptr = c.counterPtr(idx)
	}
	atomic.AddInt64(t.ptr, delta)
	rtokenPool.Put(t)
}

// Value returns the current counter value.
// The returned value may not include all of the latest operations in
// presence of concurrent modifications of the counter.
func (c *Counter) Value() int64 {
	v := int64(0)
	for i := 0; i < cstripes; i++ {
		v += atomic.LoadInt64(c.counterPtr(i))
	}
	return v
}

// Reset resets the counter to zero.
// This method should only be used when it is known that there are
// no concurrent modifications of the counter.
func (c *Counter) Reset() {
	for i := 0; i < cstripes; i++ {
		atomic.StoreInt64(c.counterPtr(i), 0)
	}
}

func (c *Counter) counterPtr(idx int) *int64 {
	return (*int64)(unsafe.Pointer(uintptr(unsafe.Pointer(&c.counters)) + uintptr(idx)*unsafe.Sizeof(counter{})))
}
