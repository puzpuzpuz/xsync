package xsync

import (
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// number of reader slots; must be a power of two
	rslots = 4096
	// slow-down guard
	nslowdown = 9
)

// pool for reader tokens
var rtokenPool sync.Pool

// RToken is a reader lock token.
type RToken struct {
	slot uint32
}

// A RBMutex is a reader biased reader/writer mutual exclusion lock.
// The lock can be held by an many readers or a single writer.
// The zero value for a RBMutex is an unlocked mutex.
//
// A RBMutex must not be copied after first use.
//
// RBMutex is based on the BRAVO (Biased Locking for Reader-Writer
// Locks) algorithm: https://arxiv.org/pdf/1810.01553.pdf
//
// RBMutex is a specialized mutex for scenarios, such as caches,
// where the vast majority of locks are acquired by readers and write
// lock acquire attempts are infrequent. In such scenarios, RBMutex
// performs better than the sync.RWMutex on large multicore machines.
//
// RBMutex extends sync.RWMutex internally and uses it as the "reader
// bias disabled" fallback, so the same semantics apply. The only
// noticeable difference is in reader tokens returned from the
// RLock/RUnlock methods.
type RBMutex struct {
	readers      [rslots]int32
	rbias        int32
	inhibitUntil time.Time
	rw           sync.RWMutex
}

// RLock locks m for reading and returns a reader token. The
// token must be used in the later RUnlock call.
//
// Should not be used for recursive read locking; a blocked Lock
// call excludes new readers from acquiring the lock.
func (m *RBMutex) RLock() *RToken {
	if atomic.LoadInt32(&m.rbias) == 1 {
		t, ok := rtokenPool.Get().(*RToken)
		if !ok {
			t = new(RToken)
			// Since rslots is a power of two, we can use & instead of %.
			t.slot = uint32(fastrand() & (rslots - 1))
		}
		if atomic.CompareAndSwapInt32(&m.readers[t.slot], 0, 1) {
			if atomic.LoadInt32(&m.rbias) == 1 {
				return t
			}
			atomic.StoreInt32(&m.readers[t.slot], 0)
		}
		rtokenPool.Put(t)
	}
	m.rw.RLock()
	if atomic.LoadInt32(&m.rbias) == 0 && time.Now().After(m.inhibitUntil) {
		atomic.StoreInt32(&m.rbias, 1)
	}
	return nil
}

// RUnlock undoes a single RLock call. A reader token obtained from
// the RLock call must be provided. RUnlock does not affect other
// simultaneous readers. A panic is raised if m is not locked for
// reading on entry to RUnlock.
func (m *RBMutex) RUnlock(t *RToken) {
	if t == nil {
		m.rw.RUnlock()
		return
	}
	if !atomic.CompareAndSwapInt32(&m.readers[t.slot], 1, 0) {
		panic("invalid reader state detected")
	}
	rtokenPool.Put(t)
}

// Lock locks m for writing. If the lock is already locked for
// reading or writing, Lock blocks until the lock is available.
func (m *RBMutex) Lock() {
	m.rw.Lock()
	if atomic.LoadInt32(&m.rbias) == 1 {
		atomic.StoreInt32(&m.rbias, 0)
		start := time.Now()
		for i := 0; i < rslots; i++ {
			for atomic.LoadInt32(&m.readers[i]) == 1 {
				runtime.Gosched()
			}
		}
		m.inhibitUntil = time.Now().Add(time.Since(start) * nslowdown)
	}
}

// Unlock unlocks m for writing. A panic is raised if m is not locked
// for writing on entry to Unlock.
//
// As with RWMutex, a locked RBMutex is not associated with a
// particular goroutine. One goroutine may RLock (Lock) a RBMutex and
// then arrange for another goroutine to RUnlock (Unlock) it.
func (m *RBMutex) Unlock() {
	m.rw.Unlock()
}
