package xsync

import (
	"runtime"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

const (
	// number of reader slots
	rslots = 4096
	// slow-down guard
	nslowdown = 9
)

var tokenPool sync.Pool

// RToken is a reader token.
// TODO include the pointer
type RToken *struct {
	ptr *int32
}

// A RBMutex is a reader biased reader/writer mutual exclusion lock.
// The lock can be held by an arbitrary number of readers or a single writer.
// The zero value for a RBMutex is an unlocked mutex.
//
// Based on the BRAVO (Biased Locking for Reader-Writer Locks) algorithm:
// https://arxiv.org/pdf/1810.01553.pdf
//
// A RBMutex must not be copied after first use.
//
// If a goroutine holds a RBMutex for reading and another goroutine might
// call Lock, no goroutine should expect to be able to acquire a read lock
// until the initial read lock is released. In particular, this prohibits
// recursive read locking. This is to ensure that the lock eventually becomes
// available; a blocked Lock call excludes new readers from acquiring the
// lock.
type RBMutex struct {
	readers      [rslots]int32
	rbias        int32
	inhibitUntil time.Time
	rw           sync.RWMutex
}

// RLock locks m for reading.
//
// It should not be used for recursive read locking; a blocked Lock
// call excludes new readers from acquiring the lock. See the
// documentation on the RBMutex type.
func (m *RBMutex) RLock() RToken {
	if atomic.LoadInt32(&m.rbias) == 1 {
		t, ok := tokenPool.Get().(RToken)
		if !ok {
			t = new(struct {
				ptr *int32
			})
		}
		slot := hash(uintptr(unsafe.Pointer(t))) % rslots
		ptr := (*int32)(unsafe.Pointer(uintptr(unsafe.Pointer(&m.readers)) + uintptr(slot)*4))
		if atomic.CompareAndSwapInt32(ptr, 0, 1) {
			if atomic.LoadInt32(&m.rbias) == 1 {
				t.ptr = ptr
				return t
			}
			atomic.StoreInt32(ptr, 0)
		}
		tokenPool.Put(t)
	}
	m.rw.RLock()
	if atomic.LoadInt32(&m.rbias) == 0 && time.Now().After(m.inhibitUntil) {
		atomic.StoreInt32(&m.rbias, 1)
	}
	return nil
}

// RUnlock undoes a single RLock call;
// it does not affect other simultaneous readers.
// It is a run-time error if m is not locked for reading
// on entry to RUnlock.
func (m *RBMutex) RUnlock(t RToken) {
	if t == nil {
		m.rw.RUnlock()
		return
	}
	if !atomic.CompareAndSwapInt32(t.ptr, 1, 0) {
		panic("invalid reader state detected")
	}
	tokenPool.Put(t)
}

// Lock locks m for writing.
// If the lock is already locked for reading or writing,
// Lock blocks until the lock is available.
func (m *RBMutex) Lock() {
	m.rw.Lock()
	if atomic.LoadInt32(&m.rbias) == 1 {
		atomic.StoreInt32(&m.rbias, 0)
		start := time.Now()
		for i := 0; i < rslots; i++ {
			ptr := (*int32)(unsafe.Pointer(uintptr(unsafe.Pointer(&m.readers)) + uintptr(i)*4))
			for atomic.LoadInt32(ptr) == 1 {
				runtime.Gosched()
			}
		}
		m.inhibitUntil = time.Now().Add(time.Since(start) * nslowdown)
	}
}

// Unlock unlocks m for writing. It is a run-time error if m is
// not locked for writing on entry to Unlock.
//
// As with Mutexes, a locked RBMutex is not associated with a particular
// goroutine. One goroutine may RLock (Lock) a RBMutex and then
// arrange for another goroutine to RUnlock (Unlock) it.
func (m *RBMutex) Unlock() {
	m.rw.Unlock()
}

// TODO find better hash function for 32 and 64-bit integers
func hash(x uintptr) uintptr {
	x = ((x >> 16) ^ x) * 0x45d9f3b
	x = ((x >> 16) ^ x) * 0x45d9f3b
	x = (x >> 16) ^ x
	return x
}
