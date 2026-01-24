package xsync_test

import (
	"errors"
	"fmt"

	"github.com/puzpuzpuz/xsync/v4"
)

func ExampleMapOf_Compute() {
	counts := xsync.NewMap[int, int]()

	// Store a new value.
	v, ok := counts.Compute(42, func(oldValue int, loaded bool) (newValue int, op xsync.ComputeOp) {
		// loaded is false here.
		newValue = 42
		op = xsync.UpdateOp
		return
	})
	// v: 42, ok: true
	fmt.Printf("v: %v, ok: %v\n", v, ok)

	// Update an existing value.
	v, ok = counts.Compute(42, func(oldValue int, loaded bool) (newValue int, op xsync.ComputeOp) {
		// loaded is true here.
		newValue = oldValue + 42
		op = xsync.UpdateOp
		return
	})
	// v: 84, ok: true
	fmt.Printf("v: %v, ok: %v\n", v, ok)

	// Set a new value or keep the old value conditionally.
	var oldVal int
	minVal := 63
	v, ok = counts.Compute(42, func(oldValue int, loaded bool) (newValue int, op xsync.ComputeOp) {
		oldVal = oldValue
		if !loaded || oldValue < minVal {
			newValue = minVal
			op = xsync.UpdateOp
			return
		}
		// Here, the value is already greater than minVal, so instead of
		// updating the map, do nothing.
		op = xsync.CancelOp
		return
	})
	// v: 84, ok: true, oldVal: 84
	fmt.Printf("v: %v, ok: %v, oldVal: %v\n", v, ok, oldVal)

	// Delete an existing value.
	v, ok = counts.Compute(42, func(oldValue int, loaded bool) (newValue int, op xsync.ComputeOp) {
		// loaded is true here.
		op = xsync.DeleteOp
		return
	})
	// v: 84, ok: false
	fmt.Printf("v: %v, ok: %v\n", v, ok)

	// Propagate an error from the compute function to the outer scope.
	var err error
	v, ok = counts.Compute(42, func(oldValue int, loaded bool) (newValue int, op xsync.ComputeOp) {
		if oldValue == 42 {
			err = errors.New("something went wrong")
			return 0, xsync.CancelOp // no need to create a key/value pair
		}
		newValue = 0
		op = xsync.UpdateOp
		return
	})
	fmt.Printf("err: %v\n", err)
}

func ExampleMap_DeleteMatching() {
	m := xsync.NewMap[string, int]()
	m.Store("alice", 10)
	m.Store("bob", 20)
	m.Store("carol", 30)
	m.Store("dave", 40)

	// Delete entries with value greater than 25.
	deleted := m.DeleteMatching(func(key string, value int) (delete, stop bool) {
		return value > 25, false
	})
	fmt.Printf("deleted: %d\n", deleted)
	fmt.Printf("size: %d\n", m.Size())

	// Output:
	// deleted: 2
	// size: 2
}

func ExampleMap_Range() {
	m := xsync.NewMap[string, int]()
	m.Store("alice", 10)
	m.Store("bob", 20)
	m.Store("carol", 30)

	// Range iterates over all entries in the map.
	// It acquires bucket locks to ensure each key is visited at most once.
	sum := 0
	m.Range(func(key string, value int) bool {
		sum += value
		return true // continue iteration
	})
	fmt.Printf("sum: %d\n", sum)

	// Output:
	// sum: 60
}

func ExampleMap_RangeRelaxed() {
	m := xsync.NewMap[string, int]()
	m.Store("alice", 10)
	m.Store("bob", 20)
	m.Store("carol", 30)

	// RangeRelaxed is a faster, lock-free alternative to Range.
	// It does not acquire locks. However, the same key may be
	// visited more than once if it is concurrently deleted and
	// re-inserted during the iteration.
	sum := 0
	m.RangeRelaxed(func(key string, value int) bool {
		sum += value
		return true // continue iteration
	})
	fmt.Printf("sum: %d\n", sum)

	// Output:
	// sum: 60
}
