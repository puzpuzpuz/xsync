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
		op = xsync.NoOp
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
			return 0, xsync.NoOp // no need to create a key/value pair
		}
		newValue = 0
		op = xsync.UpdateOp
		return
	})
	fmt.Printf("err: %v\n", err)
}
