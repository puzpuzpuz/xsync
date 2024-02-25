package xsync_test

import (
	"errors"
	"fmt"

	"github.com/puzpuzpuz/xsync/v3"
)

func ExampleMapOf_Compute() {
	counts := xsync.NewMapOf[int, int]()

	// Store a new value.
	v, ok := counts.Compute(42, func(oldValue int, loaded bool) (newValue int, delete bool) {
		// loaded is false here.
		newValue = 42
		delete = false
		return
	})
	// v: 42, ok: true
	fmt.Printf("v: %v, ok: %v\n", v, ok)

	// Update an existing value.
	v, ok = counts.Compute(42, func(oldValue int, loaded bool) (newValue int, delete bool) {
		// loaded is true here.
		newValue = oldValue + 42
		delete = false
		return
	})
	// v: 84, ok: true
	fmt.Printf("v: %v, ok: %v\n", v, ok)

	// Set a new value or keep the old value conditionally.
	var oldVal int
	minVal := 63
	v, ok = counts.Compute(42, func(oldValue int, loaded bool) (newValue int, delete bool) {
		oldVal = oldValue
		if !loaded || oldValue < minVal {
			newValue = minVal
			delete = false
			return
		}
		newValue = oldValue
		delete = false
		return
	})
	// v: 84, ok: true, oldVal: 84
	fmt.Printf("v: %v, ok: %v, oldVal: %v\n", v, ok, oldVal)

	// Delete an existing value.
	v, ok = counts.Compute(42, func(oldValue int, loaded bool) (newValue int, delete bool) {
		// loaded is true here.
		delete = true
		return
	})
	// v: 84, ok: false
	fmt.Printf("v: %v, ok: %v\n", v, ok)

	// Propagate an error from the compute function to the outer scope.
	var err error
	v, ok = counts.Compute(42, func(oldValue int, loaded bool) (newValue int, delete bool) {
		if oldValue == 42 {
			err = errors.New("something went wrong")
			return 0, true // no need to create a key/value pair
		}
		newValue = 0
		delete = false
		return
	})
	fmt.Printf("err: %v\n", err)
}
