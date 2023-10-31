//go:build go1.18
// +build go1.18

package xsync_test

import (
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
}
