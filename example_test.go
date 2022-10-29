//go:build go1.18
// +build go1.18

package xsync_test

import (
	"encoding/binary"
	"fmt"
	"hash/maphash"
	"time"

	"github.com/puzpuzpuz/xsync/v2"
)

func ExampleNewTypedMapOf() {
	type Person struct {
		GivenName   string
		FamilyName  string
		YearOfBirth int16
	}
	age := xsync.NewTypedMapOf[Person, int](func(seed maphash.Seed, p Person) uint64 {
		var h maphash.Hash
		h.SetSeed(seed)
		h.WriteString(p.GivenName)
		hash := h.Sum64()
		h.Reset()
		h.WriteString(p.FamilyName)
		hash = 31*hash + h.Sum64()
		h.Reset()
		binary.Write(&h, binary.LittleEndian, p.YearOfBirth)
		return 31*hash + h.Sum64()
	})
	Y := time.Now().Year()
	age.Store(Person{"Ada", "Lovelace", 1815}, Y-1815)
	age.Store(Person{"Charles", "Babbage", 1791}, Y-1791)
}

func ExampleMapOf_Compute() {
	counts := xsync.NewIntegerMapOf[int, int]()

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
