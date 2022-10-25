//go:build go1.18
// +build go1.18

package xsync_test

import (
	"encoding/binary"
	"hash/maphash"
	"time"

	"github.com/puzpuzpuz/xsync"
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
