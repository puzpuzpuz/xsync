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
		YearOfBirth int
	}
	age := xsync.NewTypedMapOf[Person, int](func(seed maphash.Seed, p Person) uint64 {
		var h maphash.Hash
		h.SetSeed(seed)
		// We write field numbers, to make sure that "foo", "bar" and "foob", "ar" hash differently.
		h.WriteByte(0)
		h.WriteString(p.GivenName)
		h.WriteByte(1)
		h.WriteString(p.FamilyName)
		h.WriteByte(2)
		binary.Write(&h, binary.LittleEndian, p.YearOfBirth)
		return h.Sum64()
	})
	Y := time.Now().Year()
	age.Store(Person{"Ada", "Lovelace", 1815}, Y-1815)
	age.Store(Person{"Charles", "Babbage", 1791}, Y-1791)
}
