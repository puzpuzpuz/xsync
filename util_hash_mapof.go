//go:build go1.18
// +build go1.18

package xsync

import (
	"reflect"
	"unsafe"
)

// makeHasher creates a fast hash function for the given comparable type.
// The only limitation is that the type should not contain interfaces inside
// based on runtime.typehash.
func makeHasher[T comparable]() func(T, uint64) uint64 {
	var zero T

	if reflect.TypeOf(&zero).Elem().Kind() == reflect.Interface {
		return func(value T, seed uint64) uint64 {
			iValue := any(value)
			i := (*iface)(unsafe.Pointer(&iValue))
			return runtime_typehash64(i.typ, i.word, seed)
		}
	} else {
		var iZero any = zero
		i := (*iface)(unsafe.Pointer(&iZero))
		return func(value T, seed uint64) uint64 {
			return runtime_typehash64(i.typ, unsafe.Pointer(&value), seed)
		}
	}
}
