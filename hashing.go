//go:build go1.18
// +build go1.18

package xsync

import (
	"hash/maphash"
	"reflect"
	"unsafe"
)

// makeHashFunc creates a fast hash function for the given comparable type.
// The only limitation is that the type should not contain interfaces inside
func makeHashFunc[T comparable]() func(maphash.Seed, T) uint64 {
	var zero T
	rt := reflect.TypeOf(&zero).Elem() // Elem() avoids uninformative panics when T is interface

	blocks := hGetMemLayout(rt)

	// empty struct case, should never happen irl
	if len(blocks) == 0 {
		return func(seed maphash.Seed, v T) uint64 {
			return maphash.Bytes(seed, nil)
		}
	}

	// string or a struct with a single string field
	if len(blocks) == 1 && !blocks[0].IsString {
		block := blocks[0]
		return func(seed maphash.Seed, v T) uint64 {
			return maphash.Bytes(seed, asBytes(unsafe.Pointer(&v), block.Offset, block.Length))
		}
	}

	// data type is a single contiguous region of memory (e.g. struct with integer fields)
	if len(blocks) == 1 && blocks[0].IsString {
		block := blocks[0]
		return func(seed maphash.Seed, v T) uint64 {
			return maphash.String(seed, asString(unsafe.Pointer(&v), block.Offset))
		}
	}

	// complex data type consisting of multiple blocks
	{
		return func(seed maphash.Seed, v T) uint64 {
			var h maphash.Hash
			h.SetSeed(seed)
			ptr := unsafe.Pointer(&v)

			for _, b := range blocks {
				if b.IsString {
					h.WriteString(asString(ptr, b.Offset))
				} else {
					h.Write(asBytes(ptr, b.Offset, b.Length))
				}
			}
			return h.Sum64()
		}
	}
}

// asString interprets memory at p + offset as a string header and returns the string.
func asString(p unsafe.Pointer, offset uintptr) string {
	sp := unsafe.Add(p, offset)
	return *(*string)(sp)
}

// asBytes returns block of memory at p + offset as a byte slice.
func asBytes(p unsafe.Pointer, offset uintptr, length uintptr) []byte {
	sp := unsafe.Add(p, offset)

	// Fast approach: 4.5 ns/op for int hashing.
	// Dirty, because runtime thinks that returned slice hash 1Tb of capacity
	return (*[1 << 40]byte)(sp)[:length]

	// go 1.20 way: 4.5 ns/op for int hashing
	// Need to change go.mod
	//return unsafe.Slice((*byte)(sp), int(length))

	// Clean pre go 1.20 way: 12 ns/op for int hashing
	// Very slow compared to others
	//var res []byte
	//sh := (*reflect.SliceHeader)(unsafe.Pointer(&res))
	//sh.Data = uintptr(sp)
	//sh.Len = int(length)
	//sh.Cap = int(length)
	//return res
}

type hMemBlock struct {
	Offset   uintptr
	Length   uintptr
	IsString bool
}

// hGetMemLayout returns memory layout of the given type.
// It works for comparable types with any level of nesting as long as there's no interfaces anywhere inside.
// Each type can be described as a list of memory blocks, where each block is either a string or a plain block.
// For example:
// - Point3d(x,y,z) is a single plain block of size 24
// - User(id, name, age, height) is 3 blocks plain(id), string(name), plain(age, height)
// Type can consist of multiple blocks even when there's no strings, but when padding is involved.
func hGetMemLayout(t reflect.Type) []hMemBlock {
	if !t.Comparable() {
		// this filters out slices, maps, functions, etc
		panic("type is not comparable")
	}

	return getGetMemLayoutAcc(t, 0, nil)
}

func getGetMemLayoutAcc(t reflect.Type, offset uintptr, acc []hMemBlock) []hMemBlock {
	switch t.Kind() {
	case reflect.Struct:
		n := t.NumField()
		for i := 0; i < n; i++ {
			f := t.Field(i)
			if f.Name == "_" {
				continue
			}
			acc = getGetMemLayoutAcc(f.Type, offset+f.Offset, acc)
		}

	case reflect.Array:
		// Optional optimization for [n]byte.
		// It's possible to optimize for much wider range of array types, but it's better to keep code clear.
		// This code will be executed only once per type, and people rarely use large data types for map keys
		if t.Elem().Kind() == reflect.Uint8 {
			acc = appendHashMemBlock(acc, hMemBlock{Offset: offset, Length: uintptr(t.Len()), IsString: false})
			break
		}

		// General array case
		n := t.Len()
		elem := t.Elem()
		elemSize := elem.Size()
		currentOffset := offset
		for i := 0; i < n; i++ {
			acc = getGetMemLayoutAcc(elem, currentOffset, acc)
			currentOffset += elemSize
		}

	case reflect.Interface:
		// we only support types whose layout is fully known at compile time
		panic("interface types are not supported")

	case reflect.String:
		// For strings, we use special block type
		acc = appendHashMemBlock(acc, hMemBlock{Offset: offset, Length: t.Size(), IsString: true})

	default:
		// All other comparable types are just regular blocks
		acc = appendHashMemBlock(acc, hMemBlock{Offset: offset, Length: t.Size(), IsString: false})
	}
	return acc
}

// appendHashMemBlock appends block to acc, merging it with the last block if possible
func appendHashMemBlock(acc []hMemBlock, block hMemBlock) []hMemBlock {
	if len(acc) > 0 {
		last := &acc[len(acc)-1]
		if !last.IsString && !block.IsString && last.Offset+last.Length == block.Offset {
			// merge into last segment
			last.Length += block.Length
			return acc
		}
	}

	return append(acc, block)
}
