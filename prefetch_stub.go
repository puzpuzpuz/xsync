//go:build amd64 && !appengine && !noasm
// +build amd64,!appengine,!noasm

package xsync

import "unsafe"

//go:noescape
func T0(p unsafe.Pointer)

//go:noescape
func T1(p unsafe.Pointer)

//go:noescape
func T2(p unsafe.Pointer)

//go:noescape
func NTA(p unsafe.Pointer)
