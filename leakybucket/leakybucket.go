// Package leakybucket implements leaky bucket recycling for byte slices
// to reduce burden on GC
package leakybucket

import (
	"bytes"

	"github.com/pipedrive/uncouch/erlterm"
)

var (
	freeByteList   = make(chan *[]byte, 50)
	freeBufferList = make(chan *bytes.Buffer, 50)
	freeTermList   = make(chan *erlterm.Term, 1000)
)

// GetBytes returns byte slice with cap at least the size provided
// and len == size.
// GetBytes panics if it can not provide byte slice
func GetBytes(size int32) (b *[]byte) {
	select {
	case b = <-freeByteList:
		if int32(cap(*b)) < size {
			t := make([]byte, size)
			b = &t
		} else {
			t := (*b)[:size]
			b = &t
		}
	default:
		// None free, so allocate a new one.
		t := make([]byte, size)
		b = &t
	}
	return b
}

// PutBytes adds byte array to reuse list
func PutBytes(b *[]byte) {
	select {
	case freeByteList <- b:
		// Buffer on free list; nothing more to do.
	default:
		// Free list full, just carry on.
	}
}

// GetBuffer returns bytes.Buffer object, trying to reuse if possible
func GetBuffer() (b *bytes.Buffer) {
	select {
	case b = <-freeBufferList:
	default:
		presizeBytes := make([]byte, 0, 1024*1024*5)
		b = bytes.NewBuffer(presizeBytes)
	}
	return b
}

// PutBuffer adds bytes.Buffer object to reuse list
func PutBuffer(b *bytes.Buffer) {
	b.Reset()
	select {
	case freeBufferList <- b:
		// Buffer on free list; nothing more to do.
	default:
		// Free list full, just carry on.
	}
	return
}

// GetTerm returns Term object, trying to reuse if possible
func GetTerm() (t *erlterm.Term) {
	select {
	case t = <-freeTermList:
	default:
		t = new(erlterm.Term)
		presizedBinary := make([]byte, 0, 1024)
		t.Binary = presizedBinary
	}
	return t
}

// PutTerm adds Term object to reuse list
func PutTerm(t *erlterm.Term) {
	select {
	case freeTermList <- t:
		// Term on free list; nothing more to do.
	default:
		// Free list full, just carry on.
	}
	return
}
