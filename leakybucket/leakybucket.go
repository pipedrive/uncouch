// Package leakybucket implements leaky bucket recycling for byte slices
// to reduce burden on GC
package leakybucket

var freeList = make(chan *[]byte, 100)

// Get returns byte slice with cap at least the size provided
// and len == size.
// Get panics if it can not provide byte slice
func Get(size int32) (b *[]byte) {
	select {
	case b = <-freeList:
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

// Put tries to add provide slice to free list for reuse.
func Put(b *[]byte) {
	select {
	case freeList <- b:
		// Buffer on free list; nothing more to do.
	default:
		// Free list full, just carry on.
	}
}
