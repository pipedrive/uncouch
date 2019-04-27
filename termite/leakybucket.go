package termite

import "github.com/pipedrive/uncouch/erlterm"

const termPoolSize = 100

var (
	freeTermPoolList = make(chan *[]*erlterm.Term, 50)
)

// GetTermPool returns reusable Term pool in the hope to reduce GC stress
func GetTermPool() (tp *[]*erlterm.Term) {
	select {
	case tp = <-freeTermPoolList:
	default:
		newPool := make([]*erlterm.Term, termPoolSize)
		for i := range newPool {
			newPool[i] = new(erlterm.Term)
			newPool[i].Reset()
		}
		tp = &newPool
	}
	return tp
}

// PutTermPool adds reusable Term pool to reuse list
func PutTermPool(tp *[]*erlterm.Term) {
	for i := range *tp {
		(*tp)[i].Reset()
	}
	select {
	case freeTermPoolList <- tp:
		// Term on free list; nothing more to do.
	default:
		// Free list full, just carry on.
	}
	return
}

// GetTerm provides reusable term from the pool
func (b *Builder) GetTerm() (t *erlterm.Term) {
	if b.j >= termPoolSize {
		b.termPools = append(b.termPools, GetTermPool())
		b.i++
		b.j = 0
	}
	t = (*b.termPools[b.i])[b.j]
	b.j++
	return
}
