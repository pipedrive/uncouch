package termite

import (
	"fmt"

	"github.com/pipedrive/uncouch/erlterm"
)

const termPoolSize = 300

var (
	freeTermPoolList                                                                       = make(chan *[]*erlterm.Term, 50)
	putPoolSuccess, putPoolFailure, getPoolSuccess, getPoolFailure, termPoolListIncrements int64
)

// GetProfilerData emits primitive profiler data bout our pool caching
func GetProfilerData() string {
	return fmt.Sprintf("putPoolSuccess %v, putPoolFailure %v, getPoolSuccess %v, getPoolFailure %v, termPoolListIncrements %v",
		putPoolSuccess, putPoolFailure, getPoolSuccess, getPoolFailure, termPoolListIncrements)
}

// GetTermPool returns reusable Term pool in the hope to reduce GC stress
func GetTermPool() (tp *[]*erlterm.Term) {
	select {
	case tp = <-freeTermPoolList:
		getPoolSuccess++
	default:
		getPoolFailure++
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
		putPoolSuccess++
		// Term on free list; nothing more to do.
	default:
		putPoolFailure++
		// Free list full, just carry on.
	}
	return
}

// GetTerm provides reusable term from the pool
func (b *Builder) GetTerm() (t *erlterm.Term) {
	if b.j >= termPoolSize {
		b.termPools = append(b.termPools, GetTermPool())
		termPoolListIncrements++
		b.i++
		b.j = 0
	}
	t = (*b.termPools[b.i])[b.j]
	b.j++
	return
}
