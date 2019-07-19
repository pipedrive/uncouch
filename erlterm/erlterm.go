// Package erlterm provides data structure to store erlang Term in Go.
// Only small subset of Erlang terms is supported.
package erlterm

const defaultBinarySize = 256

// TermType is Erlanf data type tag used in serialisation
type TermType byte

// Term is structure to hold de-serialise Erlang term
type Term struct {
	Term         TermType
	IntegerValue int64
	FloatValue   float64
	Binary       []byte
}

// Reset resets content of the term and readies it for (re)use
func (t *Term) Reset() error {
	t.Term = 0
	if cap(t.Binary) < defaultBinarySize {
		presizedBinary := make([]byte, 0, defaultBinarySize)
		t.Binary = presizedBinary
	} else {
		t.Binary = t.Binary[:0]
	}
	return nil
}
