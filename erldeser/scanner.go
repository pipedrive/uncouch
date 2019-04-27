// Package erldeser provides routines to deserialise Erlang terms.
// It implements small subset of External Term Format http://erlang.org/doc/apps/erts/erl_ext_dist.html
package erldeser

import (
	"encoding/binary"
	"fmt"
	"math"

	"github.com/pipedrive/uncouch/erlterm"
)

// Actual values for different data types
const (
	NewFloatExt     erlterm.TermType = 'F'
	SmallIntegerExt erlterm.TermType = 'a'
	IntegerExt      erlterm.TermType = 'b'
	AtomExt         erlterm.TermType = 'd'
	SmallTupleExt   erlterm.TermType = 'h'
	LargeTupleExt   erlterm.TermType = 'i'
	NilExt          erlterm.TermType = 'j'
	StringExt       erlterm.TermType = 'k'
	ListExt         erlterm.TermType = 'l'
	BinaryExt       erlterm.TermType = 'm'
)

// Scanner implements term scanner from provided io.Reader
type Scanner struct {
	input  []byte
	offset int64
}

// New will return term scanner
func NewScanner(input []byte) (*Scanner, error) {
	var (
		newScanner Scanner
	)
	ns := &newScanner
	ns.input = input
	return ns, nil
}

// Scan scans provided input and return deserialised Erlang term
func (s *Scanner) Scan(t *erlterm.Term) error {
	if t == nil {
		err := fmt.Errorf("Provided term is nil reference")
		slog.Error(err)
		return err
	}
	termType := erlterm.TermType(s.input[s.offset])
	s.offset++
	switch termType {
	case NewFloatExt:
		s.readNewFloat(t)
	case SmallIntegerExt:
		s.readSmallInteger(t)
	case IntegerExt:
		s.readInteger(t)
	case AtomExt:
		s.readAtom(t)
	case SmallTupleExt:
		s.readSmallTuple(t)
	case NilExt:
		s.readNil(t)
	case StringExt:
		s.readString(t)
	case ListExt:
		s.readList(t)
	case BinaryExt:
		s.readBinary(t)
	default:
		err := fmt.Errorf("Unhandled term type %v", termType)
		slog.Error(err)
		return err
	}
	return nil
}

// Rewind resets offset to be able to scan same buffer again
func (s *Scanner) Rewind() {
	s.offset = 0
}

// readNewFloat is reading serialised Erlang small integer
func (s *Scanner) readNewFloat(t *erlterm.Term) {
	bits := binary.LittleEndian.Uint64(s.input[s.offset : s.offset+8])
	s.offset += 8
	floatValue := math.Float64frombits(bits)
	t.Term = NewFloatExt
	t.FloatValue = floatValue
	return
}

// readSmallInteger is reading serialised Erlang small integer
func (s *Scanner) readSmallInteger(t *erlterm.Term) {
	intValue := int64(s.input[s.offset])
	s.offset++
	t.Term = SmallIntegerExt
	t.IntegerValue = intValue
	return
}

// readInteger is reading serialised Erlang integer
func (s *Scanner) readInteger(t *erlterm.Term) {
	intValue := binary.BigEndian.Uint32(s.input[s.offset : s.offset+4])
	s.offset += 4
	t.Term = IntegerExt
	t.IntegerValue = int64(intValue)
	return
}

// readAtom is reading serialised Erlang atom
func (s *Scanner) readAtom(t *erlterm.Term) {
	t.Term = AtomExt
	atomLength := int64(binary.BigEndian.Uint16(s.input[s.offset : s.offset+2]))
	s.offset += 2
	if atomLength > int64(cap(t.Binary)) {
		t.Binary = make([]byte, atomLength)
	} else {
		t.Binary = t.Binary[:atomLength]
	}
	copy(t.Binary, s.input[s.offset:s.offset+atomLength])
	s.offset += atomLength
	return
}

// readSmallTuple is reading serialised Erlang small tuple
func (s *Scanner) readSmallTuple(t *erlterm.Term) {
	arity := int64(s.input[s.offset])
	s.offset++
	t.Term = SmallTupleExt
	t.IntegerValue = arity
	return
}

// readNil is reading serialised Erlang empty list
func (s *Scanner) readNil(t *erlterm.Term) {
	t.Term = NilExt
	return
}

// readString is reading serialised Erlang string
func (s *Scanner) readString(t *erlterm.Term) {
	t.Term = StringExt
	stringLength := int64(binary.BigEndian.Uint16(s.input[s.offset : s.offset+2]))
	s.offset += 2

	if stringLength > int64(cap(t.Binary)) {
		t.Binary = make([]byte, stringLength)
	} else {
		t.Binary = t.Binary[:stringLength]
	}

	copy(t.Binary, s.input[s.offset:s.offset+stringLength])
	s.offset += stringLength
	return
}

// readList is reading serialised Erlang list
func (s *Scanner) readList(t *erlterm.Term) {
	listLength := int64(binary.BigEndian.Uint32(s.input[s.offset : s.offset+4]))
	s.offset += 4

	t.Term = ListExt
	t.IntegerValue = listLength
	return
}

// readBinary is reading serialised Erlang binary
func (s *Scanner) readBinary(t *erlterm.Term) {
	t.Term = BinaryExt
	binaryLength := int64(binary.BigEndian.Uint32(s.input[s.offset : s.offset+4]))
	s.offset += 4

	if binaryLength > int64(cap(t.Binary)) {
		t.Binary = make([]byte, binaryLength)
	} else {
		t.Binary = t.Binary[:binaryLength]
	}

	copy(t.Binary, s.input[s.offset:s.offset+binaryLength])
	s.offset += binaryLength
	return
}
