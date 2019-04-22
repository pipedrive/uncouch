// Package erldeser provides routines to deserialise Erlang terms.
// It implements small subset of External Term Format http://erlang.org/doc/apps/erts/erl_ext_dist.html
package erldeser

import (
	"encoding/binary"
	"fmt"
	"math"
)

// TermType is Erlanf data type tag used in serialisation
type TermType byte

// Term is structure to hold de-serialise Erlang term
type Term struct {
	Term         TermType
	IntegerValue int64
	StringValue  string
	FloatValue   float64
	Binary       []byte
}

// Actual values for different data types
const (
	NewFloatExt     TermType = 'F'
	SmallIntegerExt TermType = 'a'
	IntegerExt      TermType = 'b'
	AtomExt         TermType = 'd'
	SmallTupleExt   TermType = 'h'
	LargeTupleExt   TermType = 'i'
	NilExt          TermType = 'j'
	StringExt       TermType = 'k'
	ListExt         TermType = 'l'
	BinaryExt       TermType = 'm'
)

// Scanner implements term scanner from provided io.Reader
type Scanner struct {
	input  []byte
	offset int64
}

// New will return term scanner
func New(input []byte) (cf *Scanner, err error) {
	var (
		newScanner Scanner
	)
	ns := &newScanner
	ns.input = input
	return ns, nil
}

// Scan scans provided input and return deserialised Erlang term
func (s *Scanner) Scan() (*Term, error) {
	termType := TermType(s.input[s.offset])
	s.offset++
	switch termType {
	case NewFloatExt:
		return s.readNewFloat()
	case SmallIntegerExt:
		return s.readSmallInteger()
	case IntegerExt:
		return s.readInteger()
	case AtomExt:
		return s.readAtom()
	case SmallTupleExt:
		return s.readSmallTuple()
	case NilExt:
		return s.readNil()
	case StringExt:
		return s.readString()
	case ListExt:
		return s.readList()
	case BinaryExt:
		return s.readBinary()
	default:
		err := fmt.Errorf("Unhandled term type %v", termType)
		return nil, err
	}
}

// readNewFloat is reading serialised Erlang small integer
func (s *Scanner) readNewFloat() (*Term, error) {
	bits := binary.LittleEndian.Uint64(s.input[s.offset : s.offset+8])
	s.offset += 8
	floatValue := math.Float64frombits(bits)
	var t Term
	t.Term = NewFloatExt
	t.FloatValue = floatValue
	return &t, nil
}

// readSmallInteger is reading serialised Erlang small integer
func (s *Scanner) readSmallInteger() (*Term, error) {
	intValue := int64(s.input[s.offset])
	s.offset++
	var t Term
	t.Term = SmallIntegerExt
	t.IntegerValue = intValue
	return &t, nil
}

// readInteger is reading serialised Erlang integer
func (s *Scanner) readInteger() (*Term, error) {
	intValue := binary.BigEndian.Uint32(s.input[s.offset : s.offset+4])
	s.offset += 4
	var t Term
	t.Term = IntegerExt
	t.IntegerValue = int64(intValue)
	return &t, nil
}

// readAtom is reading serialised Erlang atom
func (s *Scanner) readAtom() (*Term, error) {
	atomLength := int64(binary.BigEndian.Uint16(s.input[s.offset : s.offset+2]))
	s.offset += 2
	atomName := make([]byte, atomLength)
	copy(atomName, s.input[s.offset:s.offset+atomLength])
	s.offset += atomLength
	var t Term
	t.Term = AtomExt
	t.StringValue = string(atomName)
	return &t, nil
}

// readSmallTuple is reading serialised Erlang small tuple
func (s *Scanner) readSmallTuple() (*Term, error) {
	arity := int64(s.input[s.offset])
	s.offset++
	var t Term
	t.Term = SmallTupleExt
	t.IntegerValue = arity
	return &t, nil
}

// readNil is reading serialised Erlang empty list
func (s *Scanner) readNil() (*Term, error) {
	var t Term
	t.Term = NilExt
	return &t, nil
}

// readString is reading serialised Erlang string
func (s *Scanner) readString() (*Term, error) {
	stringLength := int64(binary.BigEndian.Uint32(s.input[s.offset : s.offset+2]))
	s.offset += 2

	stringBytes := make([]byte, stringLength)
	copy(stringBytes, s.input[s.offset:s.offset+stringLength])
	s.offset += stringLength

	var t Term
	t.Term = StringExt
	t.Binary = stringBytes
	return &t, nil
}

// readList is reading serialised Erlang list
func (s *Scanner) readList() (*Term, error) {
	listLength := int64(binary.BigEndian.Uint32(s.input[s.offset : s.offset+4]))
	s.offset += 4

	var t Term
	t.Term = ListExt
	t.IntegerValue = listLength
	return &t, nil
}

// readBinary is reading serialised Erlang binary
func (s *Scanner) readBinary() (*Term, error) {
	binaryLength := int64(binary.BigEndian.Uint32(s.input[s.offset : s.offset+4]))
	s.offset += 4

	binaryBytes := make([]byte, binaryLength)
	copy(binaryBytes, s.input[s.offset:s.offset+binaryLength])
	s.offset += binaryLength

	var t Term
	t.Term = BinaryExt
	t.Binary = binaryBytes
	return &t, nil
}
