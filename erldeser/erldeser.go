// Package erldeser provides routines to deserialise Erlang terms.
// It implements small subset of External Term Format http://erlang.org/doc/apps/erts/erl_ext_dist.html
package erldeser

import (
	"encoding/binary"
	"fmt"
	"io"
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
	input io.Reader
}

// New will return term scanner
func New(input io.Reader) (cf *Scanner, err error) {
	var (
		newScanner Scanner
	)
	ns := &newScanner
	ns.input = input
	return ns, nil
}

// Scan scans provided input and return deserialised Erlang term
func (s *Scanner) Scan() (*Term, error) {
	var termType TermType
	err := binary.Read(s.input, binary.BigEndian, &termType)
	if err != nil {
		slog.Error(err)
		return nil, err
	}
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
		err = fmt.Errorf("Unhandled term type %v", termType)
		return nil, err
	}
}

// readNewFloat is reading serialised Erlang small integer
func (s *Scanner) readNewFloat() (*Term, error) {
	var floatValue float64
	err := binary.Read(s.input, binary.BigEndian, &floatValue)
	if err != nil {
		slog.Error(err)
		return nil, err
	}
	var t Term
	t.Term = NewFloatExt
	t.FloatValue = floatValue
	return &t, nil
}

// readSmallInteger is reading serialised Erlang small integer
func (s *Scanner) readSmallInteger() (*Term, error) {
	var intValue uint8
	err := binary.Read(s.input, binary.BigEndian, &intValue)
	if err != nil {
		slog.Error(err)
		return nil, err
	}
	var t Term
	t.Term = SmallIntegerExt
	t.IntegerValue = int64(intValue)
	return &t, nil
}

// readInteger is reading serialised Erlang integer
func (s *Scanner) readInteger() (*Term, error) {
	var intValue uint32
	err := binary.Read(s.input, binary.BigEndian, &intValue)
	if err != nil {
		slog.Error(err)
		return nil, err
	}
	var t Term
	t.Term = IntegerExt
	t.IntegerValue = int64(intValue)
	return &t, nil
}

// readAtom is reading serialised Erlang atom
func (s *Scanner) readAtom() (*Term, error) {
	var atomLength uint16
	err := binary.Read(s.input, binary.BigEndian, &atomLength)
	if err != nil {
		slog.Error(err)
		return nil, err
	}
	atomName := make([]byte, atomLength)
	err = binary.Read(s.input, binary.BigEndian, &atomName)
	if err != nil {
		slog.Error(err)
		return nil, err
	}
	var t Term
	t.Term = AtomExt
	t.StringValue = string(atomName)
	return &t, nil
}

// readSmallTuple is reading serialised Erlang small tuple
func (s *Scanner) readSmallTuple() (*Term, error) {
	var arity uint8
	err := binary.Read(s.input, binary.BigEndian, &arity)
	if err != nil {
		slog.Error(err)
		return nil, err
	}
	var t Term
	t.Term = SmallTupleExt
	t.IntegerValue = int64(arity)
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
	var stringLength uint16
	err := binary.Read(s.input, binary.BigEndian, &stringLength)
	if err != nil {
		slog.Error(err)
		return nil, err
	}
	stringBytes := make([]byte, stringLength)
	err = binary.Read(s.input, binary.BigEndian, &stringBytes)
	if err != nil {
		slog.Error(err)
		return nil, err
	}
	var t Term
	t.Term = StringExt
	t.Binary = stringBytes
	return &t, nil
}

// readList is reading serialised Erlang list
func (s *Scanner) readList() (*Term, error) {
	var listLength uint32
	err := binary.Read(s.input, binary.BigEndian, &listLength)
	if err != nil {
		slog.Error(err)
		return nil, err
	}
	var t Term
	t.Term = ListExt
	t.IntegerValue = int64(listLength)
	return &t, nil
}

// readBinary is reading serialised Erlang binary
func (s *Scanner) readBinary() (*Term, error) {
	var binaryLength uint32
	err := binary.Read(s.input, binary.BigEndian, &binaryLength)
	if err != nil {
		slog.Error(err)
		return nil, err
	}
	binaryValue := make([]byte, binaryLength)
	binary.Read(s.input, binary.BigEndian, &binaryValue)

	var t Term
	t.Term = BinaryExt
	t.Binary = binaryValue
	return &t, nil
}
