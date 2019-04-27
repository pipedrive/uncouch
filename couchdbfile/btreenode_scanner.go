package couchdbfile

import (
	"fmt"

	"github.com/pipedrive/uncouch/erldeser"
	"github.com/pipedrive/uncouch/erlterm"
)

// readFromScanner reads node structure out of erldeser.Scanner
func (n *KpNodeID) readFromScanner(s *erldeser.Scanner) error {
	var t erlterm.Term
	t.Reset()
	err := checkTuple(s, &t, 2)
	if err != nil {
		slog.Error(err)
		return err
	}
	err = checkBinary(s, &t, "kp_node")
	if err != nil {
		slog.Error(err)
		return err
	}
	err = checkList(s, &t)
	if err != nil {
		slog.Error(err)
		return err
	}
	n.Length = int32(t.IntegerValue)
	// !!!!!
	// make!
	// !!!!!
	n.Pointers = make([]PointerID, n.Length)
	for i := int32(0); i < n.Length; i++ {
		err = checkTuple(s, &t, 2)
		if err != nil {
			slog.Error(err)
			return err
		}
		err = checkBinary(s, &t, "")
		if err != nil {
			slog.Error(err)
			return err
		}
		n.Pointers[i].Key = append([]byte(nil), t.Binary...)
		err = checkTuple(s, &t, 3)
		if err != nil {
			slog.Error(err)
			return err
		}
		err = checkInteger(s, &t)
		if err != nil {
			slog.Error(err)
			return err
		}
		n.Pointers[i].Offset = t.IntegerValue
		err = checkTuple(s, &t, 3)
		if err != nil {
			slog.Error(err)
			return err
		}
		err = checkInteger(s, &t)
		if err != nil {
			slog.Error(err)
			return err
		}
		n.Pointers[i].Count = t.IntegerValue
		err = checkInteger(s, &t)
		if err != nil {
			slog.Error(err)
			return err
		}
		n.Pointers[i].Count2 = t.IntegerValue
		err = skipTerms(s, &t, 4)
		if err != nil {
			slog.Error(err)
			return err
		}

		err = checkInteger(s, &t)
		if err != nil {
			slog.Error(err)
			return err
		}
		n.Pointers[i].Size = int32(t.IntegerValue)
	}
	return nil
}

// readFromScanner reads node structure out of erldeser.Scanner
func (n *KpNodeSeq) readFromScanner(s *erldeser.Scanner) error {
	var t erlterm.Term
	t.Reset()
	err := checkTuple(s, &t, 2)
	if err != nil {
		slog.Error(err)
		return err
	}
	err = checkBinary(s, &t, "kp_node")
	if err != nil {
		slog.Error(err)
		return err
	}
	err = checkList(s, &t)
	if err != nil {
		slog.Error(err)
		return err
	}
	n.Length = int32(t.IntegerValue)
	n.Pointers = make([]PointerSeq, n.Length)
	for i := int32(0); i < n.Length; i++ {
		err = checkTuple(s, &t, 2)
		if err != nil {
			slog.Error(err)
			return err
		}
		err = checkInteger(s, &t)
		if err != nil {
			slog.Error(err)
			return err
		}
		n.Pointers[i].Seq = t.IntegerValue
		err = checkTuple(s, &t, 3)
		if err != nil {
			slog.Error(err)
			return err
		}
		err = checkInteger(s, &t)
		if err != nil {
			slog.Error(err)
			return err
		}
		n.Pointers[i].Offset = t.IntegerValue
		err = checkInteger(s, &t)
		if err != nil {
			slog.Error(err)
			return err
		}
		n.Pointers[i].Size1 = t.IntegerValue
		err = checkInteger(s, &t)
		if err != nil {
			slog.Error(err)
			return err
		}
		n.Pointers[i].Size2 = t.IntegerValue
	}
	return nil
}

// readFromScanner reads node structure out of erldeser.Scanner
func (n *KvNode) readFromScanner(s *erldeser.Scanner) error {
	var t erlterm.Term
	t.Reset()
	err := checkTuple(s, &t, 2)
	if err != nil {
		slog.Error(err)
		return err
	}
	err = checkBinary(s, &t, "kv_node")
	if err != nil {
		slog.Error(err)
		return err
	}
	err = checkList(s, &t)
	if err != nil {
		slog.Error(err)
		return err
	}
	n.Length = int32(t.IntegerValue)
	n.Documents = make([]DocumentInfo, n.Length)
	for i := int32(0); i < n.Length; i++ {

	}
	return nil
}

func checkTuple(s *erldeser.Scanner, t *erlterm.Term, tupleSize int64) error {
	err := s.Scan(t)
	if err != nil {
		slog.Error(err)
		return err
	}
	if t.Term != erldeser.SmallTupleExt && t.IntegerValue != tupleSize {
		parsingError := fmt.Errorf("Expected SmallTuple with size %v, got %v, %v", tupleSize, t.Term, t.IntegerValue)
		slog.Error(parsingError)
		return parsingError
	}
	return nil
}

func checkBinary(s *erldeser.Scanner, t *erlterm.Term, binaryValue string) error {
	err := s.Scan(t)
	if err != nil {
		slog.Error(err)
		return err
	}
	if t.Term != erldeser.AtomExt {
		parsingError := fmt.Errorf("Expected Binary with value %v, got %v, %v", binaryValue, t.Term, t.IntegerValue)
		slog.Error(parsingError)
		return parsingError
	}
	if len(binaryValue) > 0 && string(t.Binary) != binaryValue {
		parsingError := fmt.Errorf("Expected Binary with value %v, got %v, %v", binaryValue, t.Term, t.IntegerValue)
		slog.Error(parsingError)
		return parsingError
	}
	return nil
}

func checkList(s *erldeser.Scanner, t *erlterm.Term) error {
	err := s.Scan(t)
	if err != nil {
		slog.Error(err)
		return err
	}
	if t.Term != erldeser.ListExt {
		parsingError := fmt.Errorf("Expected List, got %v", t.Term)
		slog.Error(parsingError)
		return parsingError
	}
	return nil
}

func checkInteger(s *erldeser.Scanner, t *erlterm.Term) error {
	err := s.Scan(t)
	if err != nil {
		slog.Error(err)
		return err
	}
	if !(t.Term == erldeser.IntegerExt || t.Term == erldeser.SmallIntegerExt) {
		parsingError := fmt.Errorf("Expected Integer, got %v", t.Term)
		slog.Error(parsingError)
		return parsingError
	}
	return nil
}
func skipTerms(s *erldeser.Scanner, t *erlterm.Term, i int) error {
	for i := 0; i < 4; i++ {
		err := s.Scan(t)
		if err != nil {
			slog.Error(err)
			return err
		}
	}
	return nil
}
