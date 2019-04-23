package erldeser

import (
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/pipedrive/uncouch/erlterm"
	"github.com/pipedrive/uncouch/leakybucket"
)

// Termite is structure to hold recursive de-serialise Erlang term
// Perhaps it is indeed time I began to look at this whole matter of bantering
// more enthusiastically. After all, when one thinks about it, it is not such
// a foolish thing to indulge in - particularly if it is the case that in
// bantering lies the key to human warmth.
type Termite struct {
	T        erlterm.Term
	Children []*Termite
}

// ReadTermite is reading serialised Erlang term from stream
func (s *Scanner) ReadTermite() (*Termite, error) {
	var rootTermite Termite
	err := s.buildTermite(&rootTermite)
	if err != nil {
		slog.Error(err)
		return nil, err
	}
	return &rootTermite, nil
}

// buildTermite is recursive functiuon building Termite structure
func (s *Scanner) buildTermite(buildNode *Termite) error {
	t := leakybucket.GetTerm()
	// defer leakybucket.PutTerm(t)
	// Not sure we can release the term here, probably not
	s.Scan(t)
	buildNode.T = *t
	switch t.Term {
	case NewFloatExt:
	case SmallIntegerExt:
	case IntegerExt:
	case AtomExt:
	case NilExt:
	case StringExt:
	case BinaryExt:
	case SmallTupleExt:
		children := make([]*Termite, 0, 5)
		buildNode.Children = children
		for i := int64(0); i < t.IntegerValue; i++ {
			termite := new(Termite)
			temp := append(buildNode.Children, termite)
			buildNode.Children = temp
			err := s.buildTermite(termite)
			if err != nil {
				slog.Error(err)
				return err
			}
		}
	case ListExt:
		children := make([]*Termite, 0, 5)
		buildNode.Children = children
		for i := int64(0); i <= t.IntegerValue; i++ {
			termite := new(Termite)
			temp := append(buildNode.Children, termite)
			buildNode.Children = temp
			err := s.buildTermite(termite)
			if err != nil {
				slog.Error(err)
				return err
			}
		}
	default:
		err := fmt.Errorf("Unhandled term type %v", t.Term)
		slog.Error(err)
		return err
	}
	return nil
}

// String implemets Stringer for reading what's inside
func (t *Termite) String() string {
	var output strings.Builder
	formatTermite(t, &output, 0)
	return output.String()
}

// formatTermite is recursive helper for formatting complex structure
func formatTermite(t *Termite, output *strings.Builder, nestedLevel int) {
	// create nested level pad
	var pad string
	for i := 0; i < nestedLevel; i++ {
		pad = pad + "-"
	}
	pad = pad + fmt.Sprintf("%d", nestedLevel)
	switch t.T.Term {
	case NewFloatExt:
		output.WriteString(fmt.Sprintf("%s New float: %v\n", pad, t.T.FloatValue))
	case SmallIntegerExt:
		output.WriteString(fmt.Sprintf("%s Small int: %v\n", pad, t.T.IntegerValue))
	case IntegerExt:
		output.WriteString(fmt.Sprintf("%s Int: %v\n", pad, t.T.IntegerValue))
	case AtomExt:
		output.WriteString(fmt.Sprintf("%s Atom: %v\n", pad, string(t.T.Binary)))
	case SmallTupleExt:
		output.WriteString(fmt.Sprintf("%s Small tuple with count: %v\n", pad, t.T.IntegerValue))
		for i := int64(0); i < t.T.IntegerValue; i++ {
			formatTermite(t.Children[i], output, nestedLevel+1)
		}
	case NilExt:
		output.WriteString(fmt.Sprintf("%s Nil value\n", pad))
	case StringExt:
		output.WriteString(fmt.Sprintf("%s String value: %v\n", pad, t.T.Binary))
	case ListExt:
		output.WriteString(fmt.Sprintf("%s List with length: %v\n", pad, t.T.IntegerValue))
		for i := int64(0); i <= t.T.IntegerValue; i++ {
			formatTermite(t.Children[i], output, nestedLevel+1)
		}
	case BinaryExt:
		dst := make([]byte, hex.EncodedLen(len(t.T.Binary)))
		hex.Encode(dst, t.T.Binary)
		output.WriteString(fmt.Sprintf("%s Binary: %v / %v / %v\n", pad, string(dst), t.T.Binary, string(t.T.Binary)))
	default:
		output.WriteString(fmt.Sprintf("%s String can not handle %v \n", pad, t.T.Term))
	}
}

// Release releases used Terms back for reuse
func (t *Termite) Release() {
	releaseTermite(t)
	return
}

func releaseTermite(t *Termite) {
	switch t.T.Term {
	case NewFloatExt:
		leakybucket.PutTerm(&t.T)
	case SmallIntegerExt:
		leakybucket.PutTerm(&t.T)
	case IntegerExt:
		leakybucket.PutTerm(&t.T)
	case AtomExt:
		leakybucket.PutTerm(&t.T)
	case SmallTupleExt:
		for i := int64(0); i < t.T.IntegerValue; i++ {
			releaseTermite(t.Children[i])
		}
	case NilExt:
		leakybucket.PutTerm(&t.T)
	case StringExt:
		leakybucket.PutTerm(&t.T)
	case ListExt:
		for i := int64(0); i <= t.T.IntegerValue; i++ {
			releaseTermite(t.Children[i])
		}
	case BinaryExt:
		leakybucket.PutTerm(&t.T)
	default:
		slog.Errorf("Unhandled Term type in releaseTermite %v", t.T.Term)
	}
	return
}
