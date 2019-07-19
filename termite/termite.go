// Package termite data structure to store compound Erlang terms (lists, structures)
// in recursuved ata structure
package termite

import (
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/pipedrive/uncouch/erldeser"
	"github.com/pipedrive/uncouch/erlterm"
)

// Termite is structure to hold recursive de-serialise Erlang term
type Termite struct {
	T             erlterm.Term
	Children      []*Termite
	usedTermPools []*[]*erlterm.Term
}

// Builder is root wrapper and buffer for building Termites fast
type Builder struct {
	s         *erldeser.Scanner
	termPools []*[]*erlterm.Term
	i, j      int
}

// NewBuilder will return new Termite builder
func NewBuilder() (*Builder, error) {
	var (
		newBuilder Builder
	)
	nb := &newBuilder
	nb.termPools = append(nb.termPools, GetTermPool())
	return nb, nil
}

// ReadTermite is reading serialised Erlang term from stream
func (b *Builder) ReadTermite(s *erldeser.Scanner) (*Termite, error) {
	b.s = s
	var rootTermite Termite
	err := b.buildTermite(&rootTermite)
	if err != nil {
		slog.Error(err)
		b.s = nil
		return nil, err
	}
	rootTermite.usedTermPools = b.termPools
	b.s = nil
	// Add Termpool buffers to the Termite before releasing them
	return &rootTermite, nil
}

// buildTermite is recursive functiuon building Termite structure
func (b *Builder) buildTermite(buildNode *Termite) error {
	t := b.GetTerm()
	b.s.Scan(t)
	buildNode.T = *t
	switch t.Term {
	case erldeser.NewFloatExt:
	case erldeser.SmallIntegerExt:
	case erldeser.IntegerExt:
	case erldeser.AtomExt:
	case erldeser.NilExt:
	case erldeser.StringExt:
	case erldeser.BinaryExt:
	case erldeser.SmallTupleExt:
		children := make([]*Termite, 0, 5)
		buildNode.Children = children
		for i := int64(0); i < t.IntegerValue; i++ {
			termite := new(Termite)
			temp := append(buildNode.Children, termite)
			buildNode.Children = temp
			err := b.buildTermite(termite)
			if err != nil {
				slog.Error(err)
				return err
			}
		}
	case erldeser.ListExt:
		children := make([]*Termite, 0, 5)
		buildNode.Children = children
		for i := int64(0); i <= t.IntegerValue; i++ {
			termite := new(Termite)
			temp := append(buildNode.Children, termite)
			buildNode.Children = temp
			err := b.buildTermite(termite)
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

// String implemets Stringer for reading what's inside the Termite
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
	case erldeser.NewFloatExt:
		output.WriteString(fmt.Sprintf("%s New float: %v\n", pad, t.T.FloatValue))
	case erldeser.SmallIntegerExt:
		output.WriteString(fmt.Sprintf("%s Small int: %v\n", pad, t.T.IntegerValue))
	case erldeser.IntegerExt:
		output.WriteString(fmt.Sprintf("%s Int: %v\n", pad, t.T.IntegerValue))
	case erldeser.AtomExt:
		output.WriteString(fmt.Sprintf("%s Atom: %v\n", pad, string(t.T.Binary)))
	case erldeser.SmallTupleExt:
		output.WriteString(fmt.Sprintf("%s Small tuple with count: %v\n", pad, t.T.IntegerValue))
		for i := int64(0); i < t.T.IntegerValue; i++ {
			formatTermite(t.Children[i], output, nestedLevel+1)
		}
	case erldeser.NilExt:
		output.WriteString(fmt.Sprintf("%s Nil value\n", pad))
	case erldeser.StringExt:
		output.WriteString(fmt.Sprintf("%s String value: %v\n", pad, t.T.Binary))
	case erldeser.ListExt:
		output.WriteString(fmt.Sprintf("%s List with length: %v\n", pad, t.T.IntegerValue))
		for i := int64(0); i <= t.T.IntegerValue; i++ {
			formatTermite(t.Children[i], output, nestedLevel+1)
		}
	case erldeser.BinaryExt:
		dst := make([]byte, hex.EncodedLen(len(t.T.Binary)))
		hex.Encode(dst, t.T.Binary)
		output.WriteString(fmt.Sprintf("%s Binary: %v / %v / %v\n", pad, string(dst), t.T.Binary, string(t.T.Binary)))
	default:
		output.WriteString(fmt.Sprintf("%s String can not handle %v \n", pad, t.T.Term))
	}
}

// Release releases used Terms back for reuse
func (t *Termite) Release() {
	for _, tp := range t.usedTermPools {
		PutTermPool(tp)
	}
	return
}
