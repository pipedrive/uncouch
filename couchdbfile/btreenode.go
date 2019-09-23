package couchdbfile

import (
	"github.com/pipedrive/uncouch/erldeser"
	"github.com/pipedrive/uncouch/termite"
)

// KpNodeID is a subset of data in CouchDB Btree node we need for data extraction
type KpNodeID struct {
	Length   int32
	Pointers []PointerID
}

// PointerID is a subset of data in CouchDB Btree node we need for data extraction
type PointerID struct {
	Key    []byte
	Offset int64
	Count  int64
	Count2 int64
	Size   int32
}

// KpNodeSeq is a subset of data in CouchDB Btree node we need for data extraction
type KpNodeSeq struct {
	Length   int32
	Pointers []PointerSeq
}

// PointerSeq is a subset of data in CouchDB Btree node we need for data extraction
type PointerSeq struct {
	Seq    int64
	Offset int64
	Size1  int64
	Size2  int64
}

// KvNode is a subset of data in CouchDB Btree node we need for data extraction
type KvNode struct {
	Length    int32
	Documents []DocumentInfo
}

// DocumentInfo is a subset of data in CouchDB Btree node we need for data extraction
type DocumentInfo struct {
	ID        []byte
	UpdateSeq int64
	Deleted   int8
	Size1     int32
	Size2     int32
	Revisions []Revision
}

// Revision is a subset of data in CouchDB Btree node we need for data extraction
type Revision struct {
	RevID     []byte
	Offset    int64
	UpdateSeq int64
	Deleted   int8
	Size1     int32
	Size2     int32
}

// readFromTermite reads node structure out of erldeser.Termite structure
func (n *KpNodeID) readFromTermite(t *termite.Termite) error {
	n.Length = int32(t.Children[1].T.IntegerValue)
	n.Pointers = make([]PointerID, n.Length)

	for i := int32(0); i < n.Length; i++ {
		t1 := t.Children[1].Children[i]
		// slog.Debug(t)
		n.Pointers[i].Key = append([]byte(nil), t1.Children[0].T.Binary...)
		t2 := t1.Children[1]
		n.Pointers[i].Offset = t2.Children[0].T.IntegerValue
		n.Pointers[i].Count = t2.Children[1].Children[0].T.IntegerValue
		n.Pointers[i].Count2 = t2.Children[1].Children[1].T.IntegerValue
		n.Pointers[i].Size = int32(t2.Children[2].T.IntegerValue)
	}
	return nil
}

// readFromTermite reads node structure out of erldeser.Termite structure
func (n *KpNodeSeq) readFromTermite(t *termite.Termite) error {
	n.Length = int32(t.Children[1].T.IntegerValue)
	n.Pointers = make([]PointerSeq, n.Length)

	for i := int32(0); i < n.Length; i++ {
		t1 := t.Children[1].Children[i]
		// slog.Debug(t)
		n.Pointers[i].Seq = t1.Children[0].T.IntegerValue
		t2 := t1.Children[1]
		n.Pointers[i].Offset = t2.Children[0].T.IntegerValue
		n.Pointers[i].Size1 = t2.Children[1].T.IntegerValue
		n.Pointers[i].Size2 = t2.Children[2].T.IntegerValue
	}
	return nil
}

// readFromTermite reads node structure out of erldeser.Termite structure
func (n *KvNode) readFromTermite(t *termite.Termite) error {
	n.Length = int32(t.Children[1].T.IntegerValue)
	n.Documents = make([]DocumentInfo, n.Length)

	for i := int32(0); i < n.Length; i++ {
		t1 := t.Children[1].Children[i]
		n.Documents[i].ID = append([]byte(nil), t1.Children[1].Children[0].T.Binary...)
		n.Documents[i].UpdateSeq = t1.Children[1].Children[0].T.IntegerValue
		n.Documents[i].Deleted = int8(t1.Children[1].Children[1].T.IntegerValue)
		n.Documents[i].Size1 = int32(t1.Children[1].Children[2].Children[0].T.IntegerValue)
		n.Documents[i].Size2 = int32(t1.Children[1].Children[2].Children[1].T.IntegerValue)
		n.Documents[i].Revisions = make([]Revision, 0, 5)
		revNode := t1.Children[1].Children[3].Children[0].Children[1]
		// What this extra list tuple(2) wrapper is doing here?
		// Branching?
		for {
			r := new(Revision)
			r.RevID = append([]byte(nil), revNode.Children[0].T.Binary...)
			if revNode.Children[1].T.Term == erldeser.NilExt {
				r.Offset = -1
			} else {
				r.Deleted = int8(revNode.Children[1].Children[0].T.IntegerValue)
				r.Offset = revNode.Children[1].Children[1].T.IntegerValue
				r.UpdateSeq = revNode.Children[1].Children[2].T.IntegerValue
				r.Size1 = int32(revNode.Children[1].Children[3].Children[0].T.IntegerValue)
				r.Size2 = int32(revNode.Children[1].Children[3].Children[1].T.IntegerValue)
			}
			n.Documents[i].Revisions = append(n.Documents[i].Revisions, *r)
			if revNode.Children[2].T.Term == erldeser.NilExt {
				break
			}
			revNode = revNode.Children[2].Children[0]
		}
	}
	return nil
}
