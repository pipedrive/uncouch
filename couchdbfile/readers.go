package couchdbfile

import (
	"fmt"

	"github.com/pipedrive/uncouch/erlterm"
	"github.com/pipedrive/uncouch/leakybucket"

	"github.com/pipedrive/uncouch/couchbytes"
	"github.com/pipedrive/uncouch/erldeser"
)

// ReadIDNode reads node from data file
func (cf *CouchDbFile) ReadIDNode(offset int64) (*KpNodeID, *KvNode, error) {
	// slog.Debugf("Starting readNode with offset %d", offset)
	buf, err := couchbytes.ReadNodeBytes(cf.input, offset)
	if err != nil {
		slog.Error(err)
		return nil, nil, err
	}
	defer leakybucket.PutBytes(buf)
	s, err := erldeser.New(*buf)
	if err != nil {
		slog.Error(err)
		return nil, nil, err
	}
	t, err := s.ReadTermite()
	if err != nil {
		slog.Error(err)
		return nil, nil, err
	}
	// Switch
	switch string(t.Children[0].T.Binary) {
	case "kp_node":
		var kpNode KpNodeID
		err = kpNode.readFromTermite(t)
		if err != nil {
			slog.Error(err)
			return nil, nil, err
		}
		t.Release()
		return &kpNode, nil, nil
	case "kv_node":
		var kvNode KvNode
		err = kvNode.readFromTermite(t)
		if err != nil {
			slog.Error(err)
			return nil, nil, err
		}
		t.Release()
		return nil, &kvNode, nil
	default:
		err := fmt.Errorf("Unknown node type: %v", string(t.Children[0].T.Binary))
		slog.Error(err)
		return nil, nil, err
	}
}

// ReadSeqNode reads node from data file
func (cf *CouchDbFile) ReadSeqNode(offset int64) (*KpNodeSeq, *KvNode, error) {
	// slog.Debugf("Starting readNode with offset %d", offset)
	buf, err := couchbytes.ReadNodeBytes(cf.input, offset)
	if err != nil {
		slog.Error(err)
		return nil, nil, err
	}
	defer leakybucket.PutBytes(buf)
	s, err := erldeser.New(*buf)
	if err != nil {
		slog.Error(err)
		return nil, nil, err
	}
	var t erlterm.Term
	t.Reset()
	err = s.Scan(&t)
	if err != nil {
		slog.Error(err)
		return nil, nil, err
	}
	if t.Term != erldeser.SmallTupleExt && t.IntegerValue != 2 {
		parsingError := fmt.Errorf("Expected SmallTuple with size 2, got %v, %v", t.Term, t.IntegerValue)
		slog.Error(parsingError)
		return nil, nil, parsingError
	}
	err = s.Scan(&t)
	if err != nil {
		slog.Error(err)
		return nil, nil, err
	}
	if t.Term != erldeser.AtomExt {
		parsingError := fmt.Errorf("Expected Binary, got %v", t.Term)
		slog.Error(parsingError)
		return nil, nil, parsingError
	}
	termTypeString := string(t.Binary)

	/*
		t, err := s.ReadTermite()
		if err != nil {
			slog.Error(err)
			return nil, nil, err
		}
	*/
	s.Rewind()
	// Switch
	switch string(termTypeString) {
	case "kp_node":
		var kpNode KpNodeSeq
		// err = kpNode.readFromTermite(t)
		err = kpNode.readFromScanner(s)
		if err != nil {
			slog.Error(err)
			return nil, nil, err
		}
		// t.Release()
		return &kpNode, nil, nil
	case "kv_node":
		var kvNode KvNode
		t, err := s.ReadTermite()
		if err != nil {
			slog.Error(err)
			return nil, nil, err
		}
		err = kvNode.readFromTermite(t)
		// err = kvNode.readFromScanner(s)
		if err != nil {
			slog.Error(err)
			return nil, nil, err
		}
		// t.Release()
		return nil, &kvNode, nil
	default:
		err := fmt.Errorf("Unknown node type: %v", termTypeString)
		slog.Error(err)
		return nil, nil, err
	}
}

// ReadDbHeader reads DB header from data file
func (cf *CouchDbFile) ReadDbHeader() (*DbHeader, error) {
	offset, err := cf.Header.findHeader(cf.input, cf.size)
	if err != nil {
		slog.Error(err)
		return nil, err
	}
	buf, err := couchbytes.ReadDbHeaderBytes(cf.input, offset)
	if err != nil {
		slog.Error(err)
		return nil, err
	}
	defer leakybucket.PutBytes(buf)
	s, err := erldeser.New(*buf)
	if err != nil {
		slog.Error(err)
		return nil, err
	}
	t, err := s.ReadTermite()
	if err != nil {
		slog.Error(err)
		return nil, err
	}
	// slog.Debugf("%+v", t)
	var header DbHeader
	header.readFromTermite(t)
	t.Release()
	return &header, nil
}
