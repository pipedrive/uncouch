package couchdbfile

import (
	"bytes"
	"fmt"

	"github.com/pipedrive/uncouch/leakybucket"

	"github.com/pipedrive/uncouch/couchbytes"
	"github.com/pipedrive/uncouch/erldeser"
)

// ReadIDNode reads node from data file
func (cf *CouchDbFile) ReadIDNode(offset int64) (*KpNodeID, *KvNode, error) {
	// slog.Debugf("Starting readNode with offset %d", offset)
	buf, err := couchbytes.ReadNode(cf.input, offset)
	if err != nil {
		slog.Error(err)
		return nil, nil, err
	}
	defer leakybucket.Put(buf)
	bufReader := bytes.NewReader(*buf)
	s, err := erldeser.New(bufReader)
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
	switch t.Children[0].T.StringValue {
	case "kp_node":
		var kpNode KpNodeID
		err = kpNode.readFromTermite(t)
		if err != nil {
			slog.Error(err)
			return nil, nil, err
		}
		return &kpNode, nil, nil
	case "kv_node":
		var kvNode KvNode
		err = kvNode.readFromTermite(t)
		if err != nil {
			slog.Error(err)
			return nil, nil, err
		}
		return nil, &kvNode, nil
	default:
		err := fmt.Errorf("Unknown node type: %v", t.Children[0].T.StringValue)
		slog.Error(err)
		return nil, nil, err
	}
}

// ReadSeqNode reads node from data file
func (cf *CouchDbFile) ReadSeqNode(offset int64) (*KpNodeSeq, *KvNode, error) {
	// slog.Debugf("Starting readNode with offset %d", offset)
	buf, err := couchbytes.ReadNode(cf.input, offset)
	if err != nil {
		slog.Error(err)
		return nil, nil, err
	}
	defer leakybucket.Put(buf)
	bufReader := bytes.NewReader(*buf)
	s, err := erldeser.New(bufReader)
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
	switch t.Children[0].T.StringValue {
	case "kp_node":
		var kpNode KpNodeSeq
		err = kpNode.readFromTermite(t)
		if err != nil {
			slog.Error(err)
			return nil, nil, err
		}
		return &kpNode, nil, nil
	case "kv_node":
		var kvNode KvNode
		err = kvNode.readFromTermite(t)
		if err != nil {
			slog.Error(err)
			return nil, nil, err
		}
		return nil, &kvNode, nil
	default:
		err := fmt.Errorf("Unknown node type: %v", t.Children[0].T.StringValue)
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
	buf, err := couchbytes.ReadDbHeader(cf.input, offset)
	if err != nil {
		slog.Error(err)
		return nil, err
	}
	defer leakybucket.Put(buf)
	bufReader := bytes.NewReader(*buf)
	s, err := erldeser.New(bufReader)
	if err != nil {
		slog.Error(err)
		return nil, err
	}
	t, err := s.ReadTermite()
	if err != nil {
		slog.Error(err)
		return nil, err
	}
	var header DbHeader
	header.readFromTermite(t)
	return &header, nil
}
