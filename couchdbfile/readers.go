package couchdbfile

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/pipedrive/uncouch/leakybucket"
	"github.com/pipedrive/uncouch/termite"

	"github.com/pipedrive/uncouch/couchbytes"
	"github.com/pipedrive/uncouch/erldeser"
)

// ReadNodeBytes reads node bytes from given offset
func (cf *CouchDbFile) ReadNodeBytes(offset int64) (*[]byte, error) {
	return couchbytes.ReadNodeBytes(cf.input, offset)
}

// ReadIDNode reads ID Btree node from the given offset
func (cf *CouchDbFile) ReadIDNode(offset int64) (*KpNodeID, *KvNode, error) {
	// slog.Debugf("Starting readNode with offset %d", offset)
	buf, err := couchbytes.ReadNodeBytes(cf.input, offset)
	if err != nil {
		slog.Error(err)
		return nil, nil, err
	}
	defer leakybucket.PutBytes(buf)
	s, err := erldeser.NewScanner(*buf)
	if err != nil {
		slog.Error(err)
		return nil, nil, err
	}
	tb, err := termite.NewBuilder()
	if err != nil {
		slog.Error(err)
		return nil, nil, err
	}
	t, err := tb.ReadTermite(s)
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

// ReadSeqNode reads Sequence Btree node from the given offset
func (cf *CouchDbFile) ReadSeqNode(offset int64) (*KpNodeSeq, *KvNode, error) {
	// slog.Debugf("Starting readNode with offset %d", offset)
	if offset == 0 {
		return nil, nil, nil
	}

	buf, err := couchbytes.ReadNodeBytes(cf.input, offset)
	if err != nil {
		slog.Error(err)
		return nil, nil, err
	}
	defer leakybucket.PutBytes(buf)
	s, err := erldeser.NewScanner(*buf)
	if err != nil {
		slog.Error(err)
		return nil, nil, err
	}
	tb, err := termite.NewBuilder()
	if err != nil {
		slog.Error(err)
		return nil, nil, err
	}
	t, err := tb.ReadTermite(s)
	if err != nil {
		slog.Error(err)
		return nil, nil, err
	}
	// Switch
	switch string(t.Children[0].T.Binary) {
	case "kp_node":
		var kpNode KpNodeSeq
		err = kpNode.readFromTermite(t)
		if err != nil {
			slog.Error(err)
			return nil, nil, err
		}
		// t.Release()
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

// ReadDbHeader reads DB header from input Reader
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
	s, err := erldeser.NewScanner(*buf)
	if err != nil {
		slog.Error(err)
		return nil, err
	}
	tb, err := termite.NewBuilder()
	if err != nil {
		slog.Error(err)
		return nil, err
	}
	t, err := tb.ReadTermite(s)
	if err != nil {
		slog.Error(err)
		return nil, err
	}
	// slog.Debugf("%+v", t)
	var header DbHeader
	err = header.readFromTermite(t)

	t.Release()
	return &header, err
}

func (cf *CouchDbFile) Read(offset int64, jsonLines chan map[string]interface{}) {
	defer close(jsonLines)
	for {
		kpNode, kvNode, err := cf.ReadSeqNode(offset)
		if err != nil {
			panic(err)
		}
		if kpNode != nil && kvNode != nil {
			log.Info("Empty Node.")
		}
		if kpNode != nil {
			// Pointer node, dig deeper
			for _, node := range kpNode.Pointers {
				cf.Read(node.Offset, jsonLines)
			}
		} else if kvNode != nil {
			var payload map[string]interface{}
			for _, document := range kvNode.Documents {
				output := leakybucket.GetBuffer()
				cf.WriteChar("{", output)
				cf.WriteKeyValue("_id", strings.TrimSpace(string(document.ID)), output)
				cf.WriteChar(",", output)
				cf.WriteString("doc", output)
				cf.WriteChar(":", output)
				err = cf.WriteDocument(&document, output)
				if err != nil {
					panic(err)
				}
				cf.WriteChar("}", output)

				if err := json.Unmarshal(output.Bytes(), &payload); err != nil {
					panic(err)
				}
				jsonLines <- payload
				leakybucket.PutBuffer(output)
			}
		}
		break
	}
}
