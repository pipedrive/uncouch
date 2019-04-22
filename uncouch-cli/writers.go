package main

import (
	"bytes"

	"github.com/pipedrive/uncouch/couchdbfile"
)

func writeData(cf *couchdbfile.CouchDbFile) error {
	return processSeqNode(cf, cf.Header.SeqTreeState.Offset)
	// return processIDNode(cf, cf.Header.IDTreeState.Offset)
}

func processIDNode(cf *couchdbfile.CouchDbFile, offset int64) error {
	for {
		kpNode, kvNode, err := cf.ReadIDNode(offset)
		if err != nil {
			slog.Error(err)
			return err
		}
		if kpNode != nil {
			// Pointer node, dig deeper
			for _, node := range kpNode.Pointers {
				err = processIDNode(cf, node.Offset)
				if err != nil {
					slog.Error(err)
					return err
				}
			}
			return nil
		} else if kvNode != nil {
			var output bytes.Buffer
			for _, document := range kvNode.Documents {
				err = cf.WriteDocument(&document, &output)
				if err != nil {
					slog.Error(err)
					return err
				}
			}
			// fmt.Print(output.String())
			return nil
		}
	}
}

func processSeqNode(cf *couchdbfile.CouchDbFile, offset int64) error {
	for {
		kpNode, kvNode, err := cf.ReadSeqNode(offset)
		if err != nil {
			slog.Error(err)
			return err
		}
		if kpNode != nil {
			// Pointer node, dig deeper
			for _, node := range kpNode.Pointers {
				err = processSeqNode(cf, node.Offset)
				if err != nil {
					slog.Error(err)
					return err
				}
			}
			return nil
		} else if kvNode != nil {
			var output bytes.Buffer
			for _, document := range kvNode.Documents {
				err = cf.WriteDocument(&document, &output)
				if err != nil {
					slog.Error(err)
					return err
				}
			}
			// fmt.Print(output.String())
			return nil
		}
	}
}
