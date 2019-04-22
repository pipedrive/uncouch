package main

import (
	"bytes"
	"fmt"

	"github.com/pipedrive/uncouch/couchdbfile"
)

func writeData(cf *couchdbfile.CouchDbFile) error {
	return processNode(cf, cf.Header.IDTreeState.Offset)
}

func processNode(cf *couchdbfile.CouchDbFile, offset int64) error {
	for {
		kpNode, kvNode, err := cf.ReadNode(offset)
		if err != nil {
			slog.Error(err)
			return err
		}
		if kpNode != nil {
			// Pointer node, dig deeper
			for _, node := range kpNode.Pointers {
				err = processNode(cf, node.Offset)
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
			fmt.Print(output.String())
			return nil
		}
	}
}
