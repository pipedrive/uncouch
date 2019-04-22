package couchdbfile

import (
	"bytes"

	"github.com/pipedrive/uncouch/couchbytes"
	"github.com/pipedrive/uncouch/erldeser"
)

// WriteDocument writes document as JSON object into output buffer
func (cf *CouchDbFile) WriteDocument(di *DocumentInfo, output *bytes.Buffer) error {
	// Get buffer
	docBytes, err := couchbytes.ReadDocument(cf.input, di.Revisions[len(di.Revisions)-1].Offset)
	if err != nil {
		slog.Error(err)
		return err
	}
	scanner, err := erldeser.New(*docBytes)
	if err != nil {
		slog.Error(err)
		return err
	}
	err = scanner.WriteJSONToBuffer(output)
	return nil
}
