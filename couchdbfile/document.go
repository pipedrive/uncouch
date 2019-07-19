package couchdbfile

import (
	"bytes"

	"github.com/pipedrive/uncouch/couchbytes"
	"github.com/pipedrive/uncouch/erldeser"
	"github.com/pipedrive/uncouch/jsonser"
	"github.com/pipedrive/uncouch/leakybucket"
)

// WriteDocument writes document as JSON object into output buffer
func (cf *CouchDbFile) WriteDocument(di *DocumentInfo, output *bytes.Buffer) error {
	// Get buffer
	docBytes, err := couchbytes.ReadDocumentBytes(cf.input, di.Revisions[len(di.Revisions)-1].Offset)
	if err != nil {
		slog.Error(err)
		return err
	}
	defer leakybucket.PutBytes(docBytes)
	scanner, err := erldeser.NewScanner(*docBytes)
	if err != nil {
		slog.Error(err)
		return err
	}
	js, err := jsonser.New(scanner)
	if err != nil {
		slog.Error(err)
		return err
	}
	err = js.WriteJSONToBuffer(output)
	if err != nil {
		slog.Error(err)
		return err
	}
	return nil
}
