package couchdbfile

import (
	"bytes"

	"github.com/pipedrive/uncouch/couchbytes"
	"github.com/pipedrive/uncouch/erldeser"
	"github.com/pipedrive/uncouch/jsonser"
	"github.com/pipedrive/uncouch/leakybucket"
)

type CouchDbDocument struct {
	Id    string
	Rev   string
	Value map[string]interface{}
}

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

func (cf *CouchDbFile) WriteKeyValue(key string, value string, collector *bytes.Buffer) {
	cf.WriteString(key, collector)
	cf.WriteChar(":", collector)
	cf.WriteString(value, collector)
}

func (cf *CouchDbFile) WriteString(key string, collector *bytes.Buffer) {
	_, err := collector.WriteString("\"" + key + "\"")
	if err != nil {
		panic(err)
	}
}

func (cf *CouchDbFile) WriteChar(char string, collector *bytes.Buffer) {
	_, err := collector.WriteString(char)
	if err != nil {
		panic(err)
	}
}
