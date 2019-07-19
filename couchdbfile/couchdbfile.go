// Package couchdbfile provides interface to Couch DB file. It takes
// provided Reader as input and tries to parse it as CouchDB file. It
// contains methods to read Nodes and Documents.
package couchdbfile

import (
	"io"
)

// CouchDbFile is main interface to interact with single CouchDB file
type CouchDbFile struct {
	Header DbHeader
	input  io.ReadSeeker
	size   int64
}

// New will return CouchDbFile
func New(input io.ReadSeeker, size int64) (cf *CouchDbFile, err error) {
	var (
		newCouchDbFile CouchDbFile
	)
	cf = &newCouchDbFile
	// Add handle to internal input variable
	cf.input = input
	cf.size = size
	header, err := cf.ReadDbHeader()
	if err != nil {
		slog.Error(err)
		return nil, err
	}
	cf.Header = *header
	return cf, nil
}
