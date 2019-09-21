package couchdbfile

import (
	"bytes"

	"github.com/pipedrive/uncouch/couchbytes"
	"github.com/pipedrive/uncouch/erldeser"
	"github.com/pipedrive/uncouch/jsonser"
	"github.com/pipedrive/uncouch/leakybucket"
)

// Explore is development routine to try out approaches
func (cf *CouchDbFile) Explore() error {
	slog.Debug("Starting Explore ...")
	slog.Debugf("%+v", cf.Header)
	var counter int64
	nextOffset := cf.Header.IDTreeState.Offset
	for {
		kpNode, kvNode, err := cf.ReadIDNode(nextOffset)
		if err != nil {
			slog.Error(err)
			return err
		}
		if kpNode != nil {
			slog.Debugf("LEVEL KPNODE %v !!!!", counter)
			counter++
			slog.Debugf("%+v", kpNode)
			nextOffset = kpNode.Pointers[0].Offset
		} else if kvNode != nil {
			slog.Debugf("LEVEL KVNODE %v !!!!", counter)
			counter++
			for i := int32(0); i < kvNode.Length; i++ {
				slog.Debugf("%v", string(kvNode.Documents[i].ID))
				for _, rev := range kvNode.Documents[i].Revisions {
					if rev.Offset > 0 {
						docBytes, err := couchbytes.ReadDocumentBytes(cf.input, rev.Offset)
						if err != nil {
							slog.Error(err)
							return err
						}
						defer leakybucket.PutBytes(docBytes)

						/*
							docReader := *bytes.NewReader(*docBytes)
							s, err := erldeser.New(&docReader)
							if err != nil {
								slog.Error(err)
								return err
							}
							t, err := s.ReadTermite()
							slog.Debug(t.String())
							if err != nil {
								slog.Error(err)
								return err
							}
						*/
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
						var buf bytes.Buffer
						err = js.WriteJSONToBuffer(&buf, jsonser.JSONSerExtraAttrs{})
						slog.Debug(buf.String())

					}
				}
			}
			break
		}
	}
	slog.Debug("Done Explore.")
	return nil
}
