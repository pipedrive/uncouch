package cli

import (
	"bytes"
	"encoding/json"
	"github.com/pipedrive/uncouch/couchdbfile"
	"github.com/spf13/cobra"
	"io/ioutil"
	"os"
	"path"
	"strings"
)

func cmdDataFunc(cmd *cobra.Command, args []string) error {
	filename := args[0]

	// open file for reading
	f, err := os.Open(filename)
	if err != nil {
		slog.Error(err)
		return err
	}
	defer f.Close()

	// get file size
	fi, err := f.Stat()
	if err != nil {
		slog.Error(err)
		return err
	}

	// read file into memory
	fileBytes, err := ioutil.ReadAll(f)
	if err != nil {
		slog.Error(err)
		return err
	}
	memoryReader := bytes.NewReader(fileBytes)

	// get CouchDbFile
	cf, err := couchdbfile.New(memoryReader, fi.Size())
	if err != nil {
		slog.Error(err)
		return err
	}

	// read JSON from CouchDbFile and send to jsonLines channel
	couchDbDocuments := cf.Read(100)

	// read from the channel and print the results
	dbName := strings.Split(path.Base(filename), ".")[0]
	for doc := range couchDbDocuments {
		line := map[string]interface{}{
			"_id": doc.Id,
			"_db": dbName,
			"doc": doc.Value,
		}
		s, err := json.Marshal(line)

		if err != nil {
			slog.Error(err)
		}
		log.Info(string(s))
	}
	return nil
}

func cmdHeadersFunc(cmd *cobra.Command, args []string) error {
	outputdir := args[1]
	_, err := os.Stat(outputdir)
	if err != nil {
		return err
	}
	filename := args[0]
	f, err := os.Open(filename)
	if err != nil {
		slog.Error(err)
		return err
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		slog.Error(err)
		return err
	}
	fileBytes, err := ioutil.ReadAll(f)
	if err != nil {
		slog.Error(err)
		return err
	}
	memoryReader := bytes.NewReader(fileBytes)
	cf, err := couchdbfile.New(memoryReader, fi.Size())
	if err != nil {
		slog.Error(err)
		return err
	}

	return writeHeaders(cf, outputdir)
}
