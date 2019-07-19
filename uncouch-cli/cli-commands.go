package main

import (
	"bytes"
	"io/ioutil"
	"os"
	"runtime"

	"github.com/pipedrive/uncouch/couchdbfile"
	"github.com/spf13/cobra"
)

func cmdDataFunc(cmd *cobra.Command, args []string) error {
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
	return writeData(cf)
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

func cmdSandboxFunc(cmd *cobra.Command, args []string) error {
	slog.Debug("Starting Sandbox ...")
	var filename string
	if len(args) == 0 {
		switch runtime.GOOS {
		case "windows":
			// filename = "c:\\Data\\uncouch\\sandbox.1553883929.couch"
			// filename = "c:\\Data\\uncouch\\test01.1555094281.empty.couch"
			// filename = "c:\\Data\\uncouch\\test01.1555094281.single.couch"
			// filename = "c:\\Data\\uncouch\\test01.1555094281.multipleversions.couch"
			// filename = "c:\\Data\\uncouch\\test01.1555094281.multipleversions.compacted.couch"
			filename = "c:\\Data\\uncouch\\test01.1555094281.couch"
			// filename = "c:\\Data\\uncouch\\books.1555177047.couch"
			// filename = "c:\\Data\\uncouch\\activity.1555355987.couch"

		case "darwin":
			// filename = "/Users/tarmotali/Data/company_4318726.1553594378.couch"
			// filename = "/Users/tarmotali/Data/activity.1555355987.couch"
			filename = "/Users/tarmotali/Data/sandbox.1555783978.couch"
		default:
			slog.Panicf("Not implemented yet: %v\n", runtime.GOOS)
		}
	} else {
		filename = args[0]
	}
	slog.Debug(filename)
	f, err := os.Open(filename)
	if err != nil {
		slog.Errorf("Unable to open input file, error %s", err)
		return err
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		slog.Errorf("Could not obtain stat %s", err)
		return err
	}
	cf, err := couchdbfile.New(f, fi.Size())
	cf.Explore()
	if err != nil {
		slog.Error(err)
		return err
	}
	log.Debug("Done Sandbox")
	return nil
}
