package main

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/pipedrive/uncouch/config"
	"github.com/pipedrive/uncouch/couchdbfile"
	"github.com/pipedrive/uncouch/tar"
	"github.com/spf13/cobra"
	"io/ioutil"
	"os"
	"path"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

func cmdDataFunc(cmd *cobra.Command, args []string) error {
	filename := args[0]

	fc, err := auxDataFunc(filename, "")
	if err != nil {
		slog.Error(err)
		return err
	}

	return writeData(fc.Cf, fc.Filename)
}

func cmdUntarFunc(cmd *cobra.Command, args []string) error {
	var filename string = args[0]
	var dstFolder string = args[1]

	workersQ, err := strconv.ParseUint(args[2], 10, 8)
	if err != nil {
		slog.Error(err)
		return err
	}

	writersQ, err := strconv.ParseUint(args[3], 10, 8)
	if err != nil {
		slog.Error(err)
		return err
	}

	if ! strings.HasSuffix(filename, ".tar.gz") {
		err := errors.New("File is not .tar.gz")
		slog.Error(err)
		return err
	}

	var wgp, wgw sync.WaitGroup

	// Open tar.gz file.
		filesChan := make(chan tar.UntarredFile)
		writesChan := make(chan FileContent)
		untarDone := make(chan tar.Done)
		var (
			oksChan []string
			errorsChan []error
			woksChan []string
			werrorsChan []error
		)

	f, err := os.Open(filename)
	if err != nil {
		slog.Error(err)
		return err
	}

	startTs := time.Now().Format("20060102_150405")

	_, name := path.Split(filename)

	inputFolder := path.Join(config.TEMP_INPUT_DIR, strings.TrimSuffix(name, ".tar.gz"), startTs)

	go tar.Untar(inputFolder, f, filesChan, untarDone)

	createWorkers(workersQ, filesChan, writesChan, dstFolder, &wgp, &oksChan, &errorsChan)
	createWriters(writersQ, writesChan, &wgw, &woksChan, &werrorsChan)

	d := <- untarDone
	if !d.Res {
		err := errors.New("Error while untarring file.")
		slog.Error(err)
		return err
	}
	close(untarDone)

	wgp.Wait()
	close(writesChan)
	total := uint32(len(oksChan) + len(errorsChan))
	if total != d.FileQ {
		errMessage := fmt.Sprintf("Expected files: %v. Processed: %v. Ok: %v. Errors: %v", d.FileQ, total, len(oksChan), len(errorsChan))
		err := errors.New(errMessage)
		slog.Error(err)
		return err
	}

	if len(errorsChan) > 0 {
		err := errors.New("Detected errors while processing files.")
		slog.Error(err)
		for _, err := range errorsChan {
			slog.Error(err)
		}
		return err
	}

	wgw.Wait()
	total = uint32(len(woksChan) + len(werrorsChan))
	if total != d.FileQ {
		errMessage := fmt.Sprintf("Not enough files written. Expected: %v. Processed: %v. Ok: %v. Errors: %v", d.FileQ, total, len(woksChan), len(werrorsChan))
		err := errors.New(errMessage)
		slog.Error(err)
		return err
	}

	for _, f := range woksChan {
		fmt.Printf("Written data to file: %s\n", f)
	}

	if len(werrorsChan) > 0 {
		err := errors.New("Detected errors while writing files.")
		slog.Error(err)
		for _, err := range werrorsChan {
			slog.Error(err)
		}
		return err
	}

	return err
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
