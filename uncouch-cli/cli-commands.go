package main

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/pipedrive/uncouch/couchdbfile"
	"github.com/pipedrive/uncouch/tar"
	"github.com/spf13/cobra"
	"io/ioutil"
	"os"
	"strings"
	"sync"
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

func cmdUntarFunc(input, output, tmp_dir string, workersQ uint) error {
	var filename string = input
	var dstFolder string = output

	writersQ := workersQ

	if ! strings.HasSuffix(filename, ".tar.gz") {
		err := errors.New("File is not .tar.gz")
		slog.Error(err)
		return err
	}

	var wgp, wgw sync.WaitGroup

	// Open tar.gz file.
		filesChan := make(chan *tar.UntarredFile)
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

	inputFolder := tmp_dir

	go tar.Untar(inputFolder, f, filesChan, untarDone)

	writesChan := createWorkers(workersQ, filesChan, dstFolder, &wgp, &oksChan, &errorsChan)
	createWriters(writersQ, writesChan, &wgw, &woksChan, &werrorsChan)

	d := <- untarDone
	log.Info("untarDone")
	if !d.Res {
		err := errors.New("Error while untarring file.")
		slog.Error(err)
		return err
	}
	close(untarDone)

	wgp.Wait()
	log.Info("File deserializing done.")
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
	log.Info("File writing done.")
	total = uint32(len(woksChan) + len(werrorsChan))
	if total != d.FileQ {
		errMessage := fmt.Sprintf("Not enough files written. Expected: %v. Processed: %v. Ok: %v. Errors: %v", d.FileQ, total, len(woksChan), len(werrorsChan))
		err := errors.New(errMessage)
		slog.Error(err)
		return err
	}

	for _, f := range woksChan {
		log.Info(fmt.Sprintf("Written data to file: %s", f))
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
