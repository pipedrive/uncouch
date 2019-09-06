package main

import (
	"bytes"
	"errors"
	"github.com/pipedrive/uncouch/config"
	"github.com/pipedrive/uncouch/couchdbfile"
	"github.com/pipedrive/uncouch/tar"
	"github.com/spf13/cobra"
	"io/ioutil"
	"os"
	"path"
	"strconv"
	"strings"
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

func cmdUntarFunc(inputFile, outputDir, tmpDir string, workersQ uint) error {
	log.Info("Started processing: " + inputFile)

	if _, err := os.Stat(outputDir); os.IsNotExist(err) {
		log.Info("Creating output directory.")
		os.MkdirAll(outputDir, os.ModeDir)
	}

	if !strings.HasSuffix(inputFile, ".tar.gz") && !strings.HasSuffix(inputFile, ".tar") {
		err := errors.New("File is not .tar or .tar.gz archive")
		slog.Error(err)
		return err
	}

	filesChan := make(chan *tar.UntarredFile, 100)
	jsonChan := make(chan []byte, 1000)

	// Untar file and write contents to filesChan
	// channel is closed when there are no more files left
	go tar.Untar(inputFile, filesChan, tmpDir)

	// Create workers to parse couch files into JSON lines and write them to jsonChan
	// channel is closed when all couch files are processed
	go createWorkers(filesChan, jsonChan, workersQ)

	i := 0
	lineNum := 0

	outputBaseFilename := path.Join(outputDir, TrimSuffix(TrimSuffix(path.Base(inputFile), ".gz"), ".tar")+".json.gz")
	outputFilename := createOutputFilenameWithIndex(outputBaseFilename, i)
	outputFile, err := CreateGzipFile(outputFilename)
	if err != nil {
		slog.Error(err)
	}

	for jsonLine := range jsonChan {
		_, err := (outputFile.fw).Write(jsonLine)
		if err != nil {
			slog.Error(err)
		}
		err = (outputFile.fw).WriteByte('\n')
		if err != nil {
			slog.Error(err)
		}

		lineNum++
		if lineNum%10000 == 0 {
			s, err := outputFile.f.Stat()
			if err != nil {
				slog.Error(err)
			}
			if s.Size() >= config.FILE_SIZE {
				flush(outputFile)
				outputFile.gf.Close()
				outputFile.f.Close()
				i++
				outputFilename := createOutputFilenameWithIndex(outputBaseFilename, i)
				outputFile, err = CreateGzipFile(outputFilename)
				if err != nil {
					slog.Error(err)
				}
			}
		}
	}
	flush(outputFile)
	outputFile.gf.Close()
	outputFile.f.Close()
	log.Info("Finished processing: " + inputFile)
	log.Info("Lines: " + strconv.Itoa(lineNum))
	return err
}

func flush(outputFile FileCompressor) {
	err := outputFile.fw.Flush()
	if err != nil {
		slog.Error(err)
	}
	err = outputFile.gf.Flush()
	if err != nil {
		slog.Error(err)
	}
}

func TrimSuffix(s, suffix string) string {
	if strings.HasSuffix(s, suffix) {
		s = s[:len(s)-len(suffix)]
	}
	return s
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
