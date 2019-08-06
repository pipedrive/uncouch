package main

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/pipedrive/uncouch/aws"
	"github.com/pipedrive/uncouch/config"
	"github.com/pipedrive/uncouch/couchdbfile"
	"github.com/pipedrive/uncouch/tar"
	"github.com/spf13/cobra"
	"io"
	"io/ioutil"
	"os"
	"runtime"
	"strings"
	"time"
)

func cmdDataFunc(cmd *cobra.Command, args []string) error {
	filename := args[0]

	var (
		fileBytes []byte
		n int64
		err error
	)

	if strings.HasPrefix(filename, "s3://") {
		fileBytes, n, err = aws.S3FileReader(filename)
	} else {
		fileBytes, n, err = readInputFile(filename)
	}

	if err != nil {
		slog.Error(err)
		return err
	}

	memoryReader := bytes.NewReader(fileBytes)
	cf, err := couchdbfile.New(memoryReader, n)
	if err != nil {
		slog.Error(errors.New("Error in file: " + filename))
		slog.Error(err)
		return err
	}

	newFilename := createOutputFilename(filename)
	if newFilename == "" {
		err := errors.New("Could not create output filename.")
		slog.Error(err)
		return err
	}

	fmt.Println("New Filename: " + newFilename)
	return writeData(cf, newFilename)
}

func cmdUntarFunc(cmd *cobra.Command, args []string) error {
	filename := args[0]

	if ! strings.HasSuffix(filename, ".tar.gz") {
		err := errors.New("File is not .tar.gz")
		slog.Error(err)
		return err
	}

	var err error
	oldFilename := filename
	if strings.HasPrefix(filename, "s3://") {
		filename, err = aws.S3FileDownloader(filename)
		if err != nil {
			slog.Error(err)
			return err
		}
	}

	// Open tar.gz file.
		filesChan := make(chan tar.UntarredFile)
		untarDone := make(chan tar.Done)
		var (
			oksChan []string
			errorsChan []error
		)

	f, err := os.Open(filename)
	if err != nil {
		slog.Error(err)
		return err
	}

	go tar.Untar(config.TEMP_INPUT_DIR, f, filesChan, untarDone)

	workersQ := config.WORKERS_Q
	createWorkers(workersQ, filesChan, &oksChan, &errorsChan)

	d := <- untarDone
	if !d.Res {
		err := errors.New("Error while untarring file.")
		slog.Error(err)
		return err
	}

	waitingTime := 0
	for {
		if uint32(len(oksChan) + len(errorsChan)) >= d.FileQ {
			break
		}

		if waitingTime > 5 {
			if uint32(len(oksChan) + len(errorsChan)) != d.FileQ {
				errMessage := fmt.Sprintf("Not enough files processed. Expected: %v. Processed: %v. Ok: %v. Errors: %v", d.FileQ, len(oksChan) + len(errorsChan), len(oksChan), len(errorsChan))
				err := errors.New(errMessage)
				slog.Error(err)
				return err
			}
		}

		time.Sleep(1 * time.Minute)
		waitingTime += 1
	}

	for _, f := range oksChan {
		fmt.Printf("Processed file: %s\n", f)
	}

	if len(errorsChan) > 0 {
		err := errors.New("Detected errors while processing files.")
		slog.Error(err)
		for _, err := range errorsChan {
			slog.Error(err)
			return err
		}
	}

	// tarPath := path.Join(config.TEMP_OUTPUT_DIR, config.OUTPUT_FILENAME + ".tar.gz")
	tarPath := createOutputTarFilename(filename)

	var buf bytes.Buffer

	err = tar.Tar(config.TEMP_OUTPUT_DIR, &buf)
	if err != nil {
		slog.Error(err)
	}

	// If origin was S3, send result to S3.
	if strings.HasPrefix(oldFilename, "s3://") {
		newFilename := createOutputTarFilename(oldFilename)
		file := bytes.NewReader(buf.Bytes())
		err = aws.S3FileWriter(file, newFilename)
		if err != nil {
			slog.Error(err)
			return err
		}
	} else {
		// otherwise, write the .tar.gzip to local storage
		fileToWrite, err := os.OpenFile(tarPath, os.O_CREATE|os.O_RDWR, os.FileMode(0755))
		if err != nil {
			slog.Error(err)
			return err
		}
		if _, err := io.Copy(fileToWrite, &buf); err != nil {
			slog.Error(err)
		}
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
