package main

import (
	"bytes"
	"fmt"
	"github.com/pipedrive/uncouch/config"
	"github.com/pipedrive/uncouch/couchdbfile"
	"io/ioutil"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

func auxDataFunc(filename, dstFolder string) (FileContent, error) {

	var file FileContent

	fileBytes, n, err := readInputFile(filename)
	if err != nil {
		//slog.Error(err)
		return file, err
	}

	memoryReader := bytes.NewReader(fileBytes)
	cf, err := couchdbfile.New(memoryReader, n)
	if err != nil {
		//slog.Error(errors.New("Error in file: " + filename))
		//slog.Error(err)
		return file, err
	}

	filename = createOutputFilename(filename, dstFolder)

	file = FileContent{cf, filename}

	return file, nil
}

func createOutputFilename(filename, dstFolder string) string {
	u, err := url.Parse(filename)
	if err != nil {
		slog.Error(err)
		return ""
	}
	file := u.Path

	fileExt := filepath.Ext(file)

	t := time.Now()
	ts := fmt.Sprint(t.Format("_20060102"))

	if dstFolder == "" {
		dstFolder = filepath.Dir(filename)
	}
	temp := strings.Replace(file, filepath.Dir(filename), dstFolder, 1)

	var newExt string
	if config.COMPRESS_OUTPUT {
		newExt = ts + ".json.gz"
	} else {
		newExt = ts + ".json"
	}


	u.Path = strings.Replace(temp, fileExt, newExt, 1)

	return u.String()
}

func createOutputFilenameWithIndex(filename string, index uint8) string {
	s      := "00" + strconv.Itoa(int(index))

	var (newExt string
		fileExt string)
	if config.COMPRESS_OUTPUT {
		fileExt = ".json.gz"
		newExt = "_" + s[len(s)-2:] + ".json.gz"
	} else {
		fileExt = ".json"
		newExt = "_" + s[len(s)-2:] + ".json"
	}

	return strings.Replace(filename, fileExt, newExt, 1)
}

func readInputFile(filename string) ([]byte, int64, error) {
	f, err := os.Open(filename)
	if err != nil {
		//slog.Error(err)
		return nil, 0, err
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		//slog.Error(err)
		return nil, 0, err
	}

	fileBytes, err := ioutil.ReadAll(f)
	if err != nil {
		//slog.Error(err)
		return nil, 0, err
	}
	return fileBytes, fi.Size(), nil
}
