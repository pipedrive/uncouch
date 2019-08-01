package main

import (
	"fmt"
	"io/ioutil"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"
)

func createOutputFilename(filename string) string {
	u, err := url.Parse(filename)
	if err != nil {
		slog.Error(err)
		return ""
	}
	dir := u.Host
	file := u.Path

	fmt.Println("Dir: " + dir)
	fileExt := filepath.Ext(file)

	t := time.Now()
	ts := fmt.Sprint(t.Format("_20060102_150405"))

	newExt := ts + ".json"
	u.Path = strings.Replace(file, fileExt, newExt, -1)
	return u.String()
}

func readInputFile(filename string) ([]byte, int64, error) {

	f, err := os.Open(filename)
	if err != nil {
		slog.Error(err)
		return nil, 0, err
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		slog.Error(err)
		return nil, 0, err
	}

	fileBytes, err := ioutil.ReadAll(f)
	if err != nil {
		slog.Error(err)
		return nil, 0, err
	}
	return fileBytes, fi.Size(), nil
}

