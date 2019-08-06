package main

import (
	"fmt"
	"github.com/pipedrive/uncouch/config"
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
	file := u.Path

	fileExt := filepath.Ext(file)

	t := time.Now()
	ts := fmt.Sprint(t.Format("_20060102_150405"))

	temp := strings.Replace(file, config.TEMP_INPUT_DIR, config.TEMP_OUTPUT_DIR, 1)

	newExt := ts + ".json"

	u.Path = strings.Replace(temp, fileExt, newExt, 1)

	return u.String()
}

func createOutputTarFilename(filename string) string {
	u, err := url.Parse(filename)
	if err != nil {
		slog.Error(err)
		return ""
	}
	file := u.Path

	ts := fmt.Sprint(time.Now().Format("_20060102_150405"))

	fileExt := ".tar.gz"
	newExt := ts + ".tar.gz"

	u.Path = strings.Replace(file, fileExt, newExt, 1)

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

