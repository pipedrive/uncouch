package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/pipedrive/uncouch/couchdbfile"
	"github.com/pipedrive/uncouch/leakybucket"
	"github.com/pipedrive/uncouch/tar"
	"path"
	"strconv"
	"strings"
	"sync"
)

func createWorkers(filesChan chan *tar.UntarredFile, jsonChan chan []byte, workersQ uint) {
	defer close(jsonChan)
	var wgp sync.WaitGroup
	wgp.Add(int(workersQ))
	for i := uint(0); i < workersQ; i++ {
		log.Info("Starting worker: " + strconv.Itoa(int(i)))
		go worker(filesChan, jsonChan, &wgp, i)
	}
	wgp.Wait()
}

func worker(filesChan chan *tar.UntarredFile, jsonChan chan []byte, wgp *sync.WaitGroup, i uint) {
	total, oks, errs := uint64(0), uint64(0), uint64(0)
	var payload map[string]interface{}
	for current := range filesChan {
		var err error
		var file FileContent

		total++

		file, err = processAll(current.Input, current.Filepath, current.Size)
		db_name := strings.Split(path.Base(current.Filepath), ".")[0]

		if err != nil {
			errs++
			slog.Error(err)
		} else {
			oks++
			str := leakybucket.GetStrBuilder()
			err := processSeqNode(file.Cf, file.Cf.Header.SeqTreeState.Offset, str)
			if err != nil {
				slog.Error("Error in file:" + file.Filename)
				slog.Error(err)
			} else {
				scanner := bufio.NewScanner(strings.NewReader(str.String()))
				for scanner.Scan() {
					byt := scanner.Bytes()
					if err := json.Unmarshal(byt, &payload); err != nil {
						panic(err)
					}
					payload["db_name"] = db_name
					jsonLine, err := json.Marshal(payload)
					if err != nil {
						slog.Error(err)
					}
					jsonChan <- jsonLine
				}

			}
			leakybucket.PutStrBuilder(str)
		}
	}
	log.Info(fmt.Sprintf("Worker %v - Total files: %v, Ok files: %v, errors: %v", strconv.Itoa(int(i)), total, oks, errs))
	wgp.Done()
}

func processAll(buf []byte, filename string, n int64) (FileContent, error) {
	var file FileContent

	memoryReader := bytes.NewReader(buf)

	cf, err := couchdbfile.New(memoryReader, n)
	if err != nil {
		slog.Error(errors.New("Error in file: " + filename))
		//slog.Error(err)
		return file, err
	}

	// filename = createOutputFilename(filename, dstFolder)

	file = FileContent{cf, filename}

	return file, err
}

func processFiles(filename, dstFolder string) (FileContent, error) {
	file, err := auxDataFunc(filename, dstFolder)
	if err != nil {
		//slog.Error(err)
	}
	return file, err
}
