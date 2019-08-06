package main

import (
	"fmt"
	"github.com/pipedrive/uncouch/couchdbfile"
	"github.com/pipedrive/uncouch/tar"
	"io"
	"strconv"
)

func createWorkers(workersQ int, filesChan chan tar.UntarredFile, oksChan *[]string, errorsChan *[]error) () {
	for i := int(0); i < workersQ; i++ {
		fmt.Println("Starting worker: " + strconv.Itoa(i))
		go worker(filesChan, oksChan, errorsChan, i)
	}
	return
}

func worker(filesChan chan tar.UntarredFile, oksChan *[]string, errorsChan *[]error, i int) () {
	for {
		current := <- filesChan

		if current.Filepath == "finished" {
			fmt.Println("Finalizing worker: " + strconv.Itoa(i))
			return
		}

		// actual processing
		// err := processAll(current.Input, current.Filepath, current.Size)
		err := processFiles(current.Filepath)
		if err != nil {
			*errorsChan = append(*errorsChan, err)
			slog.Error(err)
		}
		*oksChan = append(*oksChan, current.Filepath)

	}
}

func processAll(memoryReader io.ReadSeeker, filename string, n int64) (error) {

	cf, err := couchdbfile.New(memoryReader, n)
	if err != nil {
		slog.Error(err)
		return err
	}

	return writeData(cf, filename)
}

func processFiles(filename string) (error) {
	arg := make([]string, 1)
	arg[0] = filename

	err := cmdDataFunc(nil, arg)
	if err != nil {
		slog.Error(err)
	}
	return err
}
