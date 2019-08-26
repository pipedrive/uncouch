package main

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/pipedrive/uncouch/couchdbfile"
	"github.com/pipedrive/uncouch/tar"
	"strconv"
	"sync"
)

type muMap map [string]mapV
type okMap map [uint]uint64
type errMap map [string]error

type mapV struct {
	Mu *sync.Mutex
	ActiveQ uint8
}

func (m muMap) AddQ(f string) {
	m[f] = mapV{Mu: m[f].Mu, ActiveQ: m[f].ActiveQ+1}
}

func (m muMap) SubstractQ(f string) {
	m[f] = mapV{Mu: m[f].Mu, ActiveQ: m[f].ActiveQ-1}
}

func (m muMap) Get(f string, mapMutex *sync.Mutex) (*sync.Mutex) {
	mapMutex.Lock()
	defer mapMutex.Unlock()

	mv, found := m[f]
	if found{
		m.AddQ(f)
		return mv.Mu
	}
	var newMutex sync.Mutex
	var newMV mapV
	newMV.Mu = &newMutex
	newMV.ActiveQ = 1
	m[f] = newMV

	return &newMutex
}

func (m muMap) Delete(f string, mapMutex *sync.Mutex) {
	mapMutex.Lock()
	defer mapMutex.Unlock()
	if m[f].ActiveQ == 1 {
		delete(m, f)
	} else {
		m.SubstractQ(f)
	}

}

func createWorkers(workersQ uint, filesChan chan *tar.UntarredFile, dstFolder string, wgp *sync.WaitGroup, oksMap okMap, errorsMap errMap) (chan FileContent) {
	wgp.Add(int(workersQ))
	writesChan := make(chan FileContent)
	var s sync.Mutex
	for i := uint(0); i < workersQ; i++ {
		log.Info("Starting worker: " + strconv.Itoa(int(i)))
		go worker(filesChan, writesChan, dstFolder, wgp, &s, oksMap, errorsMap, i)
	}
	return writesChan
}

func worker(filesChan chan *tar.UntarredFile, writesChan chan FileContent, dstFolder string, wgp *sync.WaitGroup, s *sync.Mutex, oksMap okMap, errorsMap errMap, i uint) () {
	total, oks, errs := uint64(0), uint64(0), uint64(0)
	//oksTemp := make([]string, 0)
	for {
		current, more := <- filesChan
		if !more {
			log.Info(fmt.Sprintf("Worker %v - Total files: %v, Ok files: %v, errors: %v", strconv.Itoa(int(i)), total, oks, errs))
			log.Info("Finalizing worker: " + strconv.Itoa(int(i)))
			s.Lock()
			oksMap[i] = oks
			s.Unlock()
			wgp.Done()
			return
		}

		// actual processing
		var err error
		var file FileContent

		total++

		if current.Input == nil {
			file, err = processFiles(current.Filepath, dstFolder)
		} else {
			file, err = processAll(current.Input, current.Filepath, current.Size, dstFolder)
		}

		if err != nil {
			errs++
			errorsMap[current.Filepath] = err
			slog.Error(err)
		} else {
			oks++
			//oksTemp = append(oksTemp, current.Filepath)
			writesChan <- file
		}
	}
}

func processAll(buf []byte, filename string, n int64, dstFolder string) (FileContent, error) {
	var file FileContent

	memoryReader := bytes.NewReader(buf)

	cf, err := couchdbfile.New(memoryReader, n)
	if err != nil {
		slog.Error(errors.New("Error in file: " + filename))
		//slog.Error(err)
		return file, err
	}

	filename = createOutputFilename(filename, dstFolder)

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

func createWriters(writersQ uint, writesChan chan FileContent, wgw *sync.WaitGroup, woksMap okMap, werrorsMap errMap) () {
	wgw.Add(int(writersQ))
	m := make(muMap)
	var mapMutex sync.Mutex

	var s sync.Mutex

	for i := uint(0); i < writersQ; i++ {
		log.Info("Starting writer: " + strconv.Itoa(int(i)))
		go writer(writesChan, wgw, &s, woksMap, werrorsMap, i, m, &mapMutex)
	}
	return
}

func writer(writesChan chan FileContent, wgw *sync.WaitGroup, s *sync.Mutex, woksMap okMap, werrorsMap errMap, i uint, m muMap, mMutex *sync.Mutex) () {
	total, oks, errs := uint64(0), uint64(0), uint64(0)
	//oksTemp := make([]string, 0)
	for {
		current, more := <- writesChan
		if !more {
			log.Info(fmt.Sprintf("Writer %v - Total files: %v, Ok files: %v, errors: %v", strconv.Itoa(int(i)), total, oks, errs))
			log.Info("Finalizing writer: " + strconv.Itoa(int(i)))
			s.Lock()
			woksMap[i] = oks
			s.Unlock()
			wgw.Done()
			return
		}
		//log.Info("Writer " + strconv.Itoa(int(i)) + " - Writing file: " + current.Filename)

		total++

		mu := m.Get(current.Filename, mMutex)
		// actual processing
		_, err := current.mergeWriteData(mu)

		if err != nil {
			werrorsMap[current.Filename] = err
			//slog.Error(err)
		} else {
			oks++
			//oksTemp = append(oksTemp, current.Filepath)
		}
		m.Delete(current.Filename, mMutex)
	}
}