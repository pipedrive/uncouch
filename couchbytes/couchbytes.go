// Package couchbytes provides routines to read CoucDB blocks
// into byte slices working around 4K block and compression.
package couchbytes

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/golang/snappy"
	"github.com/pipedrive/uncouch/leakybucket"
)

const (
	// BlockAlignment is block size for DB Header start
	BlockAlignment = 4096
	snappyPrefix   = 1
	magicNumber    = 131
	deflateSuffix  = 80
)

// ReadDbHeader reads byte buffer containing serialised DB header element
func ReadDbHeader(input io.ReadSeeker, offset int64) (*[]byte, error) {
	dataSize, bytesSkipped, err := readUint32Skip4K(input, offset)
	if err != nil {
		slog.Error(err)
		return nil, err
	}
	buf, _, err := readAndSkip4K(input, offset+4+bytesSkipped, dataSize)
	if err != nil {
		slog.Error(err)
		return nil, err
	}
	bufReader := bytes.NewReader(*buf)
	var md5Hash [16]byte
	err = binary.Read(bufReader, binary.BigEndian, &md5Hash)
	if err != nil {
		slog.Error(err)
		return nil, err
	}
	var magicNumber uint8
	binary.Read(bufReader, binary.BigEndian, &magicNumber)
	if err != nil {
		slog.Error(err)
		return nil, err
	}
	t := (*buf)[17:]
	return &t, nil
}

// ReadNode reads byte buffer containing serialised Node element
func ReadNode(input io.ReadSeeker, offset int64) (*[]byte, error) {
	dataSize, bytesSkipped, err := readUint32Skip4K(input, offset)
	if err != nil {
		slog.Error(err)
		return nil, err
	}
	buf, _, err := readAndSkip4K(input, offset+4+bytesSkipped, dataSize)
	if err != nil {
		slog.Error(err)
		return nil, err
	}
	return uncompressBuffer(buf)
}

// ReadDocument reads the actual stored document and returns respective buffer
func ReadDocument(input io.ReadSeeker, offset int64) (*[]byte, error) {
	combinedSize, bytesSkipped, err := readUint32Skip4K(input, offset)
	if err != nil {
		slog.Error(err)
		return nil, err
	}
	md5Flag := (combinedSize & (1 << 31)) >> 31
	dataSize := combinedSize &^ (1 << 31)
	// slog.Debugf("Offset: %v md5Flag: %v dataSize: %v", offset, md5Flag, dataSize)
	if md5Flag != 1 {
		err := fmt.Errorf("Unknown document block header %v", md5Flag)
		slog.Error(err)
		return nil, err
	}
	buf, _, err := readAndSkip4K(input, offset+4+bytesSkipped, dataSize+16)
	if err != nil {
		slog.Error(err)
		return nil, err
	}
	bufReader := *bytes.NewReader(*buf)
	/*
		md5Hash := (*buf)[:16]
		slog.Debug(hex.EncodeToString(md5Hash))
	*/
	bufReader.Seek(20, io.SeekStart)
	var docSize uint32
	err = binary.Read(&bufReader, binary.BigEndian, &docSize)
	if err != nil {
		slog.Error(err)
		return nil, err
	}
	docSlice := (*buf)[24 : docSize+24]
	docBytes, err := uncompressBuffer(&docSlice)
	if err != nil {
		slog.Error(err)
		return nil, err
	}
	return docBytes, nil
}

// uncompressBuffer uncompresses buffer if needed
func uncompressBuffer(buf *[]byte) (*[]byte, error) {
	bufReader := *bytes.NewReader(*buf)
	var b uint8
	err := binary.Read(&bufReader, binary.BigEndian, &b)
	if err != nil {
		slog.Error(err)
		return nil, err
	}
	switch b {
	case snappyPrefix:
		// slog.Debug("Snappy compressed node")
		destBuf := leakybucket.Get(int32(len(*buf) * 5))
		// Uncompress and go
		res, err := snappy.Decode(*destBuf, (*buf)[1:])
		if err != nil {
			slog.Error("Error decoding snappy", err)
			return nil, err
		}
		// Release compressed buffer
		leakybucket.Put(buf)
		// Skip the Magic Marker
		res = res[1:]
		return &res, nil
	case magicNumber:
		err = binary.Read(&bufReader, binary.BigEndian, &b)
		if err != nil {
			slog.Error(err)
			return nil, err
		}
		if b == deflateSuffix {
			slog.Debug("Deflate compressed node")
			err := fmt.Errorf("Deflate un-compression is not implemented yet")
			slog.Error(err)
			return nil, err
		}
		slog.Debug("Uncompressed node")
		t := (*buf)[1:]
		return &t, nil
	default:
		err := fmt.Errorf("Unknown block prefix %v", b)
		slog.Error(err)
		return nil, err
	}
}

// readInt32Skip4K reads data into 32 bit uint32 and skips 4K hole
func readUint32Skip4K(input io.ReadSeeker, offset int64) (uint32, int64, error) {
	buf, bytesSkipped, err := readAndSkip4K(input, offset, 4)
	if err != nil {
		slog.Error(err)
		return 0, 0, err
	}
	bufReader := *bytes.NewReader(*buf)
	var result uint32
	err = binary.Read(&bufReader, binary.BigEndian, &result)
	if err != nil {
		slog.Error(err)
		return 0, 0, err
	}
	return result, bytesSkipped, nil
}

// ReadAndSkip4K reads data into bye slice and skips 4K holes
func readAndSkip4K(input io.ReadSeeker, offset int64, dataSize uint32) (*[]byte, int64, error) {
	// We need to work around CouchDB storage system where 4K aligned bytes
	// need to be removed before processing
	_, err := input.Seek(offset, io.SeekStart)
	if err != nil {
		if err != io.EOF {
			slog.Error("Error on seeking while reading buffer.", err)
			return nil, 0, err
		}
	}
	// Get lower bound of 4K multiplier to offset
	lowerBound := offset / int64(BlockAlignment)
	if offset%int64(BlockAlignment) == 0 {
		lowerBound--
	}
	// Get upper bound of 4K multiplier to offset
	upperBound := (offset + int64(dataSize)) / int64(BlockAlignment)

	// Read into byte array
	buf := leakybucket.Get(int32(dataSize) + int32(upperBound-lowerBound))
	_, err = input.Read(*buf)
	if err != nil {
		if err != io.EOF {
			slog.Error("Error reading buffer.", err)
			return nil, 0, err
		}
	}
	for i := upperBound; i > lowerBound; i-- {
		// Cycle from back to forward and remove byte on 4K boundary
		t := append((*buf)[:(i*BlockAlignment-offset)], (*buf)[(i*BlockAlignment-offset)+1:]...)
		buf = &t
	}
	return buf, upperBound - lowerBound, nil
}
