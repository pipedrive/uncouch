package tar

import (
	"archive/tar"
	"compress/gzip"
	"github.com/pipedrive/uncouch/config"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
)

type UntarredFile struct {
	Filepath string
	Input []byte
	Size int64
}

type Done struct {
	Res bool
	FileQ uint32
}


func Untar(dst string, r io.Reader, filesChan chan *UntarredFile, done chan Done) () {

	fileQ := uint32(0)
	gzr, err := gzip.NewReader(r)
	if err != nil {
		slog.Error(err)
		done <- Done{Res:false, FileQ:fileQ}
		return
	}
	defer gzr.Close()
	tr := tar.NewReader(gzr)

	if _, err := os.Stat(dst); err != nil {
		if err := os.MkdirAll(dst, 0755); err != nil {
			slog.Error(err)
			done <- Done{Res: false, FileQ: fileQ}
			return
		}
	}

	for {
		header, err := tr.Next()

		switch {

		// if no more files are found return
		case err == io.EOF:
			close(filesChan)
			done <- Done{Res:true, FileQ:fileQ}
			return

		// return any other error
		case err != nil:
			slog.Error(err)
			done <- Done{Res:false, FileQ:fileQ}
			return

		// if the header is nil, just skip it.
		case header == nil:
			continue
		}

		// the target location where the dir/file should be created
		target := filepath.Join(dst, header.Name)

		// check the file type
		switch header.Typeflag {

		// if its a dir and it doesn't exist create it
		case tar.TypeDir:
			if config.WRITE_LOCAL_FILE_FLAG {
				if _, err := os.Stat(target); err != nil {
					if err := os.MkdirAll(target, 0755); err != nil {
						slog.Error(err)
						done <- Done{Res:false, FileQ:fileQ}
						return
					}
				}
			}
			continue

		// if it's a file create it
		case tar.TypeReg:
			if !strings.HasSuffix(header.Name, ".couch") {
				continue
			}
			fileQ++
			//------------> This is the code to write the files to disk.
			if (config.WRITE_LOCAL_FILE_FLAG) {
				err := writeUntarredFile(target, tr, header)
				if err != nil {
					slog.Error(err)
					done <- Done{Res:false, FileQ:fileQ}
					return
				}

				var f UntarredFile
				f.Filepath = target

				filesChan <- &f
				// ENDS HERE.
			} else {
				//------------> This is the code to process the files without writing to disk.
				f := processUntarredFile(target, tr, header, done)
				filesChan <- f
				// ENDS HERE.
			}
		}
	}
}

func writeUntarredFile(target string, tr *tar.Reader, header *tar.Header) (error) {
	f, err := os.OpenFile(target, os.O_CREATE|os.O_RDWR, os.FileMode(header.Mode))
	if err != nil {
		return err
	}

	// copy contents
	if _, err := io.Copy(f, tr); err != nil {
		return err
	}

	// manually close here after each file operation; defering would cause each file close
	// to wait until all operations have completed.
	return f.Close()
}

func processUntarredFile(target string, tr *tar.Reader, header *tar.Header, done chan Done) (*UntarredFile) {
	var f UntarredFile

	//log.Info(fmt.Sprintf("File: %s, Size: %v.", target, header.Size))
	buf, err := ioutil.ReadAll(tr)
	if err != nil && err != io.EOF {
		slog.Error(err)
		done <- Done{Res:false, FileQ:0}
		return &f
	}

	f.Filepath = target
	f.Input = buf
	f.Size = header.Size

	return &f
}
