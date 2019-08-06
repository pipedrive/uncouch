package tar

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"fmt"
	"github.com/pipedrive/uncouch/config"
	"io"
	"os"
	"path/filepath"
	"strings"
)

type UntarredFile struct {
	Filepath string
	Input io.ReadSeeker
	Size int64
}

type Done struct {
	Res bool
	FileQ uint32
}


func Tar(src string, buf io.Writer) error {

	zr := gzip.NewWriter(buf)
	tw := tar.NewWriter(zr)


	err := filepath.Walk(src, func(file string, fi os.FileInfo, err error) error {
		relativePath, err := filepath.Rel(config.TEMP_OUTPUT_DIR, file)

		header, err := tar.FileInfoHeader(fi, relativePath)
		if err != nil {
			slog.Error(err)
			return err
		}

		header.Name = relativePath //filepath.ToSlash(file)

		// write header
		if err := tw.WriteHeader(header); err != nil {
			slog.Error(err)
			return err
		}
		// if not a dir, write file content
		if !fi.IsDir() {
			data, err := os.Open(file)
			if err != nil {
				slog.Error(err)
				return err
			}
			if _, err := io.Copy(tw, data); err != nil {
				slog.Error(err)
				return err
			}
		}
		return nil
	})
	if err != nil {
		slog.Error(err)
		return err
	}

	// produce tar
	if err := tw.Close(); err != nil {
		slog.Error(err)
		return err
	}
	// produce gzip
	if err := zr.Close(); err != nil {
		slog.Error(err)
	}

	return err
}


func Untar(dst string, r io.Reader, filesChan chan UntarredFile, done chan Done) () {

	fileQ := uint32(0)
	gzr, err := gzip.NewReader(r)
	if err != nil {
		slog.Error(err)
		done <- Done{Res:false, FileQ:fileQ}
		return
	}
	defer gzr.Close()
	tr := tar.NewReader(gzr)

	for {
		header, err := tr.Next()

		switch {

		// if no more files are found return
		case err == io.EOF:
			for i := 0; i < config.WORKERS_Q; i++ {
				filesChan <- UntarredFile{Filepath:"finished", Input: nil, Size: 0}
			}
			done <- Done{Res:true, FileQ:fileQ}
			return

		// return any other error
		case err != nil:
			slog.Error(err)
			done <- Done{Res:false, FileQ:fileQ}
			return

		// if the header is nil, just skip it (not sure how this happens)
		case header == nil:
			continue
		}
		// the target location where the dir/file should be created
		target := filepath.Join(dst, header.Name)

		// check the file type
		switch header.Typeflag {

		// if its a dir and it doesn't exist create it
		case tar.TypeDir:
			if _, err := os.Stat(target); err != nil {
				if err := os.MkdirAll(target, 0755); err != nil {
					slog.Error(err)
					done <- Done{Res:false, FileQ:fileQ}
					return
				}
			}

			// Create directory in output folder too.
			if _, err := os.Stat(strings.Replace(target, config.TEMP_INPUT_DIR, config.TEMP_OUTPUT_DIR, 1)); err != nil {
				if err := os.MkdirAll(strings.Replace(target, config.TEMP_INPUT_DIR, config.TEMP_OUTPUT_DIR, 1), 0755); err != nil {
					slog.Error(err)
					done <- Done{Res: false, FileQ: fileQ}
					return
				}
			}

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

				filesChan <- f
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
	f.Close()
	return nil
}

func processUntarredFile(target string, tr *tar.Reader, header *tar.Header, done chan Done) (UntarredFile) {
	var f UntarredFile

	buf := make([]byte, header.Size)
	fmt.Printf("File: %s, Size: %v.\n", target, header.Size)
	_, err := tr.Read(buf)
	if err != nil && err != io.EOF {
		//err := errors.New("End of file not reached while reading file " + header.Name + ".")
		slog.Error(err)
		done <- Done{Res:false, FileQ:0}
		return f
	}
	//fmt.Println("Remain: " + string(remain))
	/*if remain != 0 {
		err := errors.New("File " + header.Name + " not completely read.")
		slog.Error(err)
		done <- false
		return
	}*/

	f.Filepath = target
	f.Input = bytes.NewReader(buf)
	f.Size = header.Size

	return f
}