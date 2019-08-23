package tar

import (
	"archive/tar"
	"compress/gzip"
	"io"
	"io/ioutil"
	"os"
	"path"
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
	Err error
	FileQ uint32
}

func Untar(dst string, r io.Reader, filesChan chan *UntarredFile, done chan Done) () {

	log.Info("Starting untar process.")
	fileQ := uint32(0)
/*	gzr, err := gzip.NewReader(r)
	if err != nil {
		slog.Error(err)
		done <- Done{Res:false, FileQ:fileQ}
		return
	}
	defer gzr.Close()*/

	tr := tar.NewReader(r)

	writeLocalFile := dst != ""

	if writeLocalFile {
		if _, err := os.Stat(dst); err != nil {
			if err := os.MkdirAll(dst, 0755); err != nil {
				slog.Error(err)
				done <- Done{Res: false, Err: err, FileQ: fileQ}
				return
			}
		}
	}

	for {
		header, err := tr.Next()

		switch {

		// if no more files are found return
		case err == io.EOF:
			close(filesChan)
			done <- Done{Res:true, Err: nil, FileQ:fileQ}
			return

		// return any other error
		case err != nil:
			slog.Error(err)
			done <- Done{Res:false, Err: err, FileQ:fileQ}
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
			if writeLocalFile {
				if _, err := os.Stat(target); err != nil {
					if err := os.MkdirAll(target, 0755); err != nil {
						//slog.Error(err)
						done <- Done{Res:false, Err: err, FileQ:fileQ}
						return
					}
				}
			}
			continue

		// if it's a file create it
		case tar.TypeReg:
			if !strings.HasSuffix(header.Name, ".couch") && !strings.HasSuffix(header.Name, ".couch.gz") {
				continue
			}
			_, ff := path.Split(header.Name)
			if strings.HasPrefix(ff, "_") {
				continue
			}
			if header.Size == 0 {
				log.Info("Skipping file " + header.Name + " - Size 0.")
				continue
			}

			// Separate if logic.
			f, err := writeToDest(writeLocalFile, target, tr, header)
			if err != nil {
				//slog.Error(err)
				done <- Done{Res:false, Err: err, FileQ:fileQ}
				return
			}

			if f.Size == 0 {
				log.Info("Skipping file " + header.Name + " - Target: " + target + " - Size 0.")
				continue
			}

			fileQ++

			filesChan <- f
			//fmt.Println("Untarred file: " + header.Name)

		}
	}
}



func replaceGz(target string) (string) {
	return strings.Replace(target,".couch.gz", ".couch", 1)
}

func writeToDest(writeLocalFile bool, target string, tr *tar.Reader, header *tar.Header) (*UntarredFile, error) {

	if strings.HasSuffix(header.Name, ".couch.gz") {
		target = replaceGz(target)
		//------------> This is the code to write the files to disk.
		if writeLocalFile {
			var f UntarredFile
			written, err := writeUntarredFileGz(target, tr, header)
			if err != nil {
				//slog.Error(err)
				return &f, err
			}

			f.Filepath = target
			f.Size = written

			return &f, err

			// ENDS HERE.
		} else {
			//------------> This is the code to process the files without writing to disk.
			f, err := processUntarredFileGz(target, tr, header)
			if err != nil {
				//slog.Error(err)
				return f, err
			}
			return f, err
			// ENDS HERE.
		}
	} else {
		//------------> This is the code to write the files to disk.
		if writeLocalFile {
			var f UntarredFile
			written, err := writeUntarredFile(target, tr, header)
			if err != nil {
				//slog.Error(err)
				return &f, err
			}

			f.Filepath = target
			f.Size = written

			return &f, err
			// ENDS HERE.
		} else {
			//------------> This is the code to process the files without writing to disk.
			f, err := processUntarredFile(target, tr, header)
			if err != nil {
				//slog.Error(err)
				return f, err
			}

			return f, err
			// ENDS HERE.
		}
	}
}

func writeUntarredFileGz(target string, tr *tar.Reader, header *tar.Header) (int64, error) {
	f, err := os.OpenFile(target, os.O_CREATE|os.O_RDWR, os.FileMode(header.Mode))
	if err != nil {
		return 0, err
	}

	gzr, err := gzip.NewReader(tr)
	if err != nil {
		//slog.Error(err)
		return 0, err
	}
	defer gzr.Close()

	written, err := io.Copy(f, gzr)
	if err != nil {
		return 0, err
	}

	return written, f.Close()
}

func writeUntarredFile(target string, tr *tar.Reader, header *tar.Header) (int64, error) {
	f, err := os.OpenFile(target, os.O_CREATE|os.O_RDWR, os.FileMode(header.Mode))
	if err != nil {
		return 0, err
	}

	written, err := io.Copy(f, tr)
	if err != nil {
		return 0, err
	}

	return written, f.Close()
}

func processUntarredFileGz(target string, tr *tar.Reader, header *tar.Header) (*UntarredFile, error) {
	var f UntarredFile

	gzr, err := gzip.NewReader(tr)
	if err != nil {
		//slog.Error(err)
		return &f, err
	}
	defer gzr.Close()

	//log.Info(fmt.Sprintf("File: %s, Size: %v.", target, header.Size))
	buf, err := ioutil.ReadAll(gzr)
	if err != nil && err != io.EOF {
		//slog.Error(err)
		return &f, err
	}

	f.Filepath = target
	f.Input = buf
	f.Size = int64(len(buf))

	return &f, err
}

func processUntarredFile(target string, tr *tar.Reader, header *tar.Header) (*UntarredFile, error) {
	var f UntarredFile

	//log.Info(fmt.Sprintf("File: %s, Size: %v.", target, header.Size))
	buf, err := ioutil.ReadAll(tr)
	if err != nil && err != io.EOF {
		//slog.Error(err)
		return &f, err
	}

	f.Filepath = target
	f.Input = buf
	f.Size = int64(len(buf))

	return &f, err
}
