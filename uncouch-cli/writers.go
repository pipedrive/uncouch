package main

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"github.com/pipedrive/uncouch/config"
	"github.com/pipedrive/uncouch/couchdbfile"
	"github.com/pipedrive/uncouch/leakybucket"
	"os"
	"path"
	"strings"
)

type FileContent struct {
	Cf       *couchdbfile.CouchDbFile
	Filename string
}

type FileCompressor struct {
	f  *os.File
	gf *gzip.Writer
	fw *bufio.Writer
}

func CreateGzipFile(name string) (f FileCompressor, err error) {
	fi, err := os.Create(name)
	if err != nil {
		slog.Error(err)
		panic(err)
	}
	gf := gzip.NewWriter(fi)
	fw := bufio.NewWriter(gf)
	f = FileCompressor{fi, gf, fw}
	return f, err
}

func WriteGzipFile(f FileCompressor, str *strings.Builder) error {
	_, err := (f.fw).WriteString(str.String())
	if err != nil {
		slog.Error(err)
	}
	return err
}

func CloseGzipFile(f FileCompressor) error {
	err := f.fw.Flush()
	if err != nil {
		slog.Error(err)
		return err
	}

	// Close the gzip first.
	err = f.gf.Close()
	if err != nil {
		slog.Error(err)
		return err
	}

	err = f.f.Close()
	if err != nil {
		slog.Error(err)
	}

	return err
}

func gzipFileWriter(str *strings.Builder, filename string) error {
	f, err := CreateGzipFile(filename)
	if err != nil {
		slog.Error(err)
		return err
	}

	err = WriteGzipFile(f, str)
	if err != nil {
		slog.Error(err)
		return err
	}

	err = CloseGzipFile(f)
	if err != nil {
		slog.Error(err)
	}

	return err
}

func fileWriter(str *strings.Builder, filename string) error {
	f, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		slog.Error(err)
		return err
	}

	_, err = f.WriteString(str.String())
	if err != nil {
		slog.Error(err)
		return err
	}
	return f.Close()
}

func writeHeaders(cf *couchdbfile.CouchDbFile, outputdir string) error {
	err := dumpIDNodeHeaders(cf, cf.Header.IDTreeState.Offset, outputdir)
	if err != nil {
		slog.Error(err)
		return err
	}
	err = dumpSeqNodeHeaders(cf, cf.Header.SeqTreeState.Offset, outputdir)
	if err != nil {
		slog.Error(err)
		return err
	}
	return nil
}

func writeNodeToFile(cf *couchdbfile.CouchDbFile, offset int64, filename string) error {
	f, err := os.Create(filename)
	if err != nil {
		slog.Error(err)
		return err
	}
	defer f.Close()
	buf, err := cf.ReadNodeBytes(offset)
	if err != nil {
		slog.Error(err)
		return err
	}
	defer leakybucket.PutBytes(buf)
	_, err = f.Write(*buf)
	if err != nil {
		slog.Error(err)
		return err
	}
	return nil
}

func dumpIDNodeHeaders(cf *couchdbfile.CouchDbFile, offset int64, outputdir string) error {
	for {
		kpNode, kvNode, err := cf.ReadIDNode(offset)
		if err != nil {
			slog.Error(err)
			return err
		}
		if kpNode != nil {
			filename := fmt.Sprintf("id-kp-%d.bin", offset)
			err := writeNodeToFile(cf, offset, path.Join(outputdir, filename))
			if err != nil {
				slog.Error(err)
				return err
			}
			// Pointer node, dig deeper
			for _, node := range kpNode.Pointers {
				err = dumpIDNodeHeaders(cf, node.Offset, outputdir)
				if err != nil {
					slog.Error(err)
					return err
				}
			}
			return nil
		} else if kvNode != nil {
			filename := fmt.Sprintf("id-kv-%d.bin", offset)
			err := writeNodeToFile(cf, offset, path.Join(outputdir, filename))
			if err != nil {
				slog.Error(err)
				return err
			}
			return nil
		}
	}
}

func dumpSeqNodeHeaders(cf *couchdbfile.CouchDbFile, offset int64, outputdir string) error {
	for {
		kpNode, kvNode, err := cf.ReadSeqNode(offset)
		if err != nil {
			slog.Error(err)
			return err
		}
		if kpNode != nil {
			filename := fmt.Sprintf("seq-kp-%d.bin", offset)
			err := writeNodeToFile(cf, offset, path.Join(outputdir, filename))
			if err != nil {
				slog.Error(err)
				return err
			}
			// Pointer node, dig deeper
			for _, node := range kpNode.Pointers {
				err = dumpSeqNodeHeaders(cf, node.Offset, outputdir)
				if err != nil {
					slog.Error(err)
					return err
				}
			}
			return nil
		} else if kvNode != nil {
			filename := fmt.Sprintf("seq-kv-%d.bin", offset)
			err := writeNodeToFile(cf, offset, path.Join(outputdir, filename))
			if err != nil {
				slog.Error(err)
				return err
			}
			return nil
		}
	}
}

func writeData(cf *couchdbfile.CouchDbFile, filename string) error {
	str := leakybucket.GetStrBuilder()

	err := processSeqNode(cf, cf.Header.SeqTreeState.Offset, str)
	if err != nil {
		slog.Error("Error in file:" + filename)
		slog.Error(err)
		return err
	}

	if config.COMPRESS_OUTPUT {
		err = gzipFileWriter(str, filename)
	} else {
		err = fileWriter(str, filename)
	}

	if err != nil {
		slog.Error(err)
	}
	leakybucket.PutStrBuilder(str)

	// return processIDNode(cf, cf.Header.IDTreeState.Offset)
	// slog.Debug(termite.GetProfilerData())
	return err
}

func processIDNode(cf *couchdbfile.CouchDbFile, offset int64) error {
	for {
		kpNode, kvNode, err := cf.ReadIDNode(offset)
		if err != nil {
			slog.Error(err)
			return err
		}
		if kpNode != nil {
			// Pointer node, dig deeper
			for _, node := range kpNode.Pointers {
				err = processIDNode(cf, node.Offset)
				if err != nil {
					slog.Error(err)
					return err
				}
			}
			return nil
		} else if kvNode != nil {
			output := leakybucket.GetBuffer()
			for _, document := range kvNode.Documents {
				err = cf.WriteDocument(&document, output)
				if err != nil {
					slog.Error(err)
					return err
				}
			}
			fmt.Print(output.String())
			leakybucket.PutBuffer(output)
			return nil
		}
		return nil
	}
}

func processSeqNode(cf *couchdbfile.CouchDbFile, offset int64, str *strings.Builder) error {
	for {
		kpNode, kvNode, err := cf.ReadSeqNode(offset)
		if err != nil {
			slog.Error(err)
			return err
		}
		if kpNode != nil && kvNode != nil {
			log.Info("Empty Node.")
		}
		if kpNode != nil {
			// Pointer node, dig deeper
			for _, node := range kpNode.Pointers {
				err = processSeqNode(cf, node.Offset, str)
				if err != nil {
					slog.Error(err)
					return err
				}
			}
			return nil
		} else if kvNode != nil {
			output := leakybucket.GetBuffer()
			for _, document := range kvNode.Documents {
				err = cf.WriteDocument(&document, output)
				if err != nil {
					slog.Error(err)
					return err
				}
			}
			str.Grow(len(output.Bytes()))
			str.Write(output.Bytes())
			// fmt.Print(output.String())
			leakybucket.PutBuffer(output)
			return nil
		}
		return nil
	}
}
