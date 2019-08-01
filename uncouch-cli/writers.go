package main

import (
	"fmt"
	"os"
	"path"
	"strings"

	"github.com/pipedrive/uncouch/aws"
	"github.com/pipedrive/uncouch/couchdbfile"
	"github.com/pipedrive/uncouch/leakybucket"
	)

func fileWriter(str *strings.Builder, filename string) (error) {
	f, err := os.Create(filename)
	if err != nil {
		slog.Error(err)
		return err
	}
	defer f.Close()

	_, err = f.WriteString(str.String())
	if err != nil {
		slog.Error(err)
		return err
	}
	return nil
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
	var str strings.Builder

	err := processSeqNode(cf, cf.Header.SeqTreeState.Offset, &str)
	if err != nil {
		slog.Error(err)
		return err
	}

	if strings.HasPrefix(filename, "s3://") {
		err = aws.S3FileWriter(&str, filename)
	} else {
		err = fileWriter(&str, filename)
	}

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
	}
}

func processSeqNode(cf *couchdbfile.CouchDbFile, offset int64, str *strings.Builder) error {
	for {
		kpNode, kvNode, err := cf.ReadSeqNode(offset)
		if err != nil {
			slog.Error(err)
			return err
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
			str.Write(output.Bytes())
			// fmt.Print(output.String())
			leakybucket.PutBuffer(output)
			return nil
		}
	}
}