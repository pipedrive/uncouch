// Package jsonser renders JSON document from Erlang internal
// representation into normal JSON
package jsonser

import (
	"bytes"
	"fmt"
	"strconv"

	"github.com/pipedrive/uncouch/erldeser"
	"github.com/pipedrive/uncouch/erlterm"
)

// JSONSer implements JSON serialiser from provided scanner
type JSONSer struct {
	termPool []*erlterm.Term
	s        *erldeser.Scanner
}

// New will return JSON serialiser
func New(s *erldeser.Scanner) (*JSONSer, error) {
	var (
		newJSONSer JSONSer
	)
	js := &newJSONSer
	js.s = s
	return js, nil
}

const maxTermPoolSize = 500

// getTerm returns Term object, trying to reuse if possible
func (js *JSONSer) getTerm() (t *erlterm.Term) {
	if len(js.termPool) > 0 {
		t, js.termPool = js.termPool[len(js.termPool)-1], js.termPool[:len(js.termPool)-1]
	} else {
		t = new(erlterm.Term)
		t.Reset()
	}
	return t
}

// putTerm adds Term object to reuse list
func (js *JSONSer) putTerm(t *erlterm.Term) {
	if len(js.termPool) < maxTermPoolSize {
		js.termPool = append(js.termPool, t)
	}
	return
}

// WriteJSONToBuffer writes Erlang serialised JSON to given buffer as normal JSON
func (js *JSONSer) WriteJSONToBuffer(collector *bytes.Buffer) error {
	err := js.readJSONValue(collector)
	if err != nil {
		slog.Error(err)
		return err
	}
	_, err = collector.WriteString("\n")
	if err != nil {
		slog.Error(err)
		return err
	}
	return nil
}

// readJSONKeyValue reads JSON key-value pairs from Erlang serialised form
func (js *JSONSer) readJSONKeyValue(collector *bytes.Buffer) error {
	t := js.getTerm()
	defer js.putTerm(t)
	js.s.Scan(t)
	switch t.Term {
	case erldeser.SmallTupleExt:
		// read key
		err := js.readJSONKey(collector)
		if err != nil {
			slog.Error(err)
			return err
		}
		// read value
		err = js.readJSONValue(collector)
		if err != nil {
			slog.Error(err)
			return err
		}
		return nil
	default:
		err := fmt.Errorf("Erlang serialised JSON key-value pair should be inside tuple, we got %v", t.Term)
		slog.Error(err)
		slog.Debug(collector.String())
		return err
	}
}

// readJSONKey is reading Erlang encoded JSON document key
func (js *JSONSer) readJSONKey(collector *bytes.Buffer) error {
	t := js.getTerm()
	defer js.putTerm(t)
	js.s.Scan(t)
	if t.Term != erldeser.BinaryExt {
		err := fmt.Errorf("Erlang serialised JSON key should be binary, we got %v", t.Term)
		slog.Error(err)
		return err
	}
	_, err := collector.WriteString("\"")
	if err != nil {
		slog.Error(err)
		return err
	}
	_, err = collector.WriteString(string(t.Binary))
	if err != nil {
		slog.Error(err)
		return err
	}
	_, err = collector.WriteString("\":")
	if err != nil {
		slog.Error(err)
		return err
	}
	return nil
}

// readJSONValue is reading Erlang encoded JSON document value
func (js *JSONSer) readJSONValue(collector *bytes.Buffer) error {
	t := js.getTerm()
	defer js.putTerm(t)
	js.s.Scan(t)
	switch t.Term {
	case erldeser.NewFloatExt:
		_, err := collector.WriteString(strconv.FormatFloat(t.FloatValue, 'g', -1, 64))
		if err != nil {
			slog.Error(err)
			return err
		}
	case erldeser.SmallIntegerExt:
		_, err := collector.WriteString(strconv.FormatInt(int64(t.IntegerValue), 10))
		if err != nil {
			slog.Error(err)
			return err
		}
	case erldeser.IntegerExt:
		_, err := collector.WriteString(strconv.FormatInt(int64(t.IntegerValue), 10))
		if err != nil {
			slog.Error(err)
			return err
		}
	case erldeser.AtomExt:
		_, err := collector.Write(t.Binary)
		if err != nil {
			slog.Error(err)
			return err
		}
	case erldeser.SmallTupleExt:
		t := js.getTerm()
		defer js.putTerm(t)
		js.s.Scan(t)
		switch t.Term {
		case erldeser.ListExt:
			_, err := collector.WriteString("{")
			if err != nil {
				slog.Error(err)
				return err
			}
			// For each element in the list
			for i := int64(0); i < t.IntegerValue; i++ {
				err := js.readJSONKeyValue(collector)
				if err != nil {
					slog.Error(err)
					return err
				}
				if i < t.IntegerValue-1 {
					_, err = collector.WriteString(",")
					if err != nil {
						slog.Error(err)
						return err
					}
				}
			}
			// We have extra nil at the end of the list?
			t := js.getTerm()
			defer js.putTerm(t)
			js.s.Scan(t)
			if t.Term != erldeser.NilExt {
				err = fmt.Errorf("Erlang serialised list should end with extra nil, but ends with %v", t.Term)
				slog.Error(err)
				return err
			}
			_, err = collector.WriteString("}")
			if err != nil {
				slog.Error(err)
				return err
			}
			return nil
		case erldeser.NilExt:
			_, err := collector.WriteString("{}")
			if err != nil {
				slog.Error(err)
				return err
			}
		default:
			slog.Debug(collector.String())
			err := fmt.Errorf("Erlang serialised JSON object should start as tuple containing list, we got %v", t.Term)
			slog.Error(err)
			return err
		}
	case erldeser.NilExt:
		_, err := collector.WriteString("null")
		if err != nil {
			slog.Error(err)
			return err
		}
	case erldeser.StringExt:
		// Actually array of small integers!!
		_, err := collector.WriteString("[")
		if err != nil {
			slog.Error(err)
			return err
		}
		l := len(t.Binary)
		for i := 0; i < l; i++ {
			_, err = collector.WriteString(strconv.FormatInt(int64(t.Binary[i]), 10))
			if err != nil {
				slog.Error(err)
				return err
			}
			if i < l-1 {
				_, err = collector.WriteString(",")
				if err != nil {
					slog.Error(err)
					return err
				}
			}
		}
		_, err = collector.WriteString("]")
		if err != nil {
			slog.Error(err)
			return err
		}
	case erldeser.SmallBigExt:
		_, err := collector.WriteString(strconv.FormatInt(int64(t.IntegerValue), 10))
		if err != nil {
			slog.Error(err)
			return err
		}
	case erldeser.ListExt:
		_, err := collector.WriteString("[")
		if err != nil {
			slog.Error(err)
			return err
		}
		for i := int64(0); i < t.IntegerValue; i++ {
			err = js.readJSONValue(collector)
			if err != nil {
				slog.Error(err)
				return err
			}
			if i < t.IntegerValue-1 {
				_, err = collector.WriteString(",")
				if err != nil {
					slog.Error(err)
					return err
				}
			}
		}
		t := js.getTerm()
		defer js.putTerm(t)
		js.s.Scan(t)
		if t.Term != erldeser.NilExt {
			err = fmt.Errorf("Erlang serialised list should end with extra nil, but ends with %v", t.Term)
			slog.Error(err)
			return err
		}
		_, err = collector.WriteString("]")
		if err != nil {
			slog.Error(err)
			return err
		}
	case erldeser.BinaryExt:
		_, err := collector.WriteString(strconv.Quote(string(t.Binary)))
		if err != nil {
			slog.Error(err)
			return err
		}

	default:
		err := fmt.Errorf("Don't know how to turn type %v into JSON value", t.Term)
		slog.Error(err)
		return err
	}
	return nil
}
