package main

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"sort"

	"go.uber.org/multierr"
)

// record is one RECORD message ready to encode.
type record struct {
	id          string
	extractedAt int64 // milliseconds since the epoch
	data        json.RawMessage
	fields      map[string]json.RawMessage
}

// fileWriter encodes the records of one output file.
type fileWriter interface {
	write(r *record) error
	// size is the number of bytes written before compression.
	size() int64
	// close finishes the file; the underlying writer then holds every byte.
	close() error
}

// streamLayout is what an encoder needs to know about the stream.
type streamLayout struct {
	name    string
	columns []column
	// declared holds every declared column name.
	declared map[string]bool
}

func newStreamLayout(name string, cols []column) *streamLayout {
	declared := make(map[string]bool, len(cols))
	for _, c := range cols {
		declared[c.name] = true
	}
	return &streamLayout{name: name, columns: cols, declared: declared}
}

// newFileWriter returns the encoder of the configured format writing to w.
func newFileWriter(f Format, layout *streamLayout, w io.Writer) (fileWriter, error) {
	switch f.FormatType {
	case formatJSONL:
		return &jsonlWriter{text: newTextSink(w, f.Compression)}, nil
	case formatCSV:
		return newCSVWriter(layout, newTextSink(w, f.Compression))
	case formatParquet:
		return newParquetWriter(f, layout, w)
	default:
		return nil, fmt.Errorf("unsupported format_type %q", f.FormatType)
	}
}

// textSink counts the bytes of a text format and optionally gzips them.
type textSink struct {
	out io.Writer
	gz  *gzip.Writer
	n   int64
}

func newTextSink(w io.Writer, compression string) *textSink {
	s := &textSink{out: w}
	if compression == compressionGzip {
		s.gz = gzip.NewWriter(w)
		s.out = s.gz
	}
	return s
}

func (s *textSink) Write(p []byte) (int, error) {
	n, err := s.out.Write(p)
	s.n += int64(n)
	return n, err
}

func (s *textSink) close() error {
	if s.gz != nil {
		return s.gz.Close()
	}
	return nil
}

// additionalProperties returns the record's undeclared fields as one JSON
// object with sorted keys, or nil when there are none.
func additionalProperties(layout *streamLayout, fields map[string]json.RawMessage) (json.RawMessage, error) {
	var keys []string
	for k := range fields {
		if !layout.declared[k] {
			keys = append(keys, k)
		}
	}
	if len(keys) == 0 {
		return nil, nil
	}
	sort.Strings(keys)
	var b bytes.Buffer
	b.WriteByte('{')
	for i, k := range keys {
		if i > 0 {
			b.WriteByte(',')
		}
		name, err := json.Marshal(k)
		if err != nil {
			return nil, err
		}
		b.Write(name)
		b.WriteByte(':')
		if err := json.Compact(&b, fields[k]); err != nil {
			return nil, fmt.Errorf("field %q: %w", k, err)
		}
	}
	b.WriteByte('}')
	return b.Bytes(), nil
}

func isNull(raw json.RawMessage) bool {
	raw = bytes.TrimSpace(raw)
	return len(raw) == 0 || string(raw) == "null"
}

// closeAll closes the text sink after the encoder's own close.
func closeAll(first func() error, sink *textSink) (rerr error) {
	defer multierr.AppendInvoke(&rerr, multierr.Invoke(sink.close))
	return first()
}
