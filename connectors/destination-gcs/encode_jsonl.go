package main

import (
	"bufio"
	"encoding/json"
	"strconv"
)

// jsonlWriter writes one object per line: the record's id, its extraction
// time and its data bytes verbatim under _airbyte_data.
type jsonlWriter struct {
	text *textSink
	buf  *bufio.Writer
}

func (w *jsonlWriter) write(r *record) error {
	if w.buf == nil {
		w.buf = bufio.NewWriter(w.text)
	}
	id, err := json.Marshal(r.id)
	if err != nil {
		return err
	}
	line := make([]byte, 0, len(r.data)+96)
	line = append(line, `{"`+colRawID+`":`...)
	line = append(line, id...)
	line = append(line, `,"`+colExtractedAt+`":`...)
	line = strconv.AppendInt(line, r.extractedAt, 10)
	line = append(line, `,"`+colData+`":`...)
	line = append(line, r.data...)
	line = append(line, '}', '\n')
	_, err = w.buf.Write(line)
	return err
}

func (w *jsonlWriter) size() int64 {
	n := w.text.n
	if w.buf != nil {
		n += int64(w.buf.Buffered())
	}
	return n
}

func (w *jsonlWriter) close() error {
	return closeAll(func() error {
		if w.buf == nil {
			return nil
		}
		return w.buf.Flush()
	}, w.text)
}
