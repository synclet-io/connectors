package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

// csvWriter writes a header of the metadata columns, the declared
// properties in schema order and _airbyte_additional_properties, then one
// row per record. Every non-null value is quoted; null is an empty unquoted
// field, so it differs from an empty string.
type csvWriter struct {
	layout *streamLayout
	text   *textSink
	buf    *bufio.Writer
}

func newCSVWriter(layout *streamLayout, text *textSink) (*csvWriter, error) {
	w := &csvWriter{layout: layout, text: text, buf: bufio.NewWriter(text)}
	header := []string{colRawID, colExtractedAt}
	for _, c := range layout.columns {
		header = append(header, c.name)
	}
	header = append(header, colAdditional)
	var line []byte
	for i, h := range header {
		if i > 0 {
			line = append(line, ',')
		}
		line = appendQuoted(line, h)
	}
	line = append(line, '\n')
	if _, err := w.buf.Write(line); err != nil {
		return nil, err
	}
	return w, nil
}

func (w *csvWriter) write(r *record) error {
	line := appendQuoted(nil, r.id)
	line = append(line, ',')
	line = appendQuoted(line, strconv.FormatInt(r.extractedAt, 10))
	for _, c := range w.layout.columns {
		line = append(line, ',')
		v, null, err := csvValue(r.fields[c.name])
		if err != nil {
			return fmt.Errorf("stream %q field %q: %w", w.layout.name, c.name, err)
		}
		if !null {
			line = appendQuoted(line, v)
		}
	}
	line = append(line, ',')
	extra, err := additionalProperties(w.layout, r.fields)
	if err != nil {
		return fmt.Errorf("stream %q: %w", w.layout.name, err)
	}
	if extra != nil {
		line = appendQuoted(line, string(extra))
	}
	line = append(line, '\n')
	_, err = w.buf.Write(line)
	return err
}

// csvValue is a string's text, or the compact JSON text of any other value.
func csvValue(raw json.RawMessage) (string, bool, error) {
	if isNull(raw) {
		return "", true, nil
	}
	raw = bytes.TrimSpace(raw)
	if raw[0] == '"' {
		var s string
		if err := json.Unmarshal(raw, &s); err != nil {
			return "", false, fmt.Errorf("value %s: %w", raw, err)
		}
		return s, false, nil
	}
	var b bytes.Buffer
	if err := json.Compact(&b, raw); err != nil {
		return "", false, fmt.Errorf("value %s: %w", raw, err)
	}
	return b.String(), false, nil
}

func appendQuoted(dst []byte, s string) []byte {
	dst = append(dst, '"')
	dst = append(dst, strings.ReplaceAll(s, `"`, `""`)...)
	return append(dst, '"')
}

func (w *csvWriter) size() int64 {
	return w.text.n + int64(w.buf.Buffered())
}

func (w *csvWriter) close() error {
	return closeAll(w.buf.Flush, w.text)
}
