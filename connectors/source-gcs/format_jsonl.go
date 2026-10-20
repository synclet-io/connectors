package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

// jsonlRows reads one JSON object per line; blank lines are skipped.
type jsonlRows struct {
	r     *bufio.Reader
	line  int
	close func() error
}

func newJSONLRows(r io.Reader, closeFn func() error) *jsonlRows {
	return &jsonlRows{r: bufio.NewReaderSize(r, 64*1024), close: closeFn}
}

func (j *jsonlRows) Next() (map[string]any, error) {
	for {
		b, err := j.r.ReadBytes('\n')
		if len(b) == 0 && err != nil {
			return nil, err
		}
		if err != nil && !errors.Is(err, io.EOF) {
			return nil, err
		}
		j.line++
		b = bytes.TrimSpace(b)
		if len(b) == 0 {
			continue
		}
		return decodeJSONLine(b, j.line)
	}
}

func decodeJSONLine(b []byte, line int) (map[string]any, error) {
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, &badRowError{err: fmt.Errorf("line %d: %w", line, err)}
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return nil, &badRowError{err: fmt.Errorf("line %d: more than one JSON value", line)}
	}
	m, ok := v.(map[string]any)
	if !ok {
		return nil, &badRowError{err: fmt.Errorf("line %d: %s, not a JSON object", line, jsonType(v))}
	}
	return m, nil
}

func (j *jsonlRows) Close() error { return j.close() }
