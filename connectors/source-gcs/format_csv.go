package main

import (
	"errors"
	"fmt"
	"io"
	"slices"
	"strconv"
)

// csvRows reads a CSV or TSV file whose columns come from its header line
// or from column_names.
type csvRows struct {
	r     *csvReader
	cols  []string
	nulls []string
	close func() error
}

func newCSVRows(r io.Reader, f Format, closeFn func() error) (*csvRows, error) {
	cr := newCSVReader(r, *f.Delimiter, *f.QuoteChar, f.EscapeChar)
	if err := cr.skipLines(f.SkipRows); err != nil {
		return nil, err
	}
	c := &csvRows{r: cr, cols: f.ColumnNames, nulls: *f.NullValues, close: closeFn}
	if !*f.Header {
		return c, nil
	}
	head, err := cr.Read()
	switch {
	case errors.Is(err, io.EOF):
		return c, nil
	case err != nil:
		return nil, fmt.Errorf("header: %w", err)
	}
	if len(f.ColumnNames) > 0 {
		return c, nil
	}
	c.cols = make([]string, len(head))
	for i, h := range head {
		name := h.text
		if name == "" {
			name = "column_" + strconv.Itoa(i+1)
		}
		if slices.Contains(c.cols[:i], name) {
			return nil, fmt.Errorf("header names column %q twice", name)
		}
		c.cols[i] = name
	}
	return c, nil
}

func (c *csvRows) Next() (map[string]any, error) {
	rec, err := c.r.Read()
	if err != nil {
		var se *csvSyntaxError
		if errors.As(err, &se) {
			return nil, &badRowError{err: err}
		}
		return nil, err
	}
	if len(rec) != len(c.cols) {
		return nil, &badRowError{err: fmt.Errorf("line %d: %d fields, the header has %d", c.r.line, len(rec), len(c.cols))}
	}
	row := make(map[string]any, len(rec))
	for i, v := range rec {
		if !v.quoted && slices.Contains(c.nulls, v.text) {
			row[c.cols[i]] = nil
			continue
		}
		row[c.cols[i]] = csvText(v.text)
	}
	return row, nil
}

func (c *csvRows) Close() error { return c.close() }
