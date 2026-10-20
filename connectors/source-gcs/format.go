package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
)

// rowSource yields the rows of one file as field name to value: nil,
// bool, json.Number, string, map[string]any, []any, or csvText for a CSV
// cell.
type rowSource interface {
	// Next returns the next row, io.EOF after the last one, or a
	// *badRowError for a row it could not read and skipped.
	Next() (map[string]any, error)
	Close() error
}

// schemaDeclarer is a rowSource whose file carries its own schema, which
// inference takes instead of looking at values.
type schemaDeclarer interface {
	declare(in *inference)
}

// badRowError is a row that cannot be read; the file's next row can.
type badRowError struct{ err error }

func (e *badRowError) Error() string { return e.err.Error() }
func (e *badRowError) Unwrap() error { return e.err }

// openRows opens an object and reads it in the stream's format after
// decompressing it.
func openRows(ctx context.Context, store objectStore, s StreamConfig, key string) (rowSource, error) {
	body, err := store.Open(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("open %q: %w", key, err)
	}
	dec, err := decompress(body, s.Compression, key)
	if err != nil {
		return nil, errors.Join(fmt.Errorf("decompress %q: %w", key, err), body.Close())
	}
	closeAll := func() error { return errors.Join(dec.Close(), body.Close()) }
	var rs rowSource
	switch s.Format.Filetype {
	case formatCSV, formatTSV:
		rs, err = newCSVRows(dec, s.Format, closeAll)
	case formatJSONL:
		rs = newJSONLRows(dec, closeAll)
	case formatParquet:
		rs, err = openParquetRows(dec, closeAll)
	default:
		err = fmt.Errorf("unknown format %q", s.Format.Filetype)
	}
	if err != nil {
		return nil, errors.Join(fmt.Errorf("read %q: %w", key, err), closeAll())
	}
	return rs, nil
}

// spool copies r into a temporary file, which the caller removes.
func spool(r io.Reader) (*os.File, int64, error) {
	f, err := os.CreateTemp("", "source-gcs-*")
	if err != nil {
		return nil, 0, err
	}
	n, err := io.Copy(f, r)
	if err != nil {
		return nil, 0, errors.Join(err, f.Close(), os.Remove(f.Name()))
	}
	return f, n, nil
}
