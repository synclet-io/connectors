package main

import (
	"fmt"
	"sort"

	"google.golang.org/api/sheets/v4"
)

const defaultBatchSize = 1000

// sheetOps abstracts Sheets API operations for testability.
type sheetOps interface {
	ensureSheet(sheetTitle string) error
	clearSheet(sheetTitle string) error
	appendRows(sheetTitle string, rows [][]interface{}) error
}

// SheetWriter handles per-stream record buffering and flushing to Google Sheets.
type SheetWriter struct {
	ops           sheetOps
	spreadsheetID string
	buffers       map[string]*streamBuffer // keyed by stream name
	batchSize     int                      // flush threshold, default 1000
}

// streamBuffer tracks per-stream state for buffered writes.
type streamBuffer struct {
	headers  []string
	rows     [][]interface{}
	syncMode string // "overwrite" or "append"
	synced   bool   // whether initial clear happened (for overwrite mode)
}

// NewSheetWriter creates a SheetWriter with the default Sheets API operations.
func NewSheetWriter(srv *sheets.Service, spreadsheetID string) *SheetWriter {
	return &SheetWriter{
		ops:           &sheetsAPIops{srv: srv, spreadsheetID: spreadsheetID},
		spreadsheetID: spreadsheetID,
		buffers:       make(map[string]*streamBuffer),
		batchSize:     defaultBatchSize,
	}
}

// newSheetWriterWithOps creates a SheetWriter with injectable operations (for testing).
func newSheetWriterWithOps(ops sheetOps, batchSize int) *SheetWriter {
	return &SheetWriter{
		ops:       ops,
		buffers:   make(map[string]*streamBuffer),
		batchSize: batchSize,
	}
}

// AddRecord buffers a record for the given stream. Auto-flushes when buffer reaches batchSize.
func (w *SheetWriter) AddRecord(streamName string, data map[string]interface{}, syncMode string) error {
	buf, ok := w.buffers[streamName]
	if !ok {
		// First record for this stream: establish column order from keys (sorted alphabetically)
		keys := make([]string, 0, len(data))
		for k := range data {
			keys = append(keys, k)
		}
		sort.Strings(keys)

		buf = &streamBuffer{
			headers:  keys,
			syncMode: syncMode,
		}
		w.buffers[streamName] = buf
	}

	// Build row in header order
	row := make([]interface{}, len(buf.headers))
	for i, h := range buf.headers {
		row[i] = data[h]
	}

	buf.rows = append(buf.rows, row)

	// Auto-flush if buffer reaches threshold
	if len(buf.rows) >= w.batchSize {
		if err := w.flushStream(streamName, buf); err != nil {
			return fmt.Errorf("auto-flush stream %q: %w", streamName, err)
		}
	}

	return nil
}

// FlushAll flushes all remaining stream buffers.
func (w *SheetWriter) FlushAll() error {
	for name, buf := range w.buffers {
		if len(buf.rows) > 0 {
			if err := w.flushStream(name, buf); err != nil {
				return fmt.Errorf("flushing stream %q: %w", name, err)
			}
		}
	}
	return nil
}

// flushStream writes buffered rows to the sheet and clears the buffer.
func (w *SheetWriter) flushStream(streamName string, buf *streamBuffer) error {
	// Ensure the sheet tab exists
	if err := w.ops.ensureSheet(streamName); err != nil {
		return fmt.Errorf("ensuring sheet %q: %w", streamName, err)
	}

	if buf.syncMode == "overwrite" && !buf.synced {
		// First flush in overwrite mode: clear sheet, write header, then data
		if err := w.ops.clearSheet(streamName); err != nil {
			return fmt.Errorf("clearing sheet %q: %w", streamName, err)
		}

		// Write header row
		headerRow := make([]interface{}, len(buf.headers))
		for i, h := range buf.headers {
			headerRow[i] = h
		}
		if err := w.ops.appendRows(streamName, [][]interface{}{headerRow}); err != nil {
			return fmt.Errorf("writing header for %q: %w", streamName, err)
		}

		buf.synced = true
	} else if buf.syncMode == "append" && !buf.synced {
		// First flush in append mode: write header (sheet may be new), never clear
		headerRow := make([]interface{}, len(buf.headers))
		for i, h := range buf.headers {
			headerRow[i] = h
		}
		if err := w.ops.appendRows(streamName, [][]interface{}{headerRow}); err != nil {
			return fmt.Errorf("writing header for %q: %w", streamName, err)
		}

		buf.synced = true
	}

	// Append data rows
	if err := w.ops.appendRows(streamName, buf.rows); err != nil {
		return fmt.Errorf("appending rows for %q: %w", streamName, err)
	}

	// Clear buffer
	buf.rows = nil

	return nil
}

// sheetsAPIops implements sheetOps using the Google Sheets API.
type sheetsAPIops struct {
	srv           *sheets.Service
	spreadsheetID string
}

func (o *sheetsAPIops) ensureSheet(sheetTitle string) error {
	resp, err := o.srv.Spreadsheets.Get(o.spreadsheetID).
		IncludeGridData(false).
		Fields("sheets.properties.title").
		Do()
	if err != nil {
		return fmt.Errorf("getting spreadsheet: %w", err)
	}

	for _, sheet := range resp.Sheets {
		if sheet.Properties.Title == sheetTitle {
			return nil // sheet exists
		}
	}

	// Create new sheet
	req := &sheets.BatchUpdateSpreadsheetRequest{
		Requests: []*sheets.Request{
			{
				AddSheet: &sheets.AddSheetRequest{
					Properties: &sheets.SheetProperties{
						Title: sheetTitle,
					},
				},
			},
		},
	}
	_, err = o.srv.Spreadsheets.BatchUpdate(o.spreadsheetID, req).Do()
	if err != nil {
		return fmt.Errorf("creating sheet %q: %w", sheetTitle, err)
	}

	return nil
}

func (o *sheetsAPIops) clearSheet(sheetTitle string) error {
	rangeStr := fmt.Sprintf("'%s'!A:ZZ", sheetTitle)
	_, err := o.srv.Spreadsheets.Values.Clear(o.spreadsheetID, rangeStr,
		&sheets.ClearValuesRequest{}).Do()
	return err
}

func (o *sheetsAPIops) appendRows(sheetTitle string, rows [][]interface{}) error {
	rangeStr := fmt.Sprintf("'%s'!A1", sheetTitle)
	_, err := o.srv.Spreadsheets.Values.Append(o.spreadsheetID, rangeStr,
		&sheets.ValueRange{Values: rows}).
		ValueInputOption("RAW").
		InsertDataOption("INSERT_ROWS").
		Do()
	return err
}
