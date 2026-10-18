package main

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"time"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

// sheetState is the STREAM state of an incremental stream: the Drive
// modifiedTime of its spreadsheet when the last read started.
type sheetState struct {
	SpreadsheetModifiedTime string `json:"spreadsheet_modified_time"`
}

func loadState(desc synclet.StreamDescriptor, state *synclet.InputState) (time.Time, string, error) {
	if state == nil {
		return time.Time{}, "", nil
	}
	raw := state.Stream(desc)
	if raw == nil {
		return time.Time{}, "", nil
	}
	var st sheetState
	if err := json.Unmarshal(raw, &st); err != nil {
		return time.Time{}, "", synclet.ConfigError(fmt.Errorf("state of %s: %w", desc.Name, err))
	}
	if st.SpreadsheetModifiedTime == "" {
		return time.Time{}, "", nil
	}
	t, err := time.Parse(time.RFC3339Nano, st.SpreadsheetModifiedTime)
	if err != nil {
		return time.Time{}, "", synclet.ConfigError(fmt.Errorf("state of %s: spreadsheet_modified_time: %w", desc.Name, err))
	}
	return t, st.SpreadsheetModifiedTime, nil
}

// Read emits the records of every configured stream, spreadsheet by
// spreadsheet. An incremental stream whose spreadsheet has not changed
// since its saved modifiedTime is skipped and its state emitted again; any
// other incremental stream is read whole and then saves the modifiedTime
// taken before its rows were read.
func (s Source) Read(ctx context.Context, cfg Config, cat *synclet.ConfiguredCatalog, state *synclet.InputState, em *synclet.Emitter) error {
	cfg.normalize()
	if err := cfg.validate(); err != nil {
		return synclet.ConfigError(err)
	}
	c, err := s.apiClient(ctx, cfg)
	if err != nil {
		return err
	}
	refs := map[string]spreadsheetRef{}
	for _, ref := range cfg.spreadsheets() {
		refs[ref.namespace] = ref
	}
	var order []string
	streams := map[string][]synclet.ConfiguredStream{}
	for _, cs := range cat.Streams {
		ns := cs.Stream.Namespace
		if _, ok := refs[ns]; !ok {
			return synclet.ConfigError(fmt.Errorf("stream %s has namespace %s, which is not a configured spreadsheet; refresh the source schema", cs.Stream.Name, ns))
		}
		if cs.SyncMode == synclet.SyncModeIncremental && !cfg.Incremental {
			return synclet.ConfigError(fmt.Errorf("stream %s syncs incrementally, but incremental is not enabled in the configuration: enable it or refresh the source schema", cs.Stream.Name))
		}
		if _, ok := streams[ns]; !ok {
			order = append(order, ns)
		}
		streams[ns] = append(streams[ns], cs)
	}
	for _, ns := range order {
		if err := s.readSpreadsheet(ctx, c, cfg, refs[ns], streams[ns], state, em); err != nil {
			return err
		}
	}
	return nil
}

func (s Source) readSpreadsheet(ctx context.Context, c *apiClient, cfg Config, ref spreadsheetRef, configured []synclet.ConfiguredStream, state *synclet.InputState, em *synclet.Emitter) error {
	log := synclet.Logger(ctx)
	props := make([][]string, len(configured))
	withCursor := make([]bool, len(configured))
	for i, cs := range configured {
		p, err := schemaProperties(cs.Stream.JSONSchema)
		if err != nil {
			return fmt.Errorf("stream %s: %w", cs.Stream.Name, err)
		}
		props[i] = p
		withCursor[i] = cfg.Incremental && (cs.SyncMode == synclet.SyncModeIncremental || slices.Contains(p, cursorColumn))
	}
	var modified time.Time
	if slices.Contains(withCursor, true) {
		var err error
		if modified, err = c.modifiedTime(ctx, ref.id); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("spreadsheet %s: %s; set its streams to full refresh and refresh the source schema to read it without", ref.id, driveUnreadable(err))
		}
	}
	// saved holds the state of each incremental stream whose spreadsheet
	// has not changed since it was saved; those streams are not read.
	saved := make([]string, len(configured))
	var titles []string
	for i, cs := range configured {
		if cs.SyncMode == synclet.SyncModeIncremental {
			t, raw, err := loadState(cs.Stream.Descriptor(), state)
			if err != nil {
				return err
			}
			if !t.IsZero() && !t.Before(modified) {
				saved[i] = raw
				continue
			}
		}
		titles = append(titles, cs.Stream.Name)
	}
	byTitle := map[string]sheet{}
	if len(titles) > 0 {
		sheets, err := loadSheets(ctx, c, cfg, ref, titles)
		if err != nil {
			return err
		}
		for _, sh := range sheets {
			byTitle[sh.props.Title] = sh
		}
		for i, cs := range configured {
			if saved[i] != "" {
				continue
			}
			if err := checkColumns(props[i], byTitle[cs.Stream.Name]); err != nil {
				return err
			}
		}
	}
	for i, cs := range configured {
		desc := cs.Stream.Descriptor()
		if saved[i] != "" {
			log.Info(fmt.Sprintf("Skipping stream %s: spreadsheet %s has not changed since %s", desc.Name, ref.id, saved[i]))
			if err := em.StreamState(desc, sheetState{SpreadsheetModifiedTime: saved[i]}); err != nil {
				return err
			}
			continue
		}
		log.Info(fmt.Sprintf("Reading stream: %s", desc.Name))
		var cursor *string
		if withCursor[i] {
			v := modified.Format(time.RFC3339Nano)
			cursor = &v
		}
		if err := readSheet(ctx, c, cfg, byTitle[cs.Stream.Name], cursor, em); err != nil {
			return fmt.Errorf("reading stream %q: %w", desc.Name, err)
		}
		if cs.SyncMode == synclet.SyncModeIncremental {
			if err := em.StreamState(desc, sheetState{SpreadsheetModifiedTime: *cursor}); err != nil {
				return err
			}
		}
		log.Info(fmt.Sprintf("Completed stream: %s", desc.Name))
	}
	return nil
}

// schemaProperties returns the property names of a stream's JSON schema.
func schemaProperties(raw json.RawMessage) ([]string, error) {
	var schema struct {
		Properties map[string]json.RawMessage `json:"properties"`
	}
	if err := json.Unmarshal(raw, &schema); err != nil {
		return nil, fmt.Errorf("json_schema: %w", err)
	}
	names := make([]string, 0, len(schema.Properties))
	for n := range schema.Properties {
		names = append(names, n)
	}
	slices.Sort(names)
	return names, nil
}

// checkColumns fails when a column of the configured schema is not in the
// sheet's current header, so values never land under a column they do not
// belong to.
func checkColumns(props []string, sh sheet) error {
	for _, p := range props {
		if p != cursorColumn && !slices.Contains(sh.header.names, p) {
			return synclet.ConfigError(fmt.Errorf("column %q is no longer in the header of sheet %q; refresh the source schema", p, sh.props.Title))
		}
	}
	return nil
}

// readSheet reads the data rows of a sheet, batch_size rows per request,
// up to the sheet's row count. cursor, when set, is added to every record
// as cursorColumn.
func readSheet(ctx context.Context, c *apiClient, cfg Config, sh sheet, cursor *string, em *synclet.Emitter) error {
	names := sh.header.names
	title := sh.props.Title
	desc := sh.descriptor()
	rowCount := sh.props.rowCount()
	warned := false
	for start := 2; start <= rowCount; start += cfg.BatchSize {
		end := min(start+cfg.BatchSize-1, rowCount)
		batches, err := c.values(ctx, sh.ref.id, []string{a1Range(title, start, end)}, cfg.ValueRender == valueRenderFormatted)
		if err != nil {
			return fmt.Errorf("reading rows %d-%d: %w", start, end, err)
		}
		for i, row := range batches[0] {
			if col := firstValueBeyond(row, len(names)); col >= 0 {
				if cfg.ColumnNaming == columnNamingStable {
					return fmt.Errorf("row %d of sheet %q has a value in column %s, right of the last header column %s: give the column a header or clear the value",
						start+i, title, columnLetter(col), columnLetter(len(names)-1))
				}
				if !warned {
					synclet.Logger(ctx).Warn(fmt.Sprintf("sheet %q: row %d has a value in column %s, right of the last named column %s; values right of %s are not read (column_naming: stable reads every column up to the last header cell)",
						title, start+i, columnLetter(col), columnLetter(len(names)-1), columnLetter(len(names)-1)))
					warned = true
				}
			}
			rec := mapRowToRecord(names, row)
			if rec == nil {
				continue
			}
			if cursor != nil {
				rec[cursorColumn] = *cursor
			}
			if err := em.Record(desc, rec); err != nil {
				return fmt.Errorf("emitting record: %w", err)
			}
		}
	}
	return nil
}

// firstValueBeyond returns the index of the first non-empty value at or
// after column n, or -1.
func firstValueBeyond(row []any, n int) int {
	for i := n; i < len(row); i++ {
		if !empty(row[i]) {
			return i
		}
	}
	return -1
}

func empty(v any) bool { return v == nil || v == "" }

// mapRowToRecord maps a data row's values to header names; a short row
// gives nulls. It returns nil for a row whose values are all empty.
func mapRowToRecord(names []string, row []any) map[string]any {
	record := make(map[string]any, len(names)+1)
	hasData := false
	for i, h := range names {
		if i < len(row) {
			record[h] = row[i]
			if !empty(row[i]) {
				hasData = true
			}
		} else {
			record[h] = nil
		}
	}
	if !hasData {
		return nil
	}
	return record
}
