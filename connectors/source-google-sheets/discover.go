package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

// cursorColumn is the column incremental: true adds to every record: the
// Drive modifiedTime of the spreadsheet, the stream's cursor.
const cursorColumn = "_spreadsheet_modified_time"

// sheet is a grid sheet of a spreadsheet with its column names.
type sheet struct {
	ref    spreadsheetRef
	props  sheetProperties
	header header
}

func (sh sheet) descriptor() synclet.StreamDescriptor {
	return synclet.StreamDescriptor{Name: sh.props.Title, Namespace: sh.ref.namespace}
}

// loadSheets reads the sheet list of a spreadsheet and the header rows of
// its grid sheets in one batchGet. With only set, it loads those sheets and
// fails when one is gone or lost its header; otherwise it loads every grid
// sheet with rows and skips a sheet without a header with a warning.
func loadSheets(ctx context.Context, c *apiClient, cfg Config, ref spreadsheetRef, only []string) ([]sheet, error) {
	log := synclet.Logger(ctx)
	all, err := c.sheets(ctx, ref.id)
	if err != nil {
		return nil, err
	}
	var picked []sheetProperties
	if only == nil {
		for _, p := range all {
			if p.readable() {
				picked = append(picked, p)
			}
		}
	} else {
		byTitle := map[string]sheetProperties{}
		for _, p := range all {
			byTitle[p.Title] = p
		}
		for _, title := range only {
			p, ok := byTitle[title]
			if !ok || !p.readable() {
				return nil, synclet.ConfigError(fmt.Errorf("sheet %q is no longer a grid sheet with rows in spreadsheet %s; refresh the source schema", title, ref.id))
			}
			picked = append(picked, p)
		}
	}
	if len(picked) == 0 {
		return nil, nil
	}
	ranges := make([]string, len(picked))
	for i, p := range picked {
		ranges[i] = a1Range(p.Title, 1, 1)
	}
	rows, err := c.values(ctx, ref.id, ranges, true)
	if err != nil {
		return nil, err
	}
	out := make([]sheet, 0, len(picked))
	for i, p := range picked {
		var first []any
		if len(rows[i]) > 0 {
			first = rows[i][0]
		}
		h, err := columnNames(first, cfg.ColumnNaming, cfg.NamesConversion)
		switch {
		case errors.Is(err, errNoHeader) && only == nil:
			log.Warn(fmt.Sprintf("Skipping sheet %q: %v", p.Title, err))
			continue
		case errors.Is(err, errNoHeader):
			return nil, synclet.ConfigError(fmt.Errorf("sheet %q has no header row anymore; refresh the source schema", p.Title))
		case err != nil:
			return nil, synclet.ConfigError(fmt.Errorf("sheet %q: %w", p.Title, err))
		}
		if cfg.Incremental {
			for _, n := range h.names {
				if n == cursorColumn {
					return nil, synclet.ConfigError(fmt.Errorf("sheet %q has a column named %s, the cursor column incremental: true adds: rename the column", p.Title, cursorColumn))
				}
			}
		}
		if h.dropped > 0 {
			log.Warn(fmt.Sprintf("sheet %q: the header stops at the empty cell %s1, so %d header cell(s) right of it get no column (column_naming: stable names every column)",
				p.Title, columnLetter(len(h.names)), h.dropped))
		}
		out = append(out, sheet{ref: ref, props: p, header: h})
	}
	return out, nil
}

// stream is the catalog stream of a sheet; incremental adds the cursor
// column and the incremental sync mode.
func (sh sheet) stream(cfg Config, incremental bool) synclet.Stream {
	valueType := typed(synclet.Null, synclet.String)
	if cfg.ValueRender == valueRenderUnformatted {
		valueType = typed(synclet.Null, synclet.String, synclet.Number, boolean)
	}
	props := make(map[synclet.PropertyName]synclet.PropertySpec, len(sh.header.names)+1)
	for _, n := range sh.header.names {
		props[synclet.PropertyName(n)] = synclet.PropertySpec{PropertyType: valueType}
	}
	st := synclet.Stream{
		Name:               sh.props.Title,
		Namespace:          sh.ref.namespace,
		SupportedSyncModes: []synclet.SyncMode{synclet.SyncModeFullRefresh},
	}
	if incremental {
		props[cursorColumn] = synclet.PropertySpec{
			PropertyType: synclet.PropertyType{Type: synclet.PropTypes{synclet.String}, AirbyteType: synclet.TimestampWithTZ},
			Format:       synclet.DateTime,
		}
		st.SupportedSyncModes = append(st.SupportedSyncModes, synclet.SyncModeIncremental)
		st.SourceDefinedCursor = true
		st.DefaultCursorField = []string{cursorColumn}
	}
	st.JSONSchema = synclet.ObjectSchema(props)
	return st
}

// Discover returns one stream per grid sheet with a header row of every
// configured spreadsheet.
func (s Source) Discover(ctx context.Context, cfg Config) (*synclet.Catalog, error) {
	cfg.normalize()
	if err := cfg.validate(); err != nil {
		return nil, synclet.ConfigError(err)
	}
	c, err := s.apiClient(ctx, cfg)
	if err != nil {
		return nil, err
	}
	log := synclet.Logger(ctx)
	cat := &synclet.Catalog{Streams: []synclet.Stream{}}
	for _, ref := range cfg.spreadsheets() {
		sheets, err := loadSheets(ctx, c, cfg, ref, nil)
		if err != nil {
			return nil, err
		}
		incremental := cfg.Incremental && driveReadable(ctx, c, log, ref)
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		for _, sh := range sheets {
			cat.Streams = append(cat.Streams, sh.stream(cfg, incremental))
		}
	}
	return cat, nil
}

// driveReadable reports whether Drive returns the modifiedTime of a
// spreadsheet, warning when it does not.
func driveReadable(ctx context.Context, c *apiClient, log *slog.Logger, ref spreadsheetRef) bool {
	if _, err := c.modifiedTime(ctx, ref.id); err != nil {
		if ctx.Err() == nil {
			log.Warn(fmt.Sprintf("spreadsheet %s: %s; its streams offer full refresh only", ref.id, driveUnreadable(err)))
		}
		return false
	}
	return true
}
