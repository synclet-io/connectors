package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"time"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

// pageSize is the pageSize of record requests, the most Airtable allows.
const pageSize = "100"

// cursorState is the STREAM state of an incremental table: the greatest
// cursor value emitted so far, RFC 3339 in UTC.
type cursorState struct {
	Cursor string `json:"cursor"`
}

// Read emits every configured stream in catalog order. All streams share one
// client, so the spacing of requests per base holds across streams.
func (s Source) Read(ctx context.Context, cfg Config, catalog *synclet.ConfiguredCatalog, state *synclet.InputState, em *synclet.Emitter) error {
	if err := cfg.validate(); err != nil {
		return synclet.ConfigError(err)
	}
	m := newMetadata(s.apiClient(cfg))
	for _, cs := range catalog.Streams {
		r, err := m.tableRead(ctx, cfg, cs)
		if err != nil {
			return err
		}
		r.em = em
		if r.incremental {
			if raw := state.Stream(r.desc); raw != nil {
				var st cursorState
				if err := json.Unmarshal(raw, &st); err != nil {
					return synclet.ConfigError(fmt.Errorf("state of %s: %w", r.desc.Name, err))
				}
				if st.Cursor != "" {
					t, err := time.Parse(time.RFC3339Nano, st.Cursor)
					if err != nil {
						return synclet.ConfigError(fmt.Errorf("state of %s: cursor %q is not an RFC 3339 time", r.desc.Name, st.Cursor))
					}
					r.saved = t
				}
			}
		}
		if err := r.read(ctx, m.c); err != nil {
			return err
		}
	}
	return nil
}

// tableRead is the read of one configured stream: the table it reads, the
// output name of every field id it emits and, for an incremental read, the
// cursor field.
type tableRead struct {
	desc        synclet.StreamDescriptor
	baseID      string
	tableID     string
	columns     map[string]string
	types       map[string]string
	incremental bool
	cursor      field
	saved       time.Time
	em          *synclet.Emitter
	warned      map[string]bool
}

// tableRead resolves a configured stream to its table through the schema's
// x-airtable-* hints, or by stream name and field names when the schema has
// none. Field ids map to the catalog's property names, so a field renamed
// since discover keeps its column.
func (m *metadata) tableRead(ctx context.Context, cfg Config, cs synclet.ConfiguredStream) (*tableRead, error) {
	desc := cs.Stream.Descriptor()
	var schema struct {
		Properties map[string]struct {
			FieldID string `json:"x-airtable-field-id"`
		} `json:"properties"`
		BaseID  string `json:"x-airtable-base-id"`
		TableID string `json:"x-airtable-table-id"`
	}
	if err := json.Unmarshal(cs.Stream.JSONSchema, &schema); err != nil {
		return nil, synclet.ConfigError(fmt.Errorf("json_schema of %s: %w", desc.Name, err))
	}
	var t table
	var baseID string
	if schema.BaseID != "" && schema.TableID != "" {
		tables, err := m.baseTables(ctx, schema.BaseID)
		if err != nil {
			return nil, err
		}
		found := false
		for _, tb := range tables {
			if tb.ID == schema.TableID {
				t, found = tb, true
				break
			}
		}
		if !found {
			return nil, synclet.ConfigError(fmt.Errorf("stream %s: table %s no longer exists in base %s; refresh the source schema", desc.Name, schema.TableID, schema.BaseID))
		}
		baseID = schema.BaseID
	} else {
		streams, err := m.streams(ctx, cfg)
		if err != nil {
			return nil, err
		}
		found := false
		for _, ts := range streams {
			if ts.name == desc.Name {
				t, baseID, found = ts.table, ts.base.ID, true
				break
			}
		}
		if !found {
			return nil, synclet.ConfigError(fmt.Errorf("unknown stream %q", desc.Name))
		}
	}

	byID := map[string]field{}
	byName := map[string]field{}
	for _, f := range t.Fields {
		byID[f.ID] = f
		byName[propertyName(f)] = f
	}
	r := &tableRead{
		desc: desc, baseID: baseID, tableID: t.ID,
		columns: map[string]string{}, types: map[string]string{}, warned: map[string]bool{},
		incremental: cs.SyncMode == synclet.SyncModeIncremental,
	}
	for name, p := range schema.Properties {
		if name == idProperty || name == createdTimeProperty {
			continue
		}
		f, ok := byID[p.FieldID]
		if !ok {
			f, ok = byName[name]
		}
		if ok {
			r.columns[f.ID] = name
			r.types[f.ID] = f.Type
		}
	}
	if !r.incremental {
		return r, nil
	}
	path := cs.CursorField
	if len(path) == 0 {
		path = cs.Stream.DefaultCursorField
	}
	if len(path) != 1 {
		return nil, synclet.ConfigError(fmt.Errorf("stream %s: incremental sync needs a last-modified-time field as its cursor; the table has none, so it syncs as full refresh only", desc.Name))
	}
	for id, name := range r.columns {
		if name == path[0] {
			r.cursor = byID[id]
		}
	}
	if r.cursor.Type != lastModifiedTime {
		return nil, synclet.ConfigError(fmt.Errorf("stream %s: cursor field %q is not a last-modified-time field; only a \"Last modified time\" field can be the cursor", desc.Name, path[0]))
	}
	return r, nil
}

// read pages through the table's records by offset. An incremental read
// sorts by the cursor field and, with a saved cursor, asks only for
// records whose cursor is at or after it, or whose cursor is blank and
// whose created time is at or after it; it emits its state at the end.
func (r *tableRead) read(ctx context.Context, c *apiClient) error {
	query := url.Values{"pageSize": {pageSize}, "returnFieldsByFieldId": {"true"}}
	if r.incremental {
		query.Set("sort[0][field]", r.cursor.ID)
		query.Set("sort[0][direction]", "asc")
		if !r.saved.IsZero() {
			query.Set("filterByFormula", sinceFormula(r.cursor.Name, r.saved))
		}
	}
	top := r.saved
	path := r.baseID + "/" + r.tableID
	for {
		var page struct {
			Records []struct {
				ID          string                     `json:"id"`
				CreatedTime string                     `json:"createdTime"`
				Fields      map[string]json.RawMessage `json:"fields"`
			} `json:"records"`
			Offset string `json:"offset"`
		}
		if err := c.get(ctx, r.baseID, path, query, &page); err != nil {
			return baseError(r.baseID, err)
		}
		for _, rec := range page.Records {
			out := map[string]any{idProperty: rec.ID, createdTimeProperty: rec.CreatedTime}
			for id, v := range rec.Fields {
				if name, ok := r.columns[id]; ok {
					out[name] = r.value(ctx, id, v)
				}
			}
			if r.incremental {
				t, err := r.cursorOf(rec.Fields[r.cursor.ID], rec.CreatedTime)
				if err != nil {
					return fmt.Errorf("stream %s: record %s: %w", r.desc.Name, rec.ID, err)
				}
				if t.After(top) {
					top = t
				}
			}
			if err := r.em.Record(r.desc, out); err != nil {
				return err
			}
		}
		if page.Offset == "" {
			break
		}
		query.Set("offset", page.Offset)
	}
	if r.incremental && !top.IsZero() {
		return r.em.StreamState(r.desc, cursorState{Cursor: top.UTC().Format(time.RFC3339Nano)})
	}
	return nil
}

// cursorOf returns a record's cursor value, or its created time when the
// cursor field is blank.
func (r *tableRead) cursorOf(raw json.RawMessage, createdTime string) (time.Time, error) {
	var s string
	if len(raw) > 0 && !bytes.Equal(raw, []byte("null")) {
		if err := json.Unmarshal(raw, &s); err != nil {
			return time.Time{}, fmt.Errorf("cursor field %q: %w", r.cursor.Name, err)
		}
	}
	if s == "" {
		s = createdTime
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return time.Time{}, fmt.Errorf("cursor value %q is not an RFC 3339 time", s)
	}
	return t, nil
}

// sinceFormula is the filterByFormula of an incremental read from saved:
// the cursor field at or after saved when it has a value, else the created
// time at or after saved. IF keeps a blank cursor out of DATETIME_DIFF.
func sinceFormula(name string, saved time.Time) string {
	ref := "{" + strings.ReplaceAll(name, "}", `\}`) + "}"
	ts := "DATETIME_PARSE('" + saved.UTC().Format(time.RFC3339Nano) + "')"
	return fmt.Sprintf("IF(%[1]s, DATETIME_DIFF(%[1]s, %[2]s, 'milliseconds') >= 0, DATETIME_DIFF(CREATED_TIME(), %[2]s, 'milliseconds') >= 0)", ref, ts)
}

// value returns a field value as emitted. The error values of formula,
// rollup and lookup fields ({"error": "#ERROR!"}, {"specialValue": "NaN"})
// become null, with one warning per field.
func (r *tableRead) value(ctx context.Context, id string, raw json.RawMessage) any {
	switch r.types[id] {
	case "formula", "rollup":
		if isErrorValue(raw) {
			r.warn(ctx, id, raw)
			return nil
		}
	case "multipleLookupValues":
		if isErrorValue(raw) {
			r.warn(ctx, id, raw)
			return nil
		}
		var items []json.RawMessage
		if json.Unmarshal(raw, &items) == nil {
			out := make([]any, len(items))
			for i, it := range items {
				if isErrorValue(it) {
					r.warn(ctx, id, it)
					continue
				}
				out[i] = it
			}
			return out
		}
	}
	return raw
}

func (r *tableRead) warn(ctx context.Context, id string, raw json.RawMessage) {
	if r.warned[id] {
		return
	}
	r.warned[id] = true
	synclet.Logger(ctx).Warn("field returned an error value; emitting null", "stream", r.desc.Name, "field", r.columns[id], "value", string(raw))
}

// isErrorValue reports whether raw is a computed field's error value: an
// object whose only key is "error" or "specialValue".
func isErrorValue(raw json.RawMessage) bool {
	if len(raw) == 0 || raw[0] != '{' {
		return false
	}
	var obj map[string]json.RawMessage
	if json.Unmarshal(raw, &obj) != nil || len(obj) != 1 {
		return false
	}
	_, e := obj["error"]
	_, sv := obj["specialValue"]
	return e || sv
}
