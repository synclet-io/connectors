package main

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"time"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

// cursorState is the STREAM state of the databases stream: the greatest
// last_edited_time emitted, RFC 3339 in UTC.
type cursorState struct {
	LastEditedTime string `json:"last_edited_time,omitempty"`
}

// pageState is the STREAM state of the pages and blocks streams: the
// greatest last_edited_time of the pages found through search, and per data
// source the greatest last_edited_time of its rows.
type pageState struct {
	LastEditedTime string            `json:"last_edited_time,omitempty"`
	DataSources    map[string]string `json:"data_sources,omitempty"`
}

// reader is one read: the client every stream shares, so the request
// spacing holds across streams.
type reader struct {
	c   *apiClient
	cfg Config
	em  *synclet.Emitter
}

// Read emits every configured stream in catalog order.
func (s Source) Read(ctx context.Context, cfg Config, catalog *synclet.ConfiguredCatalog, state *synclet.InputState, em *synclet.Emitter) error {
	if err := cfg.validate(); err != nil {
		return synclet.ConfigError(err)
	}
	r := &reader{c: s.apiClient(cfg), cfg: cfg, em: em}
	for _, cs := range catalog.Streams {
		desc := cs.Stream.Descriptor()
		d, ok := streamDefByName(desc.Name)
		if !ok {
			return synclet.ConfigError(fmt.Errorf("unknown stream %q", desc.Name))
		}
		incremental := cs.SyncMode == synclet.SyncModeIncremental
		if incremental && !d.incremental {
			return synclet.ConfigError(fmt.Errorf("stream %s supports full refresh only", desc.Name))
		}
		if incremental && len(cs.CursorField) > 0 && (len(cs.CursorField) != 1 || cs.CursorField[0] != cursorField) {
			return synclet.ConfigError(fmt.Errorf("stream %s: the cursor is %s, chosen by the source", desc.Name, cursorField))
		}
		var saved json.RawMessage
		if incremental {
			saved = state.Stream(desc)
		}
		var err error
		switch d.name {
		case usersStream:
			err = r.readUsers(ctx, desc, d)
		case databasesStream:
			err = r.readDatabases(ctx, desc, d, incremental, saved)
		case pagesStream:
			err = r.walkPages(ctx, desc, incremental, saved, func(_ context.Context, p page, _ time.Time) error {
				return r.em.Record(desc, d.record(p.obj))
			})
		case blocksStream:
			err = r.walkPages(ctx, desc, incremental, saved, func(ctx context.Context, p page, since time.Time) error {
				w := &blockWalker{r: r, desc: desc, def: d, pageID: p.id, since: since}
				return w.children(ctx, p.id, 1)
			})
		}
		if err != nil {
			return fmt.Errorf("stream %s: %w", desc.Name, err)
		}
	}
	return nil
}

func (r *reader) readUsers(ctx context.Context, desc synclet.StreamDescriptor, d streamDef) error {
	return r.c.getAll(ctx, "users", func(raw json.RawMessage) (bool, error) {
		var obj map[string]json.RawMessage
		if err := json.Unmarshal(raw, &obj); err != nil {
			return false, fmt.Errorf("decode user: %w", err)
		}
		return true, r.em.Record(desc, d.record(obj))
	})
}

// search pages through POST /search for objects of kind ("page" or
// "data_source"), the most recently edited first when newestFirst, else the
// least recently edited first. A complete listing sorts oldest first, so an
// object edited during the listing moves behind the cursor, where it is
// still read, instead of ahead of it.
func (r *reader) search(ctx context.Context, kind string, newestFirst bool, fn func(json.RawMessage) (bool, error)) error {
	direction := "ascending"
	if newestFirst {
		direction = "descending"
	}
	body := map[string]any{
		"filter": map[string]any{"property": "object", "value": kind},
		"sort":   map[string]any{"direction": direction, "timestamp": cursorField},
	}
	return r.c.postAll(ctx, "search", body, fn)
}

// notionObject is a decoded Notion object: its id, last_edited_time and parent,
// and every top-level key as returned.
type notionObject struct {
	id     string
	edited time.Time
	parent struct {
		Type         string `json:"type"`
		DataSourceID string `json:"data_source_id"`
		DatabaseID   string `json:"database_id"`
	}
	obj map[string]json.RawMessage
}

type page = notionObject

func decodeObject(raw json.RawMessage, kind string) (notionObject, error) {
	var o notionObject
	if err := json.Unmarshal(raw, &o.obj); err != nil {
		return o, fmt.Errorf("decode %s: %w", kind, err)
	}
	if err := json.Unmarshal(o.obj["id"], &o.id); err != nil || o.id == "" {
		return o, fmt.Errorf("%s without an id: %s", kind, truncate(raw))
	}
	var edited string
	if err := json.Unmarshal(o.obj[cursorField], &edited); err != nil {
		return o, fmt.Errorf("%s %s: no last_edited_time", kind, o.id)
	}
	t, err := time.Parse(time.RFC3339Nano, edited)
	if err != nil {
		return o, fmt.Errorf("%s %s: last_edited_time %q is not an RFC 3339 time", kind, o.id, edited)
	}
	o.edited = t
	if p, ok := o.obj["parent"]; ok {
		if err := json.Unmarshal(p, &o.parent); err != nil {
			return o, fmt.Errorf("%s %s: decode parent: %w", kind, o.id, err)
		}
	}
	return o, nil
}

func truncate(raw []byte) string {
	if len(raw) > 200 {
		return string(raw[:200]) + "..."
	}
	return string(raw)
}

func formatTime(t time.Time) string { return t.UTC().Format(time.RFC3339Nano) }

// parseSaved parses a saved cursor; "" is the zero time.
func parseSaved(desc synclet.StreamDescriptor, v string) (time.Time, error) {
	if v == "" {
		return time.Time{}, nil
	}
	t, err := time.Parse(time.RFC3339Nano, v)
	if err != nil {
		return time.Time{}, synclet.ConfigError(fmt.Errorf("state of %s: %q is not an RFC 3339 time", desc.Name, v))
	}
	return t, nil
}

// since returns the start of an incremental read from saved: the lookback
// window before it, or the zero time without a saved cursor.
func (r *reader) since(saved time.Time) time.Time {
	if saved.IsZero() {
		return saved
	}
	return saved.Add(-r.cfg.lookback())
}

func laterOf(a, b time.Time) time.Time {
	if b.After(a) {
		return b
	}
	return a
}

// readDatabases emits the data sources search finds. An incremental read
// searches the most recently edited first, stops at the first one edited
// before the lookback window and emits its state at the end.
func (r *reader) readDatabases(ctx context.Context, desc synclet.StreamDescriptor, d streamDef, incremental bool, raw json.RawMessage) error {
	var st cursorState
	if raw != nil {
		if err := json.Unmarshal(raw, &st); err != nil {
			return synclet.ConfigError(fmt.Errorf("state of %s: %w", desc.Name, err))
		}
	}
	saved, err := parseSaved(desc, st.LastEditedTime)
	if err != nil {
		return err
	}
	since := r.since(saved)
	top := saved
	seen := map[string]bool{}
	err = r.search(ctx, "data_source", incremental, func(raw json.RawMessage) (bool, error) {
		o, err := decodeObject(raw, "data source")
		if err != nil {
			return false, err
		}
		if incremental && o.edited.Before(since) {
			return false, nil
		}
		if seen[o.id] {
			return true, nil
		}
		seen[o.id] = true
		rec := d.record(o.obj)
		if o.parent.DatabaseID != "" {
			id, err := json.Marshal(o.parent.DatabaseID)
			if err != nil {
				return false, err
			}
			rec["database_id"] = id
		}
		top = laterOf(top, o.edited)
		return true, r.em.Record(desc, rec)
	})
	if err != nil {
		return err
	}
	if incremental && !top.IsZero() {
		return r.em.StreamState(desc, cursorState{LastEditedTime: formatTime(top)})
	}
	return nil
}

// dataSourceIDs returns the ids of every data source shared with the
// integration, in search order.
func (r *reader) dataSourceIDs(ctx context.Context) ([]string, error) {
	var ids []string
	seen := map[string]bool{}
	err := r.search(ctx, "data_source", false, func(raw json.RawMessage) (bool, error) {
		o, err := decodeObject(raw, "data source")
		if err != nil {
			return false, err
		}
		if !seen[o.id] {
			seen[o.id] = true
			ids = append(ids, o.id)
		}
		return true, nil
	})
	return ids, err
}

// walkPages calls visit once for every page: first the rows of every
// shared data source, read by querying it, then the pages search finds
// whose parent is not one of those data sources. since is the start of the
// incremental read the page belongs to, or the zero time.
//
// An incremental read queries a data source for rows edited on or after
// its saved cursor less the lookback window, or in full when it has none,
// and emits the state after each data source. Search runs the most
// recently edited first and stops at the first page edited before its
// saved cursor less the lookback window; the state is emitted again at the
// end. A full refresh searches the least recently edited first.
func (r *reader) walkPages(ctx context.Context, desc synclet.StreamDescriptor, incremental bool, raw json.RawMessage, visit func(context.Context, page, time.Time) error) error {
	var st pageState
	if raw != nil {
		if err := json.Unmarshal(raw, &st); err != nil {
			return synclet.ConfigError(fmt.Errorf("state of %s: %w", desc.Name, err))
		}
	}
	next := pageState{LastEditedTime: st.LastEditedTime, DataSources: map[string]string{}}
	maps.Copy(next.DataSources, st.DataSources)

	sources, err := r.dataSourceIDs(ctx)
	if err != nil {
		return err
	}
	shared := map[string]bool{}
	for _, id := range sources {
		shared[id] = true
	}
	seen := map[string]bool{}
	for _, ds := range sources {
		var saved time.Time
		if incremental {
			if saved, err = parseSaved(desc, st.DataSources[ds]); err != nil {
				return err
			}
		}
		since := r.since(saved)
		body := map[string]any{
			"sorts": []map[string]any{{"timestamp": cursorField, "direction": "ascending"}},
		}
		if !since.IsZero() {
			body["filter"] = map[string]any{
				"timestamp": cursorField,
				cursorField: map[string]any{"on_or_after": formatTime(since)},
			}
		}
		top := saved
		err := r.c.postAll(ctx, "data_sources/"+ds+"/query", body, func(raw json.RawMessage) (bool, error) {
			p, err := decodeObject(raw, "page")
			if err != nil {
				return false, err
			}
			if p.edited.Before(since) || seen[p.id] {
				return true, nil
			}
			seen[p.id] = true
			top = laterOf(top, p.edited)
			return true, visit(ctx, p, since)
		})
		if err != nil {
			return fmt.Errorf("data source %s: %w", ds, err)
		}
		if incremental {
			if !top.IsZero() {
				next.DataSources[ds] = formatTime(top)
			}
			if err := r.em.StreamState(desc, next); err != nil {
				return err
			}
		}
	}

	var saved time.Time
	if incremental {
		if saved, err = parseSaved(desc, st.LastEditedTime); err != nil {
			return err
		}
	}
	since := r.since(saved)
	top := saved
	err = r.search(ctx, "page", incremental, func(raw json.RawMessage) (bool, error) {
		p, err := decodeObject(raw, "page")
		if err != nil {
			return false, err
		}
		if p.edited.Before(since) {
			return false, nil
		}
		if shared[p.parent.DataSourceID] || seen[p.id] {
			return true, nil
		}
		seen[p.id] = true
		top = laterOf(top, p.edited)
		return true, visit(ctx, p, since)
	})
	if err != nil {
		return err
	}
	if !incremental {
		return nil
	}
	if !top.IsZero() {
		next.LastEditedTime = formatTime(top)
	}
	return r.em.StreamState(desc, next)
}
