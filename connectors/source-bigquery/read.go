package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"

	"cloud.google.com/go/bigquery"
	synclet "github.com/synclet-io/synclet-go-sdk"
	"google.golang.org/api/iterator"
)

// checkpointRows is how many records of an incremental stream go between
// two STATE messages.
const checkpointRows = 10_000

// streamState is the STREAM state of an incremental stream: the greatest
// cursor read so far, as its record value renders as a string.
type streamState struct {
	CursorField []string `json:"cursor_field"`
	Cursor      *string  `json:"cursor,omitempty"`
}

// Read emits every configured stream in catalog order. A full refresh reads a
// table through tabledata.list and a view through a query; an incremental
// read queries the rows whose cursor is at least the saved one, in cursor
// order.
func (s Source) Read(ctx context.Context, cfg Config, catalog *synclet.ConfiguredCatalog, state *synclet.InputState, em *synclet.Emitter) error {
	c, err := s.client(ctx, cfg)
	if err != nil {
		return err
	}
	defer func() { _ = c.Close() }()
	for _, cs := range catalog.Streams {
		if err := readStream(ctx, c, cfg, cs, state, em); err != nil {
			return err
		}
	}
	return nil
}

// streamPlan is how one configured stream is read.
type streamPlan struct {
	table    *table
	columns  []string
	selected map[string]bool
	cursor   *bigquery.FieldSchema
	from     *string
}

func quote(name string) string {
	return "`" + strings.ReplaceAll(strings.ReplaceAll(name, `\`, `\\`), "`", "\\`") + "`"
}

// query renders the statement that reads the plan's columns, and the cursor
// column when it is not one of them.
func (p streamPlan) query() (string, []bigquery.QueryParameter) {
	cols := slices.Clone(p.columns)
	if p.cursor != nil && !p.selected[p.cursor.Name] {
		cols = append(cols, p.cursor.Name)
	}
	exprs := make([]string, len(cols))
	for i, c := range cols {
		exprs[i] = quote(c)
	}
	var b strings.Builder
	fmt.Fprintf(&b, "SELECT %s FROM %s.%s.%s", strings.Join(exprs, ", "), quote(p.table.project), quote(p.table.dataset), quote(p.table.name))
	var params []bigquery.QueryParameter
	if p.cursor != nil {
		ref := quote(p.cursor.Name)
		if p.from != nil {
			fmt.Fprintf(&b, " WHERE %s >= CAST(@cursor AS %s)", ref, cursorTypes[p.cursor.Type])
			params = append(params, bigquery.QueryParameter{Name: "cursor", Value: *p.from})
		}
		fmt.Fprintf(&b, " ORDER BY %s ASC NULLS LAST", ref)
	}
	return b.String(), params
}

func (p streamPlan) rows(ctx context.Context, c *bigquery.Client) (*bigquery.RowIterator, error) {
	if p.cursor == nil && p.table.typ == bigquery.RegularTable {
		return c.DatasetInProject(p.table.project, p.table.dataset).Table(p.table.name).Read(ctx), nil
	}
	sql, params := p.query()
	q := c.Query(sql)
	q.Parameters = params
	return q.Read(ctx)
}

func readStream(ctx context.Context, c *bigquery.Client, cfg Config, cs synclet.ConfiguredStream, state *synclet.InputState, em *synclet.Emitter) error {
	desc := cs.Stream.Descriptor()
	p, err := planStream(ctx, c, cfg, cs, state)
	if err != nil {
		return err
	}
	incremental := cs.SyncMode == synclet.SyncModeIncremental
	it, err := p.rows(ctx, c)
	if err != nil {
		return fmt.Errorf("read %s: %w", streamName(desc), err)
	}
	st := streamState{Cursor: p.from}
	if p.cursor != nil {
		st.CursorField = []string{p.cursor.Name}
	}
	var n int
	for {
		var row []bigquery.Value
		err := it.Next(&row)
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return fmt.Errorf("read %s: %w", streamName(desc), err)
		}
		record := make(map[string]any, len(p.columns))
		for i, f := range it.Schema {
			isCursor := p.cursor != nil && f.Name == p.cursor.Name
			if !p.selected[f.Name] && !isCursor {
				continue
			}
			v, err := fieldValue(f, row[i])
			if err != nil {
				return fmt.Errorf("read %s: %w", streamName(desc), err)
			}
			if isCursor {
				if s, ok := cursorString(v); ok {
					st.Cursor = &s
				}
			}
			if p.selected[f.Name] {
				record[f.Name] = v
			}
		}
		if err := em.Record(desc, record); err != nil {
			return err
		}
		n++
		if incremental && n%checkpointRows == 0 && st.Cursor != nil {
			if err := em.StreamState(desc, st); err != nil {
				return err
			}
		}
	}
	if incremental {
		return em.StreamState(desc, st)
	}
	return nil
}

// planStream checks a configured stream against the table and the saved
// state and returns how to read it.
func planStream(ctx context.Context, c *bigquery.Client, cfg Config, cs synclet.ConfiguredStream, state *synclet.InputState) (streamPlan, error) {
	desc := cs.Stream.Descriptor()
	dataset := desc.Namespace
	if dataset == "" {
		if len(cfg.Datasets) != 1 {
			return streamPlan{}, synclet.ConfigError(fmt.Errorf("stream %s: set the stream's namespace to its dataset", streamName(desc)))
		}
		dataset = cfg.Datasets[0]
	}
	t, err := loadTable(ctx, c.Dataset(dataset).Table(desc.Name))
	switch {
	case apiCode(err) == http.StatusNotFound:
		return streamPlan{}, synclet.ConfigError(fmt.Errorf("stream %s: no such table or view in project %q", streamName(desc), cfg.ProjectID))
	case err != nil:
		return streamPlan{}, fmt.Errorf("stream %s: %w", streamName(desc), err)
	case !slices.Contains(streamTypes, t.typ):
		return streamPlan{}, synclet.ConfigError(fmt.Errorf("stream %s: tables of type %s are not read", streamName(desc), t.typ))
	}
	p := streamPlan{table: t, selected: map[string]bool{}}

	names, err := schemaColumns(cs.Stream.JSONSchema)
	if err != nil {
		return streamPlan{}, synclet.ConfigError(fmt.Errorf("stream %s: json_schema: %w", streamName(desc), err))
	}
	for _, name := range names {
		if _, ok := t.column(name); !ok {
			return streamPlan{}, synclet.ConfigError(fmt.Errorf("stream %s: column %q does not exist", streamName(desc), name))
		}
		p.selected[name] = true
	}
	for _, f := range t.schema {
		if p.selected[f.Name] {
			p.columns = append(p.columns, f.Name)
		}
	}

	if cs.SyncMode != synclet.SyncModeIncremental {
		return p, nil
	}
	field := cs.CursorField
	if len(field) == 0 {
		field = cs.Stream.DefaultCursorField
	}
	if len(field) != 1 {
		return streamPlan{}, synclet.ConfigError(fmt.Errorf("stream %s: incremental sync needs a cursor_field naming one column, got %q", streamName(desc), field))
	}
	f, ok := t.column(field[0])
	if !ok {
		return streamPlan{}, synclet.ConfigError(fmt.Errorf("stream %s: cursor column %q does not exist", streamName(desc), field[0]))
	}
	if _, ok := cursorTypes[f.Type]; !ok || f.Repeated {
		typ := string(f.Type)
		if f.Repeated {
			typ = "REPEATED " + typ
		}
		return streamPlan{}, synclet.ConfigError(fmt.Errorf("stream %s: column %q of type %s cannot be a cursor", streamName(desc), f.Name, typ))
	}
	p.cursor = f

	raw := state.Stream(desc)
	if raw == nil {
		return p, nil
	}
	var saved streamState
	if err := json.Unmarshal(raw, &saved); err != nil {
		return streamPlan{}, synclet.ConfigError(fmt.Errorf("stream %s: invalid state: %w", streamName(desc), err))
	}
	if !slices.Equal(saved.CursorField, field) {
		synclet.Logger(ctx).Warn("cursor field changed, reading the stream from the start",
			"stream", streamName(desc), "saved", strings.Join(saved.CursorField, "."), "configured", field[0])
		return p, nil
	}
	p.from = saved.Cursor
	return p, nil
}

func schemaColumns(raw json.RawMessage) ([]string, error) {
	var s struct {
		Properties map[string]json.RawMessage `json:"properties"`
	}
	if err := json.Unmarshal(raw, &s); err != nil {
		return nil, err
	}
	if len(s.Properties) == 0 {
		return nil, errors.New("no properties")
	}
	names := make([]string, 0, len(s.Properties))
	for name := range s.Properties {
		names = append(names, name)
	}
	slices.Sort(names)
	return names, nil
}

func streamName(desc synclet.StreamDescriptor) string {
	if desc.Namespace == "" {
		return desc.Name
	}
	return desc.Namespace + "." + desc.Name
}
