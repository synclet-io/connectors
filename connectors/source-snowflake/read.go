package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

// checkpointRows is how many records of an incremental stream go between
// two STATE messages.
var checkpointRows = 10_000

// streamState is the STREAM state of an incremental stream: the greatest
// cursor read so far, in the text form the cursor column is selected in.
type streamState struct {
	CursorField []string `json:"cursor_field"`
	Cursor      *string  `json:"cursor,omitempty"`
}

// Read emits every configured stream in catalog order. An incremental read
// selects the rows whose cursor is at least the saved one, in cursor order;
// rows with a NULL cursor come last and do not move the state. A NaN or
// infinite FLOAT cursor fails the read: no saved cursor could resume past it.
func (s Source) Read(ctx context.Context, cfg Config, catalog *synclet.ConfiguredCatalog, state *synclet.InputState, em *synclet.Emitter) error {
	c, err := s.connect(ctx, cfg)
	if err != nil {
		return err
	}
	defer c.close()
	for _, cs := range catalog.Streams {
		if err := c.readStream(ctx, cs, state, em); err != nil {
			return err
		}
	}
	return nil
}

// selected is a column a read selects.
type selected struct {
	column
	kind kind
}

// streamPlan is how one configured stream is read.
type streamPlan struct {
	ref     string
	columns []selected
	cursor  *selected
	from    *string
}

// query renders the statement that reads the plan's columns, and the cursor
// column last when it is not one of them.
func (p streamPlan) query() (string, []any) {
	cols := slices.Clone(p.columns)
	if p.cursor != nil && p.cursorIndex() < 0 {
		cols = append(cols, *p.cursor)
	}
	exprs := make([]string, len(cols))
	for i, c := range cols {
		exprs[i] = c.kind.expr(ident(c.name))
	}
	var b strings.Builder
	fmt.Fprintf(&b, "SELECT %s FROM %s", strings.Join(exprs, ", "), p.ref)
	var args []any
	if p.cursor != nil {
		ref := ident(p.cursor.name)
		if p.from != nil {
			fmt.Fprintf(&b, " WHERE %s >= %s", ref, cursorParam(p.cursor.column, p.cursor.kind))
			args = append(args, *p.from)
		}
		fmt.Fprintf(&b, " ORDER BY %s ASC NULLS LAST", ref)
	}
	return b.String(), args
}

// cursorIndex is the position of the cursor among the selected columns, or
// -1 when it is selected after them.
func (p streamPlan) cursorIndex() int {
	return slices.IndexFunc(p.columns, func(c selected) bool { return c.name == p.cursor.name })
}

func (c *conn) readStream(ctx context.Context, cs synclet.ConfiguredStream, state *synclet.InputState, em *synclet.Emitter) error {
	desc := cs.Stream.Descriptor()
	p, err := c.planStream(ctx, cs, state)
	if err != nil {
		return err
	}
	incremental := cs.SyncMode == synclet.SyncModeIncremental
	sqlText, args := p.query()
	rows, err := c.db.QueryContext(ctx, sqlText, args...)
	if err != nil {
		return fmt.Errorf("read %s: %w", p.ref, c.queryError(err))
	}
	defer func() { _ = rows.Close() }()

	st := streamState{Cursor: p.from}
	cursorAt := -1
	if p.cursor != nil {
		st.CursorField = []string{p.cursor.name}
		if cursorAt = p.cursorIndex(); cursorAt < 0 {
			cursorAt = len(p.columns)
		}
	}
	n := len(p.columns)
	if p.cursor != nil && cursorAt == n {
		n++
	}
	cells := make([]sql.NullString, n)
	ptrs := make([]any, n)
	for i := range cells {
		ptrs[i] = &cells[i]
	}
	var count int
	for rows.Next() {
		if err := rows.Scan(ptrs...); err != nil {
			return fmt.Errorf("read %s: %w", p.ref, err)
		}
		record := make(map[string]any, len(p.columns))
		for i, col := range p.columns {
			v, err := col.kind.value(cells[i])
			if err != nil {
				return fmt.Errorf("read %s: column %q: %w", p.ref, col.name, err)
			}
			record[col.name] = v
		}
		if cursorAt >= 0 && cells[cursorAt].Valid {
			cv, err := p.cursor.kind.value(cells[cursorAt])
			if err != nil {
				return fmt.Errorf("read %s: cursor column %q: %w", p.ref, p.cursor.name, err)
			}
			if cv == nil {
				return fmt.Errorf("stream %s: cursor column %q is %s at row %d: NaN and infinity cannot be saved as a cursor, "+
					"because Snowflake orders them above every number and a resumed read would never get past them",
					streamName(desc), p.cursor.name, cells[cursorAt].String, count+1)
			}
			v := cells[cursorAt].String
			st.Cursor = &v
		}
		if err := em.Record(desc, record); err != nil {
			return err
		}
		count++
		if incremental && count%checkpointRows == 0 && st.Cursor != nil {
			if err := em.StreamState(desc, st); err != nil {
				return err
			}
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("read %s: %w", p.ref, c.queryError(err))
	}
	if incremental {
		return em.StreamState(desc, st)
	}
	return nil
}

// planStream checks a configured stream against the table and the saved
// state and returns how to read it.
func (c *conn) planStream(ctx context.Context, cs synclet.ConfiguredStream, state *synclet.InputState) (streamPlan, error) {
	desc := cs.Stream.Descriptor()
	name := streamName(desc)
	schema := desc.Namespace
	if schema == "" {
		if len(c.cfg.Schemas) != 1 {
			return streamPlan{}, synclet.ConfigError(fmt.Errorf("stream %s: set the stream's namespace to its schema", name))
		}
		schema = c.cfg.Schemas[0]
	}
	t := &table{schema: schema, name: desc.Name}
	ref := c.tableRef(schema, desc.Name)
	rows, err := c.rows(ctx, `SELECT COLUMN_NAME, DATA_TYPE, NUMERIC_PRECISION, NUMERIC_SCALE FROM `+ident(c.database)+
		`.INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION`, schema, desc.Name)
	if err != nil {
		return streamPlan{}, fmt.Errorf("stream %s: list columns: %w", name, err)
	}
	if len(rows) == 0 {
		return streamPlan{}, synclet.ConfigError(fmt.Errorf("stream %s: no table or view %s is visible to role %q", name, ref, c.role))
	}
	for _, r := range rows {
		col, err := parseColumn(r)
		if err != nil {
			return streamPlan{}, fmt.Errorf("stream %s: %w", name, err)
		}
		t.columns = append(t.columns, col)
	}
	p := streamPlan{ref: ref}

	names, err := schemaColumns(cs.Stream.JSONSchema)
	if err != nil {
		return streamPlan{}, synclet.ConfigError(fmt.Errorf("stream %s: json_schema: %w", name, err))
	}
	for _, n := range names {
		if _, ok := t.column(n); !ok {
			return streamPlan{}, synclet.ConfigError(fmt.Errorf("stream %s: column %q does not exist", name, n))
		}
	}
	for _, col := range t.columns {
		if !slices.Contains(names, col.name) {
			continue
		}
		k, err := col.kind()
		if err != nil {
			return streamPlan{}, synclet.ConfigError(fmt.Errorf("stream %s: %w", name, err))
		}
		p.columns = append(p.columns, selected{col, k})
	}

	if cs.SyncMode != synclet.SyncModeIncremental {
		return p, nil
	}
	field := cs.CursorField
	if len(field) == 0 {
		field = cs.Stream.DefaultCursorField
	}
	if len(field) != 1 {
		return streamPlan{}, synclet.ConfigError(fmt.Errorf("stream %s: incremental sync needs a cursor_field naming one column, got %q", name, field))
	}
	col, ok := t.column(field[0])
	if !ok {
		return streamPlan{}, synclet.ConfigError(fmt.Errorf("stream %s: cursor column %q does not exist", name, field[0]))
	}
	k, err := col.kind()
	if err != nil || !cursorKinds[k] {
		return streamPlan{}, synclet.ConfigError(fmt.Errorf("stream %s: column %q of type %s cannot be a cursor", name, col.name, col.dataType))
	}
	p.cursor = &selected{col, k}

	raw := state.Stream(desc)
	if raw == nil {
		return p, nil
	}
	var saved streamState
	if err := json.Unmarshal(raw, &saved); err != nil {
		return streamPlan{}, synclet.ConfigError(fmt.Errorf("stream %s: invalid state: %w", name, err))
	}
	if !slices.Equal(saved.CursorField, field) {
		synclet.Logger(ctx).Warn("cursor field changed, reading the stream from the start",
			"stream", name, "saved", strings.Join(saved.CursorField, "."), "configured", field[0])
		return p, nil
	}
	if saved.Cursor != nil {
		v, err := k.value(sql.NullString{String: *saved.Cursor, Valid: true})
		if err != nil {
			return streamPlan{}, synclet.ConfigError(fmt.Errorf("stream %s: invalid state cursor: %w", name, err))
		}
		if v == nil {
			return streamPlan{}, synclet.ConfigError(fmt.Errorf("stream %s: invalid state cursor %q: NaN and infinity are not cursor values", name, *saved.Cursor))
		}
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
