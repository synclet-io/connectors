package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/jackc/pgx/v5"
	synclet "github.com/synclet-io/synclet-go-sdk"
)

// checkpointRows is how many records of an incremental stream go between
// two STATE messages.
const checkpointRows = 10_000

const (
	tableAlias  = "_synclet_t"
	recordAlias = "_synclet_r"
)

// streamState is the STREAM state of an incremental stream: the greatest
// cursor read so far, as Postgres renders the column as text.
type streamState struct {
	CursorField []string `json:"cursor_field"`
	Cursor      *string  `json:"cursor,omitempty"`
}

// Read emits every configured stream in catalog order: a full refresh reads
// the whole relation, an incremental read the rows whose cursor is at least
// the saved one, in cursor order.
func (Source) Read(ctx context.Context, cfg Config, catalog *synclet.ConfiguredCatalog, state *synclet.InputState, em *synclet.Emitter) error {
	cfg.applyDefaults()
	d, err := connect(ctx, cfg)
	if err != nil {
		return err
	}
	defer d.close()
	for _, cs := range catalog.Streams {
		if err := readStream(ctx, d.Conn, cfg, cs, state, em); err != nil {
			return err
		}
	}
	return nil
}

// selectQuery renders the rows of a table as JSON records.
type selectQuery struct {
	table   *table
	columns []column
	cursor  *column
	from    *string
}

func ident(parts ...string) string { return pgx.Identifier(parts).Sanitize() }

// sql returns the statement and its arguments. Postgres renders each record
// with row_to_json, so numbers and JSON keep every digit; the second column
// is the cursor as text.
func (q selectQuery) sql() (string, []any) {
	exprs := make([]string, len(q.columns))
	for i, c := range q.columns {
		exprs[i] = c.typ.expr(tableAlias+"."+ident(c.name)) + " AS " + ident(c.name)
	}
	cursor := "NULL::text"
	if q.cursor != nil {
		cursor = tableAlias + "." + ident(q.cursor.name) + "::text"
	}
	var b strings.Builder
	fmt.Fprintf(&b, "SELECT row_to_json(%s)::text, %s FROM %s AS %s, LATERAL (SELECT %s) AS %s",
		recordAlias, cursor, ident(q.table.schema, q.table.name), tableAlias, strings.Join(exprs, ", "), recordAlias)
	var args []any
	if q.cursor != nil {
		ref := tableAlias + "." + ident(q.cursor.name)
		if q.from != nil {
			fmt.Fprintf(&b, " WHERE %s >= $1::%s", ref, q.cursor.sqlType)
			args = append(args, *q.from)
		}
		fmt.Fprintf(&b, " ORDER BY %s", ref)
	}
	return b.String(), args
}

func readStream(ctx context.Context, conn *pgx.Conn, cfg Config, cs synclet.ConfiguredStream, state *synclet.InputState, em *synclet.Emitter) error {
	desc := cs.Stream.Descriptor()
	q, err := planStream(ctx, conn, cfg, cs, state)
	if err != nil {
		return err
	}
	incremental := cs.SyncMode == synclet.SyncModeIncremental
	sql, args := q.sql()
	rows, err := conn.Query(ctx, sql, args...)
	if err != nil {
		return fmt.Errorf("read %s: %w", streamName(desc), err)
	}
	defer rows.Close()

	st := streamState{Cursor: q.from}
	if q.cursor != nil {
		st.CursorField = []string{q.cursor.name}
	}
	var n int
	for rows.Next() {
		var record []byte
		var cursor *string
		if err := rows.Scan(&record, &cursor); err != nil {
			return fmt.Errorf("read %s: %w", streamName(desc), err)
		}
		if err := em.Record(desc, json.RawMessage(record)); err != nil {
			return err
		}
		if cursor != nil {
			st.Cursor = cursor
		}
		n++
		if incremental && n%checkpointRows == 0 && st.Cursor != nil {
			if err := em.StreamState(desc, st); err != nil {
				return err
			}
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("read %s: %w", streamName(desc), err)
	}
	if incremental {
		return em.StreamState(desc, st)
	}
	return nil
}

// planStream checks a configured stream against the database and the saved
// state and returns the query that reads it.
func planStream(ctx context.Context, conn *pgx.Conn, cfg Config, cs synclet.ConfiguredStream, state *synclet.InputState) (selectQuery, error) {
	desc := cs.Stream.Descriptor()
	schemas := cfg.Schemas
	if desc.Namespace != "" {
		schemas = []string{desc.Namespace}
	}
	tables, err := loadTables(ctx, conn, schemas, desc.Name)
	if err != nil {
		return selectQuery{}, err
	}
	if len(tables) == 0 {
		return selectQuery{}, synclet.ConfigError(fmt.Errorf("stream %s: no such table or view, or user %q may not SELECT from it", streamName(desc), cfg.Username))
	}
	t := tables[0]
	q := selectQuery{table: t}

	names, err := schemaColumns(cs.Stream.JSONSchema)
	if err != nil {
		return selectQuery{}, synclet.ConfigError(fmt.Errorf("stream %s: json_schema: %w", streamName(desc), err))
	}
	for _, c := range t.columns {
		if slices.Contains(names, c.name) {
			q.columns = append(q.columns, c)
		}
	}
	for _, name := range names {
		if _, ok := t.column(name); !ok {
			return selectQuery{}, synclet.ConfigError(fmt.Errorf("stream %s: column %q does not exist", streamName(desc), name))
		}
	}

	if cs.SyncMode != synclet.SyncModeIncremental {
		return q, nil
	}
	field := cs.CursorField
	if len(field) == 0 {
		field = cs.Stream.DefaultCursorField
	}
	if len(field) != 1 {
		return selectQuery{}, synclet.ConfigError(fmt.Errorf("stream %s: incremental sync needs a cursor_field naming one column, got %q", streamName(desc), field))
	}
	c, ok := t.column(field[0])
	if !ok {
		return selectQuery{}, synclet.ConfigError(fmt.Errorf("stream %s: cursor column %q does not exist", streamName(desc), field[0]))
	}
	q.cursor = &c

	raw := state.Stream(desc)
	if raw == nil {
		return q, nil
	}
	var saved streamState
	if err := json.Unmarshal(raw, &saved); err != nil {
		return selectQuery{}, synclet.ConfigError(fmt.Errorf("stream %s: invalid state: %w", streamName(desc), err))
	}
	if !slices.Equal(saved.CursorField, field) {
		synclet.Logger(ctx).Warn("cursor field changed, reading the stream from the start",
			"stream", streamName(desc), "saved", strings.Join(saved.CursorField, "."), "configured", field[0])
		return q, nil
	}
	q.from = saved.Cursor
	return q, nil
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
