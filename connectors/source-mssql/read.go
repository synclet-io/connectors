package main

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

// checkpointRows is how many records of an incremental stream go between
// two STATE messages.
const checkpointRows = 10_000

const tableAlias = "_synclet_t"

// streamState is the STREAM state of an incremental stream: the greatest
// cursor read so far, as SQL Server renders the column as text.
type streamState struct {
	CursorField []string `json:"cursor_field"`
	Cursor      *string  `json:"cursor,omitempty"`
}

// Read emits every configured stream in catalog order: a full refresh reads
// the whole table or view, an incremental read the rows whose cursor is at
// least the saved one, in cursor order. Rows are streamed: each record is
// emitted as it is read.
func (Source) Read(ctx context.Context, cfg Config, catalog *synclet.ConfiguredCatalog, state *synclet.InputState, em *synclet.Emitter) error {
	cfg.applyDefaults()
	d, err := connect(ctx, cfg)
	if err != nil {
		return err
	}
	defer d.close()
	for _, cs := range catalog.Streams {
		if err := readStream(ctx, d.DB, cfg, cs, state, em); err != nil {
			return err
		}
	}
	return nil
}

// selectQuery reads the rows of a table.
type selectQuery struct {
	table   *table
	columns []column
	cursor  *column
	from    *string
}

func ident(parts ...string) string {
	quoted := make([]string, len(parts))
	for i, p := range parts {
		quoted[i] = "[" + strings.ReplaceAll(p, "]", "]]") + "]"
	}
	return strings.Join(quoted, ".")
}

// sql returns the statement and its arguments: one expression per column,
// then the cursor as text.
func (q selectQuery) sql() (string, []any) {
	exprs := make([]string, 0, len(q.columns)+1)
	for _, c := range q.columns {
		exprs = append(exprs, c.typ.expr(tableAlias+"."+ident(c.name)))
	}
	if q.cursor != nil {
		exprs = append(exprs, q.cursor.typ.cursorExpr(tableAlias+"."+ident(q.cursor.name)))
	}
	var b strings.Builder
	fmt.Fprintf(&b, "SELECT %s FROM %s AS %s", strings.Join(exprs, ", "), ident(q.table.schema, q.table.name), tableAlias)
	var args []any
	if q.cursor != nil {
		ref := tableAlias + "." + ident(q.cursor.name)
		if q.from != nil {
			fmt.Fprintf(&b, " WHERE %s >= %s", ref, q.cursor.typ.cursorParam("@p1"))
			args = append(args, *q.from)
		}
		fmt.Fprintf(&b, " ORDER BY %s", ref)
	}
	return b.String(), args
}

// recordWriter renders one row as a JSON object into a buffer it reuses.
type recordWriter struct {
	columns []column
	keys    [][]byte
	buf     []byte
}

func newRecordWriter(columns []column) (*recordWriter, error) {
	w := &recordWriter{columns: columns, keys: make([][]byte, len(columns))}
	for i, c := range columns {
		k, err := json.Marshal(c.name)
		if err != nil {
			return nil, err
		}
		w.keys[i] = k
	}
	return w, nil
}

func (w *recordWriter) write(values []any) ([]byte, error) {
	w.buf = append(w.buf[:0], '{')
	for i, c := range w.columns {
		if i > 0 {
			w.buf = append(w.buf, ',')
		}
		w.buf = append(w.buf, w.keys[i]...)
		w.buf = append(w.buf, ':')
		var err error
		w.buf, err = appendValue(w.buf, c.typ.kind(), values[i])
		if err != nil {
			return nil, fmt.Errorf("column %q: %w", c.name, err)
		}
	}
	return append(w.buf, '}'), nil
}

// appendValue appends v, as the driver scanned the expression of a column
// of kind k, as the JSON value the column's schema declares.
func appendValue(b []byte, k kind, v any) ([]byte, error) {
	switch v := v.(type) {
	case nil:
		return append(b, "null"...), nil
	case bool:
		return strconv.AppendBool(b, v), nil
	case int64:
		return strconv.AppendInt(b, v, 10), nil
	case float64:
		if k == kindReal {
			return strconv.AppendFloat(b, float64(float32(v)), 'g', -1, 32), nil
		}
		return strconv.AppendFloat(b, v, 'g', -1, 64), nil
	case float32:
		return strconv.AppendFloat(b, float64(v), 'g', -1, 32), nil
	case []byte:
		if k != kindBinary {
			return appendString(b, string(v))
		}
		b = append(b, '"')
		b = base64.StdEncoding.AppendEncode(b, v)
		return append(b, '"'), nil
	case string:
		if k == kindDecimal || k == kindMoney {
			return appendNumber(b, v)
		}
		return appendString(b, v)
	default:
		return nil, fmt.Errorf("unexpected value of type %T", v)
	}
}

func appendString(b []byte, s string) ([]byte, error) {
	enc, err := json.Marshal(s)
	if err != nil {
		return nil, err
	}
	return append(b, enc...), nil
}

// appendNumber appends the decimal text s as a JSON number with every digit.
func appendNumber(b []byte, s string) ([]byte, error) {
	s = strings.TrimSpace(s)
	neg := strings.HasPrefix(s, "-")
	digits := strings.TrimPrefix(s, "-")
	if strings.HasPrefix(digits, ".") {
		digits = "0" + digits
	}
	if neg {
		digits = "-" + digits
	}
	if !json.Valid([]byte(digits)) {
		return nil, fmt.Errorf("%q is not a number", s)
	}
	return append(b, digits...), nil
}

func readStream(ctx context.Context, conn *sql.DB, cfg Config, cs synclet.ConfiguredStream, state *synclet.InputState, em *synclet.Emitter) error {
	desc := cs.Stream.Descriptor()
	q, err := planStream(ctx, conn, cfg, cs, state)
	if err != nil {
		return err
	}
	w, err := newRecordWriter(q.columns)
	if err != nil {
		return err
	}
	incremental := cs.SyncMode == synclet.SyncModeIncremental
	query, args := q.sql()
	rows, err := conn.QueryContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("read %s: %w", streamName(desc), err)
	}
	defer func() { _ = rows.Close() }()

	st := streamState{Cursor: q.from}
	if q.cursor != nil {
		st.CursorField = []string{q.cursor.name}
	}
	n := len(q.columns)
	if q.cursor != nil {
		n++
	}
	values := make([]any, n)
	targets := make([]any, n)
	for i := range values {
		targets[i] = &values[i]
	}
	var count int
	for rows.Next() {
		if err := rows.Scan(targets...); err != nil {
			return fmt.Errorf("read %s: %w", streamName(desc), err)
		}
		record, err := w.write(values)
		if err != nil {
			return fmt.Errorf("read %s: %w", streamName(desc), err)
		}
		if err := em.Record(desc, json.RawMessage(record)); err != nil {
			return err
		}
		if q.cursor != nil {
			if s, ok := values[len(values)-1].(string); ok {
				st.Cursor = &s
			}
		}
		count++
		if incremental && count%checkpointRows == 0 && st.Cursor != nil {
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
func planStream(ctx context.Context, conn *sql.DB, cfg Config, cs synclet.ConfiguredStream, state *synclet.InputState) (selectQuery, error) {
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
	if len(tables) > 1 {
		in := make([]string, len(tables))
		for i, t := range tables {
			in[i] = strconv.Quote(t.schema)
		}
		return selectQuery{}, synclet.ConfigError(fmt.Errorf("stream %s: a table or view of that name is in schemas %s; set the stream's namespace", streamName(desc), strings.Join(in, ", ")))
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
	if !c.typ.orderable() {
		return selectQuery{}, synclet.ConfigError(fmt.Errorf("stream %s: cursor column %q is of type %s, which SQL Server cannot order; pick a rowversion, datetime2, integer or other comparable column", streamName(desc), c.name, c.typ.name))
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
