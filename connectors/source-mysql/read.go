package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	synclet "github.com/synclet-io/synclet-go-sdk"
	"go.uber.org/multierr"
)

// checkpointRows is how many records of an incremental stream go between
// two STATE messages.
const checkpointRows = 10_000

// streamState is the STREAM state of an incremental stream: the greatest
// cursor read so far, as MySQL renders the column as text; a binary cursor
// is base64-encoded and a BIT cursor an unsigned integer.
type streamState struct {
	CursorField []string `json:"cursor_field"`
	Cursor      *string  `json:"cursor,omitempty"`
}

// savedState is a saved STREAM state: a streamState, or the state of version
// 0.2, which has a phase, a cursor_field string and a cursor_value.
type savedState struct {
	CursorField json.RawMessage `json:"cursor_field"`
	Cursor      *string         `json:"cursor"`
	Phase       string          `json:"phase"`
	CursorValue any             `json:"cursor_value"`
}

// Read emits every configured stream: with CDC from the binlog, otherwise in
// catalog order, a full refresh reading the whole table and an incremental
// read the rows whose cursor is at least the saved one, in cursor order.
func (Source) Read(ctx context.Context, cfg Config, catalog *synclet.ConfiguredCatalog, state *synclet.InputState, em *synclet.Emitter) (rerr error) {
	cfg.applyDefaults()
	d, err := connect(ctx, cfg)
	if err != nil {
		return err
	}
	defer multierr.AppendInvoke(&rerr, multierr.Invoke(d.close))
	if cfg.cdc() {
		return readCDC(ctx, d.DB, cfg, catalog, state, em)
	}
	for _, cs := range catalog.Streams {
		if err := readStream(ctx, d.DB, cfg, cs, state, em); err != nil {
			return err
		}
	}
	return nil
}

// selectQuery reads the rows of a table: the columns, then the cursor.
type selectQuery struct {
	table   *table
	columns []column
	cursor  *column
	from    *string
	fromArg any
}

// sql returns the statement and its arguments.
func (q selectQuery) sql() (string, []any) {
	exprs := make([]string, 0, len(q.columns)+1)
	for _, c := range q.columns {
		exprs = append(exprs, c.expr()+" AS "+ident(c.name))
	}
	if q.cursor != nil {
		exprs = append(exprs, ident(q.cursor.name))
	}
	var b strings.Builder
	fmt.Fprintf(&b, "SELECT %s FROM %s", strings.Join(exprs, ", "), ident(q.table.schema, q.table.name))
	var args []any
	if q.cursor != nil {
		if q.from != nil {
			fmt.Fprintf(&b, " WHERE %s >= ?", ident(q.cursor.name))
			var arg any = *q.from
			if q.fromArg != nil {
				arg = q.fromArg
			}
			args = append(args, arg)
		}
		fmt.Fprintf(&b, " ORDER BY %s", ident(q.cursor.name))
	}
	return b.String(), args
}

// rowEncoder renders scanned rows of columns as JSON records.
type rowEncoder struct {
	desc         synclet.StreamDescriptor
	table        *table
	columns      []column
	failZeroDate bool
	fields       []field
}

func newRowEncoder(desc synclet.StreamDescriptor, t *table, columns []column, cfg Config) *rowEncoder {
	e := &rowEncoder{desc: desc, table: t, columns: columns, failZeroDate: cfg.ZeroDateHandling == zeroDateFail}
	e.fields = make([]field, len(columns))
	for i, c := range columns {
		e.fields[i].key = fieldKey(c.name)
	}
	return e
}

// fieldsOf returns the fields of a row; raw holds a value per column.
func (e *rowEncoder) fieldsOf(raw []sql.RawBytes) ([]field, error) {
	for i, c := range e.columns {
		v, err := c.encode(raw[i], e.failZeroDate)
		if errors.Is(err, errZeroDate) {
			return nil, e.zeroDateError(c, raw[i], raw)
		}
		if err != nil {
			return nil, err
		}
		e.fields[i].value = v
	}
	return e.fields, nil
}

func (e *rowEncoder) zeroDateError(c column, value []byte, raw []sql.RawBytes) error {
	row := "a row"
	var key []string
	for _, pk := range e.table.primaryKey {
		i := slices.IndexFunc(e.columns, func(c column) bool { return c.name == pk })
		if i < 0 {
			key = nil
			break
		}
		key = append(key, pk+"="+string(raw[i]))
	}
	if len(key) > 0 {
		row = "the row with " + strings.Join(key, ", ")
	}
	return synclet.ConfigError(fmt.Errorf("stream %s: column %q holds the zero date %q in %s; set zero_date_handling to %q to read zero dates as null",
		streamName(e.desc), c.name, value, row, zeroDateNull))
}

func readStream(ctx context.Context, conn *sql.DB, cfg Config, cs synclet.ConfiguredStream, state *synclet.InputState, em *synclet.Emitter) (rerr error) {
	desc := cs.Stream.Descriptor()
	q, err := planStream(ctx, conn, cfg, cs, state)
	if err != nil {
		return err
	}
	incremental := cs.SyncMode == synclet.SyncModeIncremental
	text, args := q.sql()
	rows, err := conn.QueryContext(ctx, text, args...)
	if err != nil {
		return fmt.Errorf("read %s: %w", streamName(desc), err)
	}
	defer multierr.AppendInvoke(&rerr, multierr.Close(rows))

	n := len(q.columns)
	if q.cursor != nil {
		n++
	}
	raw := make([]sql.RawBytes, n)
	dest := make([]any, n)
	for i := range raw {
		dest[i] = &raw[i]
	}
	enc := newRowEncoder(desc, q.table, q.columns, cfg)
	st := streamState{Cursor: q.from}
	if q.cursor != nil {
		st.CursorField = []string{q.cursor.name}
	}
	var buf []byte
	var count int
	for rows.Next() {
		if err := rows.Scan(dest...); err != nil {
			return fmt.Errorf("read %s: %w", streamName(desc), err)
		}
		fields, err := enc.fieldsOf(raw)
		if err != nil {
			return err
		}
		buf = appendRecord(buf[:0], fields)
		if err := em.Record(desc, json.RawMessage(buf)); err != nil {
			return err
		}
		if q.cursor != nil && raw[n-1] != nil {
			c := q.cursor.cursorText(raw[n-1])
			st.Cursor = &c
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
	if desc.Namespace != "" && desc.Namespace != cfg.Database {
		return selectQuery{}, synclet.ConfigError(fmt.Errorf("stream %s: namespace %q is not the configured database %q", streamName(desc), desc.Namespace, cfg.Database))
	}
	tables, err := loadTables(ctx, conn, cfg, desc.Name)
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

	raw := savedStreamState(state, desc)
	if raw == nil {
		return q, nil
	}
	var saved savedState
	if err := decodeState(raw, &saved); err != nil {
		return selectQuery{}, synclet.ConfigError(fmt.Errorf("stream %s: invalid state: %w", streamName(desc), err))
	}
	log := synclet.Logger(ctx)
	if saved.Phase != "" {
		var savedField string
		_ = json.Unmarshal(saved.CursorField, &savedField)
		switch {
		case savedField != field[0]:
			log.Warn("cursor field changed, reading the stream from the start",
				"stream", streamName(desc), "saved", savedField, "configured", field[0])
		case saved.Phase != "incremental":
			log.Warn("the saved state is an unfinished snapshot of version 0.2, reading the stream from the start",
				"stream", streamName(desc), "phase", saved.Phase)
		case lostBytes(c, saved.CursorValue):
			return selectQuery{}, synclet.ConfigError(fmt.Errorf("stream %s: the cursor of column %q saved by version 0.2 lost bytes that are not valid UTF-8; reset the state of this stream", streamName(desc), c.name))
		default:
			q.from = legacyCursor(c, saved.CursorValue)
		}
		return q.withArg(desc)
	}
	var savedField []string
	if err := json.Unmarshal(saved.CursorField, &savedField); err != nil {
		return selectQuery{}, synclet.ConfigError(fmt.Errorf("stream %s: invalid state: cursor_field: %w", streamName(desc), err))
	}
	if !slices.Equal(savedField, field) {
		log.Warn("cursor field changed, reading the stream from the start",
			"stream", streamName(desc), "saved", strings.Join(savedField, "."), "configured", field[0])
		return q, nil
	}
	q.from = saved.Cursor
	return q.withArg(desc)
}

// withArg sets the query argument of the saved cursor, q.from, which is in
// the form cursorText saves.
func (q selectQuery) withArg(desc synclet.StreamDescriptor) (selectQuery, error) {
	if q.from == nil {
		return q, nil
	}
	arg, err := q.cursor.cursorArg(*q.from)
	if err != nil {
		return selectQuery{}, synclet.ConfigError(fmt.Errorf("stream %s: %w", streamName(desc), err))
	}
	q.fromArg = arg
	return q, nil
}

// lostBytes reports whether a 0.2 cursor of a binary or BIT column holds
// U+FFFD: 0.2 saved the column's bytes as JSON text, which replaced bytes
// that are not valid UTF-8, so the cursor cannot be restored.
func lostBytes(c column, v any) bool {
	s, ok := v.(string)
	switch c.kind {
	case kindBinary, kindBit, kindBool:
		return ok && strings.ContainsRune(s, utf8.RuneError)
	}
	return false
}

// legacyCursor is the cursor of a 0.2 state in the form cursorText saves:
// numbers keep their digits, a date or time column's RFC 3339 value becomes
// a MySQL datetime in UTC, and the raw bytes 0.2 saved for a binary or BIT
// column become base64 or an unsigned integer.
func legacyCursor(c column, v any) *string {
	var s string
	switch x := v.(type) {
	case nil:
		return nil
	case json.Number:
		s = x.String()
	case string:
		s = x
		switch c.kind {
		case kindBinary, kindBit, kindBool:
			s = c.cursorText([]byte(x))
			return &s
		}
		if c.kind == kindDate || c.kind == kindDatetime || c.kind == kindTimestamp {
			if t, err := time.Parse(time.RFC3339Nano, x); err == nil {
				s = t.UTC().Format("2006-01-02 15:04:05.999999")
			}
		}
	case bool:
		s = "0"
		if x {
			s = "1"
		}
	default:
		s = fmt.Sprint(x)
	}
	return &s
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
