package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	mssql "github.com/microsoft/go-mssqldb"
	synclet "github.com/synclet-io/synclet-go-sdk"
)

const (
	// stageTable is the session temporary table one stream's batch is
	// bulk-copied into before it reaches the target table.
	stageTable = "#synclet_stage"
	// colSeq orders the rows of the stage table by arrival.
	colSeq = "_synclet_seq"
)

// batch holds the rows of one stream not yet committed and the position of
// each row's record in the stream.
type batch struct {
	rows    [][]any
	records []int
}

func (b *batch) reset() {
	b.rows = b.rows[:0]
	b.records = b.records[:0]
}

// writer buffers records of every stream and commits them in batches. A
// STATE is echoed once every record received before it is committed; the
// states of an overwrite stream, and GLOBAL and LEGACY states while the
// catalog has an overwrite stream, are held until the shadow tables replace
// the old ones.
type writer struct {
	conn   *sql.Conn
	schema string
	em     *synclet.Emitter
	limits Destination

	plans  []*streamPlan
	byDesc map[synclet.StreamDescriptor]*streamPlan

	batches  []batch
	received []int
	unknown  []map[string]struct{}
	rows     int
	size     int

	queued     []synclet.StateMessage
	held       []synclet.StateMessage
	overwrite  map[synclet.StreamDescriptor]bool
	lastCommit time.Time
}

func newWriter(conn *sql.Conn, schema string, em *synclet.Emitter, limits Destination, plans []*streamPlan) *writer {
	w := &writer{
		conn:       conn,
		schema:     schema,
		em:         em,
		limits:     limits,
		plans:      plans,
		byDesc:     map[synclet.StreamDescriptor]*streamPlan{},
		batches:    make([]batch, len(plans)),
		received:   make([]int, len(plans)),
		unknown:    make([]map[string]struct{}, len(plans)),
		overwrite:  map[synclet.StreamDescriptor]bool{},
		lastCommit: time.Now(),
	}
	for _, p := range plans {
		w.byDesc[p.desc] = p
		if p.mode == synclet.DestinationSyncModeOverwrite {
			w.overwrite[p.desc] = true
		}
	}
	return w
}

// prepare creates the schema when missing and makes every stream's table
// ready.
func (w *writer) prepare(ctx context.Context) error {
	if err := ensureSchema(ctx, w.conn, w.schema); err != nil {
		return err
	}
	for _, p := range w.plans {
		if err := prepareTable(ctx, w.conn, w.schema, p); err != nil {
			return fmt.Errorf("stream %q: %w", p.desc.Name, err)
		}
	}
	return nil
}

// addRecord validates one record, renders it into its stream's batch and
// flushes when the buffer reaches a limit.
func (w *writer) addRecord(ctx context.Context, r *synclet.RecordMessage) error {
	p, ok := w.byDesc[synclet.StreamDescriptor{Name: r.Stream, Namespace: r.Namespace}]
	if !ok {
		return fmt.Errorf("record of stream %q (namespace %q), which is not in the catalog", r.Stream, r.Namespace)
	}
	w.received[p.index]++
	pos := w.received[p.index]
	fail := func(format string, args ...any) error {
		return fmt.Errorf("stream %q: record %d: %s", p.desc.Name, pos, fmt.Sprintf(format, args...))
	}

	data := bytes.TrimSpace(r.Data)
	if len(data) == 0 || data[0] != '{' {
		return fail("not a JSON object")
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return fail("invalid JSON: %v", err)
	}
	for name := range fields {
		if _, ok := p.byName[name]; !ok {
			if w.unknown[p.index] == nil {
				w.unknown[p.index] = map[string]struct{}{}
			}
			w.unknown[p.index][name] = struct{}{}
		}
	}

	extracted := time.Now()
	if r.EmittedAt > 0 {
		extracted = time.UnixMilli(r.EmittedAt)
	}
	row := make([]any, 0, len(metaColumns)+len(p.columns))
	row = append(row, mssql.UniqueIdentifier(uuid.New()), extracted.UTC())
	for i, c := range p.columns {
		val, err := encodeValue(c.kind, fields[c.name])
		if err != nil {
			return fail("column %q: %v", c.name, err)
		}
		if slices.Contains(p.pk, i) {
			if val == nil {
				return fail("primary key column %q is null", c.name)
			}
			if err := checkKeyText(c, val); err != nil {
				return fail("primary key column %q: %v", c.name, err)
			}
		}
		row = append(row, val)
	}
	b := &w.batches[p.index]
	b.rows = append(b.rows, row)
	b.records = append(b.records, pos)
	w.rows++
	w.size += len(r.Data)

	if w.rows >= w.limits.maxRows || w.size >= w.limits.maxBytes {
		return w.flush(ctx)
	}
	return nil
}

// checkKeyText fails for a string key value NVARCHAR(255) cannot hold, and
// for one ending in a space: SQL Server ignores trailing spaces when it
// compares strings, so "a" and "a " would be one key.
func checkKeyText(c column, val any) error {
	s, ok := val.(string)
	if c.sqlType != keyTextType || !ok {
		return nil
	}
	if identLen(s) > maxKeyText {
		return fmt.Errorf("value is longer than %d characters, which a %s key column holds", maxKeyText, keyTextType)
	}
	if strings.HasSuffix(s, " ") {
		return fmt.Errorf("value %q ends with a space, which SQL Server ignores when it compares keys", s)
	}
	return nil
}

// addState echoes st when no record is pending; otherwise it queues st and
// flushes when the last commit is older than the age limit.
func (w *writer) addState(ctx context.Context, st synclet.StateMessage) error {
	if w.rows == 0 {
		return w.release([]synclet.StateMessage{st})
	}
	w.queued = append(w.queued, st)
	if time.Since(w.lastCommit) >= w.limits.maxAge {
		return w.flush(ctx)
	}
	return nil
}

// release echoes states whose records are committed in their final table
// and holds the rest until the swap.
func (w *writer) release(states []synclet.StateMessage) error {
	for _, st := range states {
		if w.isHeld(st) {
			w.held = append(w.held, st)
			continue
		}
		if err := emitState(w.em, st); err != nil {
			return fmt.Errorf("echo state: %w", err)
		}
	}
	return nil
}

// isHeld reports whether st waits for the swap. A STREAM state is matched
// to its catalog stream by name and namespace, and, when no stream has that
// exact namespace, by name alone: it is held if any overwrite stream has its
// name, or if it matches no catalog stream while the catalog has an
// overwrite stream.
func (w *writer) isHeld(st synclet.StateMessage) bool {
	if len(w.overwrite) == 0 {
		return false
	}
	if st.Type != synclet.StateTypeStream || st.Stream == nil {
		return true
	}
	desc := st.Stream.StreamDescriptor
	if _, ok := w.byDesc[desc]; ok {
		return w.overwrite[desc]
	}
	for d := range w.overwrite {
		if d.Name == desc.Name {
			return true
		}
	}
	for d := range w.byDesc {
		if d.Name == desc.Name {
			return false
		}
	}
	return true
}

// flush commits every pending row in one transaction, then echoes the
// queued states.
func (w *writer) flush(ctx context.Context) error {
	if w.rows > 0 {
		tx, err := w.conn.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("begin batch: %w", err)
		}
		defer func() { _ = tx.Rollback() }()
		for _, p := range w.plans {
			if len(w.batches[p.index].rows) == 0 {
				continue
			}
			if err := w.load(ctx, tx, p); err != nil {
				return fmt.Errorf("stream %q: load batch into %s: %w", p.desc.Name, qualified(w.schema, p.target()), err)
			}
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit batch: %w", err)
		}
		for i := range w.batches {
			w.batches[i].reset()
		}
		w.rows, w.size = 0, 0
		w.lastCommit = time.Now()
	}
	queued := w.queued
	w.queued = nil
	return w.release(queued)
}

// stageColumn names the column of the stage table that holds column i of
// allColumns; the target's names may hold characters the driver's bulk copy
// cannot quote.
func stageColumn(i int) string {
	return "c" + strconv.Itoa(i)
}

// load bulk-copies one stream's batch into the stage table and moves it
// into the target table.
func (w *writer) load(ctx context.Context, tx *sql.Tx, p *streamPlan) error {
	b := &w.batches[p.index]
	cols := p.allColumns()
	defs := []string{quote(colSeq) + " BIGINT NOT NULL"}
	names := []string{colSeq}
	for i, c := range cols {
		c.ident, c.key = stageColumn(i), false
		defs = append(defs, columnDef(c))
		names = append(names, c.ident)
	}
	if _, err := tx.ExecContext(ctx, "DROP TABLE IF EXISTS "+stageTable+"; CREATE TABLE "+stageTable+" ("+strings.Join(defs, ", ")+")"); err != nil {
		return fmt.Errorf("create stage table: %w", err)
	}
	stmt, err := tx.PrepareContext(ctx, mssql.CopyIn(stageTable, mssql.BulkOptions{KeepNulls: true}, names...))
	if err != nil {
		return fmt.Errorf("start bulk copy: %w", err)
	}
	args := make([]any, len(names))
	for i, row := range b.rows {
		args[0] = int64(b.records[i])
		copy(args[1:], row)
		if _, err := stmt.ExecContext(ctx, args...); err != nil {
			_ = stmt.Close()
			return fmt.Errorf("bulk copy record %d: %w", b.records[i], err)
		}
	}
	if _, err := stmt.ExecContext(ctx); err != nil {
		_ = stmt.Close()
		return fmt.Errorf("bulk copy: %w", err)
	}
	if err := stmt.Close(); err != nil {
		return fmt.Errorf("bulk copy: %w", err)
	}
	if _, err := tx.ExecContext(ctx, moveSQL(w.schema, p)); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, "DROP TABLE "+stageTable); err != nil {
		return fmt.Errorf("drop stage table: %w", err)
	}
	return nil
}

// moveSQL returns the statement that moves the stage table's rows into a
// stream's target table. append and overwrite insert them in arrival order.
// append_dedup keeps one row per key, the one with the highest cursor and,
// among equal cursors, the last arrival, and merges it into the table
// unless the stored row has a higher cursor; a NULL cursor is lower than
// any other.
func moveSQL(schema string, p *streamPlan) string {
	cols := p.allColumns()
	target := qualified(schema, p.target())
	stage := make([]string, len(cols))
	for i := range cols {
		stage[i] = stageColumn(i)
	}
	if p.mode != synclet.DestinationSyncModeAppendDedup {
		return "INSERT INTO " + target + " (" + identList(cols) + ") SELECT " + strings.Join(stage, ", ") +
			" FROM " + stageTable + " ORDER BY " + quote(colSeq)
	}

	stageOf := func(c column) string {
		return "s." + stageColumn(slices.IndexFunc(cols, func(x column) bool { return x.ident == c.ident }))
	}
	var keys, on []string
	for _, c := range p.pkColumns() {
		keys = append(keys, stageOf(c)[2:])
		on = append(on, "t."+quote(c.ident)+" = "+stageOf(c))
	}
	order := quote(colSeq) + " DESC"
	matched := "WHEN MATCHED"
	if p.cursor >= 0 {
		cur := p.columns[p.cursor]
		order = stageOf(cur)[2:] + " DESC, " + order
		matched += " AND (t." + quote(cur.ident) + " IS NULL OR " + stageOf(cur) + " >= t." + quote(cur.ident) + ")"
	}
	var set []string
	for _, c := range cols {
		if !c.key {
			set = append(set, quote(c.ident)+" = "+stageOf(c))
		}
	}
	vals := make([]string, len(cols))
	for i := range cols {
		vals[i] = "s." + stage[i]
	}
	return "MERGE INTO " + target + " WITH (HOLDLOCK) AS t USING (SELECT " + strings.Join(stage, ", ") +
		" FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY " + strings.Join(keys, ", ") + " ORDER BY " + order + ") AS _synclet_rn FROM " + stageTable +
		") AS r WHERE _synclet_rn = 1) AS s ON " + strings.Join(on, " AND ") +
		" " + matched + " THEN UPDATE SET " + strings.Join(set, ", ") +
		" WHEN NOT MATCHED THEN INSERT (" + identList(cols) + ") VALUES (" + strings.Join(vals, ", ") + ");"
}

// finish commits the remaining rows, replaces the tables of overwrite
// streams and echoes the held states.
func (w *writer) finish(ctx context.Context) error {
	if err := w.flush(ctx); err != nil {
		return err
	}
	if len(w.overwrite) > 0 {
		if err := swapTables(ctx, w.conn, w.schema, w.plans); err != nil {
			return err
		}
	}
	held := w.held
	w.held = nil
	for _, st := range held {
		if err := emitState(w.em, st); err != nil {
			return fmt.Errorf("echo state: %w", err)
		}
	}
	return nil
}

// warnUnknown logs, once per stream, the record fields the stream's schema
// does not have; they are not written.
func (w *writer) warnUnknown(ctx context.Context) {
	for _, p := range w.plans {
		if len(w.unknown[p.index]) == 0 {
			continue
		}
		names := make([]string, 0, len(w.unknown[p.index]))
		for n := range w.unknown[p.index] {
			names = append(names, n)
		}
		slices.Sort(names)
		synclet.Logger(ctx).Warn(fmt.Sprintf("stream %q: fields not in the catalog schema are not written: %s", p.desc.Name, strings.Join(names, ", ")))
	}
}
