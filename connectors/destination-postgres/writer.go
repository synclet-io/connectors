package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	synclet "github.com/synclet-io/synclet-go-sdk"
)

// batch holds the rows of one stream not yet committed, in COPY text format,
// and the position of each row's record in the stream.
type batch struct {
	rows    bytes.Buffer
	records []int
}

// writer buffers records of every stream and commits them in batches. A
// STATE is echoed once every record received before it is committed; the
// states of an overwrite stream, and GLOBAL and LEGACY states while the
// catalog has an overwrite stream, are held until the shadow tables replace
// the old ones.
type writer struct {
	conn   *pgx.Conn
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
	seq      int64

	queued     []synclet.StateMessage
	held       []synclet.StateMessage
	overwrite  map[synclet.StreamDescriptor]bool
	lastCommit time.Time
}

func newWriter(conn *pgx.Conn, schema string, em *synclet.Emitter, limits Destination, plans []*streamPlan) *writer {
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

// prepare creates the schema and makes every stream's table ready.
func (w *writer) prepare(ctx context.Context) error {
	if _, err := w.conn.Exec(ctx, "CREATE SCHEMA IF NOT EXISTS "+quote(w.schema)); err != nil {
		return fmt.Errorf("create schema %q: %w", w.schema, err)
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
	for _, idx := range p.pk {
		name := p.columns[idx].name
		if v, ok := fields[name]; !ok || string(bytes.TrimSpace(v)) == "null" {
			return fail("primary key column %q is null", name)
		}
	}

	extracted := time.Now()
	if r.EmittedAt > 0 {
		extracted = time.UnixMilli(r.EmittedAt)
	}
	w.seq++
	b := &w.batches[p.index]
	before := b.rows.Len()
	b.rows.WriteString(strconv.FormatInt(w.seq, 10))
	b.rows.WriteString("\t" + uuid.NewString())
	b.rows.WriteString("\t" + extracted.UTC().Format(time.RFC3339Nano))
	for _, c := range p.columns {
		b.rows.WriteByte('\t')
		val, null, err := encodeValue(c.typ, fields[c.name])
		if err != nil {
			b.rows.Truncate(before)
			return fail("column %q: %v", c.name, err)
		}
		if null {
			b.rows.WriteString(`\N`)
			continue
		}
		copyEscape(&b.rows, val)
	}
	b.rows.WriteByte('\n')
	b.records = append(b.records, pos)
	w.rows++
	w.size += len(r.Data)

	if w.rows >= w.limits.maxRows || w.size >= w.limits.maxBytes {
		return w.flush(ctx)
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

func (w *writer) isHeld(st synclet.StateMessage) bool {
	if st.Type == synclet.StateTypeStream && st.Stream != nil {
		return w.overwrite[st.Stream.StreamDescriptor]
	}
	return len(w.overwrite) > 0
}

// flush commits every pending row in one transaction, then echoes the
// queued states.
func (w *writer) flush(ctx context.Context) error {
	if w.rows > 0 {
		tx, err := w.conn.Begin(ctx)
		if err != nil {
			return fmt.Errorf("begin batch: %w", err)
		}
		defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
		for _, p := range w.plans {
			if len(w.batches[p.index].records) == 0 {
				continue
			}
			if err := w.load(ctx, tx, p); err != nil {
				return err
			}
		}
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("commit batch: %w", err)
		}
		for i := range w.batches {
			w.batches[i].rows.Reset()
			w.batches[i].records = w.batches[i].records[:0]
		}
		w.rows, w.size = 0, 0
		w.lastCommit = time.Now()
	}
	queued := w.queued
	w.queued = nil
	return w.release(queued)
}

// load copies one stream's batch into a temporary table and inserts it into
// the stream's target table, merging on the primary key for append_dedup.
func (w *writer) load(ctx context.Context, tx pgx.Tx, p *streamPlan) error {
	stage := quote(fmt.Sprintf("_synclet_stage_%d", p.index))
	cols := p.allColumns()
	defs := append([]string{quote(colSeq) + " bigint"}, columnDefs(cols)...)
	if _, err := tx.Exec(ctx, "CREATE TEMP TABLE "+stage+" ("+strings.Join(defs, ", ")+") ON COMMIT DROP"); err != nil {
		return fmt.Errorf("stream %q: create staging table: %w", p.desc.Name, err)
	}
	b := &w.batches[p.index]
	copySQL := "COPY " + stage + " (" + quote(colSeq) + ", " + identList(cols) + ") FROM STDIN"
	if _, err := tx.Conn().PgConn().CopyFrom(ctx, bytes.NewReader(b.rows.Bytes()), copySQL); err != nil {
		return w.copyError(p, b, err)
	}
	if _, err := tx.Exec(ctx, insertSQL(w.schema, p, stage)); err != nil {
		return fmt.Errorf("stream %q: insert batch into %s: %w", p.desc.Name, quote(w.schema, p.target()), err)
	}
	return nil
}

// insertSQL moves a staged batch into the target table. append_dedup keeps
// one row per primary key: the one with the highest cursor, the latest
// arrival among equal cursors, and replaces a stored row only when its cursor
// is not lower.
func insertSQL(schema string, p *streamPlan, stage string) string {
	cols := identList(p.allColumns())
	target := quote(schema, p.target())
	if p.mode != synclet.DestinationSyncModeAppendDedup {
		return "INSERT INTO " + target + " (" + cols + ") SELECT " + cols + " FROM " + stage + " ORDER BY " + quote(colSeq)
	}
	pk := identList(p.pkColumns())
	order := pk + ", "
	if p.cursor >= 0 {
		order += quote(p.columns[p.cursor].ident) + " DESC NULLS LAST, "
	}
	order += quote(colSeq) + " DESC"
	var set []string
	for _, c := range p.allColumns() {
		if slices.ContainsFunc(p.pk, func(i int) bool { return p.columns[i].ident == c.ident }) {
			continue
		}
		set = append(set, quote(c.ident)+" = EXCLUDED."+quote(c.ident))
	}
	sql := "INSERT INTO " + target + " AS t (" + cols + ") SELECT DISTINCT ON (" + pk + ") " + cols +
		" FROM " + stage + " ORDER BY " + order +
		" ON CONFLICT (" + pk + ") DO UPDATE SET " + strings.Join(set, ", ")
	if p.cursor >= 0 {
		cur := quote(p.columns[p.cursor].ident)
		sql += " WHERE t." + cur + " IS NULL OR EXCLUDED." + cur + " >= t." + cur
	}
	return sql
}

var copyWhere = regexp.MustCompile(`line (\d+)(?:, column ([^:]+))?`)

// copyError names the stream, the record and the column of a value Postgres
// rejected while loading a batch.
func (w *writer) copyError(p *streamPlan, b *batch, err error) error {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return fmt.Errorf("stream %q: load batch: %w", p.desc.Name, err)
	}
	m := copyWhere.FindStringSubmatch(pgErr.Where)
	if m == nil {
		return fmt.Errorf("stream %q: load batch: %s (SQLSTATE %s)", p.desc.Name, pgErr.Message, pgErr.Code)
	}
	line, _ := strconv.Atoi(m[1])
	pos := "a record"
	if line >= 1 && line <= len(b.records) {
		pos = "record " + strconv.Itoa(b.records[line-1])
	}
	if m[2] == "" {
		return fmt.Errorf("stream %q: %s: %s (SQLSTATE %s)", p.desc.Name, pos, pgErr.Message, pgErr.Code)
	}
	name := m[2]
	if orig, ok := p.byIdent[name]; ok {
		name = orig
	}
	return fmt.Errorf("stream %q: %s: column %q: %s (SQLSTATE %s)", p.desc.Name, pos, name, pgErr.Message, pgErr.Code)
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
