package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
	synclet "github.com/synclet-io/synclet-go-sdk"
)

// Limits of one INSERT statement: rows, and record data in bytes.
const (
	chunkRows  = 1000
	chunkBytes = 4 << 20
)

// batch holds the rows of one stream not yet committed, the size of each
// row's record and the position of each row's record in the stream.
type batch struct {
	rows    [][]any
	sizes   []int
	records []int
}

func (b *batch) reset() {
	b.rows = b.rows[:0]
	b.sizes = b.sizes[:0]
	b.records = b.records[:0]
}

// writer buffers records of every stream and commits them in batches. A
// STATE is echoed once every record received before it is committed; the
// states of an overwrite stream, and GLOBAL and LEGACY states while the
// catalog has an overwrite stream, are held until the shadow tables replace
// the old ones.
type writer struct {
	conn   *sql.Conn
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

func newWriter(conn *sql.Conn, em *synclet.Emitter, limits Destination, plans []*streamPlan) *writer {
	w := &writer{
		conn:       conn,
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

// prepare makes every stream's table ready.
func (w *writer) prepare(ctx context.Context) error {
	for _, p := range w.plans {
		if err := prepareTable(ctx, w.conn, p); err != nil {
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
	row = append(row, uuid.NewString(), extracted.UTC().Format("2006-01-02 15:04:05.000000"))
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
	b.sizes = append(b.sizes, len(r.Data))
	b.records = append(b.records, pos)
	w.rows++
	w.size += len(r.Data)

	if w.rows >= w.limits.maxRows || w.size >= w.limits.maxBytes {
		return w.flush(ctx)
	}
	return nil
}

// checkKeyText fails for a string key value VARCHAR(255) under utf8mb4_bin
// cannot hold as given: longer than 255 characters, or ending in a space,
// which the collation ignores when it compares keys.
func checkKeyText(c column, val any) error {
	s, ok := val.(string)
	if !c.key || !ok {
		return nil
	}
	if utf8.RuneCountInString(s) > maxKeyText {
		return fmt.Errorf("value is longer than %d characters, which a %s key column holds", maxKeyText, keyTextType)
	}
	if strings.HasSuffix(s, " ") {
		return fmt.Errorf("value %q ends with a space, which MySQL ignores when it compares keys", s)
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
				return err
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

// load inserts one stream's batch into its target table in multi-row INSERT
// statements of at most chunkRows rows or chunkBytes of record data.
func (w *writer) load(ctx context.Context, tx *sql.Tx, p *streamPlan) error {
	b := &w.batches[p.index]
	head, tail := insertSQL(p)
	ncols := len(metaColumns) + len(p.columns)
	placeholders := "(" + strings.Repeat("?, ", ncols-1) + "?)"
	for start := 0; start < len(b.rows); {
		end, size := start, 0
		for end < len(b.rows) && end-start < chunkRows && (end == start || size+b.sizes[end] <= chunkBytes) {
			size += b.sizes[end]
			end++
		}
		var q strings.Builder
		q.WriteString(head)
		args := make([]any, 0, (end-start)*ncols)
		for i := start; i < end; i++ {
			if i > start {
				q.WriteString(", ")
			}
			q.WriteString(placeholders)
			args = append(args, b.rows[i]...)
		}
		q.WriteString(tail)
		if _, err := tx.ExecContext(ctx, q.String(), args...); err != nil {
			return w.insertError(p, b.records[start:end], err)
		}
		start = end
	}
	return nil
}

// insertSQL returns the statement that inserts rows into a stream's target
// table, split around its VALUES rows. append_dedup updates the stored row of
// a key with the incoming one unless the stored cursor is higher; rows run in
// arrival order, so among equal cursors the last arrival wins. The cursor
// column is assigned last, since every assignment sees the ones before it.
func insertSQL(p *streamPlan) (head, tail string) {
	head = "INSERT INTO " + quote(p.target()) + " (" + identList(p.allColumns()) + ") VALUES "
	if p.mode != synclet.DestinationSyncModeAppendDedup {
		return head, ""
	}
	stored := func(c column) string { return quote(p.target()) + "." + quote(c.ident) }
	incoming := func(c column) string { return quote(rowAlias) + "." + quote(c.ident) }
	var cond string
	if p.cursor >= 0 {
		cur := p.columns[p.cursor]
		cond = stored(cur) + " IS NULL OR " + incoming(cur) + " >= " + stored(cur)
	}
	var set []string
	assign := func(c column) {
		val := incoming(c)
		if cond != "" {
			val = "IF(" + cond + ", " + val + ", " + stored(c) + ")"
		}
		set = append(set, quote(c.ident)+" = "+val)
	}
	for _, c := range p.allColumns() {
		isKey := slices.ContainsFunc(p.pk, func(i int) bool { return p.columns[i].ident == c.ident })
		isCursor := p.cursor >= 0 && p.columns[p.cursor].ident == c.ident
		if !isKey && !isCursor {
			assign(c)
		}
	}
	if p.cursor >= 0 && !slices.Contains(p.pk, p.cursor) {
		assign(p.columns[p.cursor])
	}
	return head, " AS " + quote(rowAlias) + " ON DUPLICATE KEY UPDATE " + strings.Join(set, ", ")
}

var rowWhere = regexp.MustCompile(`(?:column '((?:[^']|'')*)' )?at row (\d+)`)

// insertError names the stream, the record and the column of a value MySQL
// rejected; MySQL numbers the rows of one statement from 1.
func (w *writer) insertError(p *streamPlan, records []int, err error) error {
	var myErr *mysql.MySQLError
	if !errors.As(err, &myErr) {
		return fmt.Errorf("stream %q: insert batch into %s: %w", p.desc.Name, quote(p.target()), err)
	}
	m := rowWhere.FindStringSubmatch(myErr.Message)
	if m == nil {
		return fmt.Errorf("stream %q: insert batch into %s: %s (MySQL error %d)", p.desc.Name, quote(p.target()), myErr.Message, myErr.Number)
	}
	row, _ := strconv.Atoi(m[2])
	pos := "a record"
	if row >= 1 && row <= len(records) {
		pos = "record " + strconv.Itoa(records[row-1])
	}
	if m[1] == "" {
		return fmt.Errorf("stream %q: %s: %s (MySQL error %d)", p.desc.Name, pos, myErr.Message, myErr.Number)
	}
	name := strings.ReplaceAll(m[1], "''", "'")
	for ident, orig := range p.byIdent {
		if strings.EqualFold(ident, name) {
			name = orig
			break
		}
	}
	return fmt.Errorf("stream %q: %s: column %q: %s (MySQL error %d)", p.desc.Name, pos, name, myErr.Message, myErr.Number)
}

// finish commits the remaining rows, replaces the tables of overwrite
// streams and echoes the held states.
func (w *writer) finish(ctx context.Context) error {
	if err := w.flush(ctx); err != nil {
		return err
	}
	if len(w.overwrite) > 0 {
		if err := swapTables(ctx, w.conn, w.plans); err != nil {
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
