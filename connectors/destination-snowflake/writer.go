package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	synclet "github.com/synclet-io/synclet-go-sdk"
)

// batch holds the rows of one stream not yet committed, as CSV, and the
// position of each row's record in the stream.
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
	c      *conn
	em     *synclet.Emitter
	limits Destination
	dir    string

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

func newWriter(c *conn, em *synclet.Emitter, limits Destination, plans []*streamPlan, dir string) *writer {
	w := &writer{
		c:          c,
		em:         em,
		limits:     limits,
		dir:        dir,
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

// prepare creates the schema and makes every stream's tables ready.
func (w *writer) prepare(ctx context.Context) error {
	if err := w.c.ensureSchema(ctx); err != nil {
		return err
	}
	for _, p := range w.plans {
		if err := w.c.prepareTable(ctx, p); err != nil {
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
	vals := make([]string, 0, len(p.columns))
	nulls := make([]bool, 0, len(p.columns))
	for _, c := range p.columns {
		val, null, err := encodeValue(c.typ, fields[c.name])
		if err != nil {
			return fail("column %q: %v", c.name, err)
		}
		vals = append(vals, val)
		nulls = append(nulls, null)
	}

	w.seq++
	b := &w.batches[p.index]
	csvField(&b.rows, strconv.FormatInt(w.seq, 10))
	b.rows.WriteByte(',')
	csvField(&b.rows, uuid.NewString())
	b.rows.WriteByte(',')
	csvField(&b.rows, extracted.UTC().Format("2006-01-02 15:04:05.000000000")+" +00:00")
	for i, v := range vals {
		b.rows.WriteByte(',')
		if !nulls[i] {
			csvField(&b.rows, v)
		}
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

// csvField writes s quoted, with its quotes doubled. COPY reads an empty
// unquoted field as NULL, so every value is quoted.
func csvField(buf *bytes.Buffer, s string) {
	buf.WriteByte('"')
	buf.WriteString(strings.ReplaceAll(s, `"`, `""`))
	buf.WriteByte('"')
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

// isHeld reports whether st may cover an overwrite stream. A STREAM state
// belongs to the catalog stream of its exact descriptor; failing that, to
// every catalog stream of its name, since sources and platforms disagree on
// namespaces. A state that matches no catalog stream, like a GLOBAL or LEGACY
// one, is held while the catalog has an overwrite stream.
func (w *writer) isHeld(st synclet.StateMessage) bool {
	if st.Type != synclet.StateTypeStream || st.Stream == nil {
		return len(w.overwrite) > 0
	}
	desc := st.Stream.StreamDescriptor
	if _, ok := w.byDesc[desc]; ok {
		return w.overwrite[desc]
	}
	named := false
	for d := range w.byDesc {
		if d.Name != desc.Name {
			continue
		}
		if w.overwrite[d] {
			return true
		}
		named = true
	}
	return !named && len(w.overwrite) > 0
}

// flush stages every pending batch, moves the staged rows into the target
// tables in one transaction, then echoes the queued states.
func (w *writer) flush(ctx context.Context) error {
	if w.rows > 0 {
		for _, p := range w.plans {
			if len(w.batches[p.index].records) == 0 {
				continue
			}
			if err := w.stage(ctx, p); err != nil {
				return fmt.Errorf("stream %q: %w", p.desc.Name, err)
			}
		}
		if err := w.commit(ctx); err != nil {
			return err
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

// stage writes one stream's batch to a gzip file, PUTs it to the target's
// table stage and copies it into the staging table.
func (w *writer) stage(ctx context.Context, p *streamPlan) error {
	b := &w.batches[p.index]
	name := "synclet_" + strings.ReplaceAll(uuid.NewString(), "-", "") + ".csv.gz"
	path := filepath.Join(w.dir, name)
	if err := writeGzip(path, b.rows.Bytes()); err != nil {
		return fmt.Errorf("write batch file: %w", err)
	}
	defer func() { _ = os.Remove(path) }()

	tableStage := "@" + w.c.schemaRef() + ".%" + ident(p.target())
	put := "PUT 'file://" + filepath.ToSlash(path) + "' " + tableStage + " SOURCE_COMPRESSION = GZIP AUTO_COMPRESS = FALSE"
	if err := w.c.put(ctx, put); err != nil {
		return fmt.Errorf("put batch to %s: %w", tableStage, err)
	}
	loaded, err := w.c.copyInto(ctx, copySQL(w.c.tableRef(stageTable(p.index)), tableStage, name))
	if err != nil {
		return fmt.Errorf("copy batch into %s: %w", w.c.tableRef(stageTable(p.index)), err)
	}
	if loaded != int64(len(b.records)) {
		return fmt.Errorf("COPY INTO loaded %d of %d rows of the batch", loaded, len(b.records))
	}
	return nil
}

func writeGzip(path string, data []byte) (rerr error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	defer func() {
		if err := f.Close(); err != nil && rerr == nil {
			rerr = err
		}
	}()
	zw := gzip.NewWriter(f)
	if _, err := zw.Write(data); err != nil {
		return err
	}
	return zw.Close()
}

// commit inserts or merges every staged batch into its target table and
// empties the staging tables, in one transaction.
func (w *writer) commit(ctx context.Context) error {
	tx, err := w.c.c.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin batch: %w", w.c.sessionError(err))
	}
	defer func() { _ = tx.Rollback() }()
	for _, p := range w.plans {
		if len(w.batches[p.index].records) == 0 {
			continue
		}
		target := w.c.tableRef(p.target())
		stage := w.c.tableRef(stageTable(p.index))
		q := insertSQL(target, p, stage)
		if p.mode == synclet.DestinationSyncModeAppendDedup {
			q = mergeSQL(target, p, stage)
		}
		if _, err := tx.ExecContext(ctx, q); err != nil {
			return fmt.Errorf("stream %q: insert batch into %s: %w", p.desc.Name, target, w.c.sessionError(err))
		}
		if _, err := tx.ExecContext(ctx, "DELETE FROM "+stage); err != nil {
			return fmt.Errorf("stream %q: empty %s: %w", p.desc.Name, stage, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit batch: %w", err)
	}
	return nil
}

// finish commits the remaining rows, replaces the tables of overwrite
// streams and echoes the held states.
func (w *writer) finish(ctx context.Context) error {
	if err := w.flush(ctx); err != nil {
		return err
	}
	if len(w.overwrite) > 0 {
		if err := w.c.swapTables(ctx, w.plans); err != nil {
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

// put runs a PUT and checks that the file was uploaded.
func (c *conn) put(ctx context.Context, q string) error {
	rows, err := c.c.QueryContext(ctx, q)
	if err != nil {
		return c.sessionError(err)
	}
	results, err := resultRows(rows)
	if err != nil {
		return err
	}
	if len(results) == 0 {
		return errors.New("PUT reported no file")
	}
	for _, r := range results {
		if r["status"] != "UPLOADED" {
			return fmt.Errorf("PUT of %s: status %s %s", r["source"], r["status"], r["message"])
		}
	}
	return nil
}

// copyInto runs a COPY INTO and returns the rows it loaded. A file that is
// not LOADED is an error.
func (c *conn) copyInto(ctx context.Context, q string) (int64, error) {
	rows, err := c.c.QueryContext(ctx, q)
	if err != nil {
		return 0, c.sessionError(err)
	}
	results, err := resultRows(rows)
	if err != nil {
		return 0, err
	}
	var total int64
	for _, r := range results {
		if r["status"] != "LOADED" {
			return 0, fmt.Errorf("file %s: status %q: %s", r["file"], r["status"], r["first_error"])
		}
		n, err := strconv.ParseInt(r["rows_loaded"], 10, 64)
		if err != nil {
			return 0, fmt.Errorf("file %s: rows_loaded %q: %w", r["file"], r["rows_loaded"], err)
		}
		total += n
	}
	return total, nil
}

// resultRows reads every row of rows as a map from lower-case column name
// to text; NULL is the empty string.
func resultRows(rows *sql.Rows) (_ []map[string]string, rerr error) {
	defer func() {
		if err := rows.Close(); err != nil && rerr == nil {
			rerr = err
		}
	}()
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	var out []map[string]string
	for rows.Next() {
		cells := make([]sql.NullString, len(cols))
		ptrs := make([]any, len(cols))
		for i := range cells {
			ptrs[i] = &cells[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		r := make(map[string]string, len(cols))
		for i, c := range cols {
			r[strings.ToLower(c)] = cells[i].String
		}
		out = append(out, r)
	}
	return out, rows.Err()
}
