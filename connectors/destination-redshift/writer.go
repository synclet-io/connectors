package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	synclet "github.com/synclet-io/synclet-go-sdk"
)

const (
	// nullMarker is the NULL AS string of COPY. Every other field is
	// double-quoted; a value equal to the marker fails its record.
	nullMarker = "__synclet_null__"
	// maxRowBytes is the largest row COPY loads.
	maxRowBytes = 4 << 20
)

// copyOptions are the format options of every batch COPY. Without
// TRUNCATECOLUMNS, ACCEPTINVCHARS or ROUNDEC, COPY rejects a value instead of
// altering it; the writer only stages values the column holds exactly.
const copyOptions = "FORMAT AS CSV GZIP NULL AS " + "'" + nullMarker + "'" + " DATEFORMAT 'YYYY-MM-DD' TIMEFORMAT 'auto'"

// batch holds the rows of one stream not yet committed, as gzipped CSV, and
// the position of each row's record in the stream.
type batch struct {
	data    bytes.Buffer
	gz      *gzip.Writer
	records []int
}

// change is one entry of _airbyte_meta.changes.
type change struct {
	Field  string `json:"field"`
	Change string `json:"change"`
	Reason string `json:"reason"`
}

// writer buffers records of every stream and commits them in batches. A
// STATE is echoed once every record received before it is committed; the
// states of an overwrite stream, and GLOBAL and LEGACY states while the
// catalog has an overwrite stream, are held until the shadow tables replace
// the old ones.
type writer struct {
	w      warehouse
	st     *staging
	schema string
	em     *synclet.Emitter
	limits Destination
	syncID string
	purge  bool

	plans  []*streamPlan
	byDesc map[synclet.StreamDescriptor]*streamPlan

	batches   []batch
	received  []int
	unknown   []map[string]struct{}
	truncated []map[string]int
	rows      int
	size      int
	seq       int64
	flushes   int
	inTx      bool

	queued     []synclet.StateMessage
	held       []synclet.StateMessage
	overwrite  map[synclet.StreamDescriptor]bool
	lastCommit time.Time
}

func newWriter(w warehouse, st *staging, cfg Config, em *synclet.Emitter, limits Destination, syncID string, plans []*streamPlan) *writer {
	wr := &writer{
		w:          w,
		st:         st,
		schema:     cfg.Schema,
		em:         em,
		limits:     limits,
		syncID:     syncID,
		purge:      cfg.UploadingMethod.purge(),
		plans:      plans,
		byDesc:     map[synclet.StreamDescriptor]*streamPlan{},
		batches:    make([]batch, len(plans)),
		received:   make([]int, len(plans)),
		unknown:    make([]map[string]struct{}, len(plans)),
		truncated:  make([]map[string]int, len(plans)),
		overwrite:  map[synclet.StreamDescriptor]bool{},
		lastCommit: time.Now(),
	}
	for _, p := range plans {
		wr.byDesc[p.desc] = p
		if p.mode == synclet.DestinationSyncModeOverwrite {
			wr.overwrite[p.desc] = true
		}
	}
	return wr
}

// prepare creates the schema and makes every stream's table ready.
func (w *writer) prepare(ctx context.Context) error {
	if err := w.w.exec(ctx, "CREATE SCHEMA IF NOT EXISTS "+quote(w.schema)); err != nil {
		return fmt.Errorf("create schema %q: %w", w.schema, err)
	}
	for _, p := range w.plans {
		if err := prepareTable(ctx, w.w, w.schema, p); err != nil {
			return fmt.Errorf("stream %q: %w", p.desc.Name, err)
		}
	}
	return nil
}

// addRecord validates one record, renders it as a CSV row of its stream's
// batch and flushes when the buffer reaches a limit.
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
	values := make([]string, len(p.columns))
	nulls := make([]bool, len(p.columns))
	changes := []change{}
	for i, c := range p.columns {
		val, null, truncated, err := encodeValue(c.typ, fields[c.name])
		if err != nil {
			return fail("column %q: %v", c.name, err)
		}
		if !null && val == nullMarker {
			return fail("column %q: value equals the staging NULL marker %s", c.name, nullMarker)
		}
		if truncated {
			changes = append(changes, change{Field: c.name, Change: "TRUNCATED", Reason: "DESTINATION_FIELD_SIZE_LIMITATION"})
		}
		values[i], nulls[i] = val, null
	}
	meta, err := json.Marshal(map[string][]change{"changes": changes})
	if err != nil {
		return fail("encode _airbyte_meta: %v", err)
	}

	var row bytes.Buffer
	seq := w.seq + 1
	csvField(&row, strconv.FormatInt(seq, 10), false)
	row.WriteByte(',')
	csvField(&row, uuid.NewString(), false)
	row.WriteByte(',')
	csvField(&row, formatTimestamptz(extracted), false)
	row.WriteByte(',')
	csvField(&row, string(meta), false)
	for i := range p.columns {
		row.WriteByte(',')
		csvField(&row, values[i], nulls[i])
	}
	row.WriteByte('\n')
	if row.Len() > maxRowBytes {
		return fail("row is larger than 4 MiB (%d bytes), the most Redshift COPY loads", row.Len())
	}

	b := &w.batches[p.index]
	if b.gz == nil {
		b.gz = gzip.NewWriter(&b.data)
	}
	if _, err := b.gz.Write(row.Bytes()); err != nil {
		return fail("buffer row: %v", err)
	}
	w.seq = seq
	for _, ch := range changes {
		if w.truncated[p.index] == nil {
			w.truncated[p.index] = map[string]int{}
		}
		w.truncated[p.index][ch.Field]++
	}
	b.records = append(b.records, pos)
	w.rows++
	w.size += len(r.Data)

	if w.rows >= w.limits.maxRows || w.size >= w.limits.maxBytes {
		return w.flush(ctx)
	}
	return nil
}

// csvField appends a CSV field: the NULL marker bare, any other value
// double-quoted with its quotes doubled.
func csvField(b *bytes.Buffer, val string, null bool) {
	if null {
		b.WriteString(nullMarker)
		return
	}
	b.WriteByte('"')
	b.WriteString(strings.ReplaceAll(val, `"`, `""`))
	b.WriteByte('"')
}

func formatTimestamptz(t time.Time) string {
	t = t.UTC().Truncate(time.Microsecond)
	return t.Format("2006-01-02 15:04:05.999999") + "+00"
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

// staged is one uploaded batch file.
type staged struct {
	plan *streamPlan
	key  string
}

// flush uploads every pending batch, loads them in one transaction, then
// echoes the queued states. A failed flush rolls back, deletes the uploaded
// files and echoes nothing.
func (w *writer) flush(ctx context.Context) (rerr error) {
	if w.rows > 0 {
		w.flushes++
		var files []staged
		defer func() {
			if rerr != nil || w.purge {
				w.deleteStaged(ctx, files, rerr == nil)
			}
		}()
		for _, p := range w.plans {
			b := &w.batches[p.index]
			if len(b.records) == 0 {
				continue
			}
			if err := b.gz.Close(); err != nil {
				return fmt.Errorf("stream %q: compress batch: %w", p.desc.Name, err)
			}
			key := w.st.key(w.syncID + "/" + p.table + "/" + strconv.Itoa(w.flushes) + ".csv.gz")
			if err := w.st.put(ctx, key, b.data.Bytes()); err != nil {
				return fmt.Errorf("stream %q: stage batch: %w", p.desc.Name, err)
			}
			files = append(files, staged{plan: p, key: key})
		}

		if err := w.w.exec(ctx, "BEGIN"); err != nil {
			return fmt.Errorf("begin batch: %w", err)
		}
		w.inTx = true
		defer func() {
			if w.inTx {
				w.inTx = false
				_ = w.w.exec(context.WithoutCancel(ctx), "ROLLBACK")
			}
		}()
		for _, f := range files {
			if err := w.load(ctx, f); err != nil {
				return err
			}
		}
		if err := w.w.exec(ctx, "COMMIT"); err != nil {
			return fmt.Errorf("commit batch: %w", err)
		}
		w.inTx = false
		for i := range w.batches {
			w.batches[i] = batch{}
		}
		w.rows, w.size = 0, 0
		w.lastCommit = time.Now()
	}
	queued := w.queued
	w.queued = nil
	return w.release(queued)
}

// deleteStaged removes uploaded batch files. After a commit a failure is a
// warning: the rows are loaded and the files are only leftovers.
func (w *writer) deleteStaged(ctx context.Context, files []staged, committed bool) {
	if len(files) == 0 {
		return
	}
	keys := make([]string, len(files))
	for i, f := range files {
		keys[i] = f.key
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cleanupTimeout)
	defer cancel()
	if err := w.st.delete(ctx, keys); err != nil {
		what := "staged files of a failed batch"
		if committed {
			what = "loaded staged files"
		}
		synclet.Logger(ctx).Warn(fmt.Sprintf("delete %s: %v", what, err))
	}
}

// load copies one stream's batch file into a temporary table and inserts
// it into the stream's target table, merging on the primary key for
// append_dedup.
func (w *writer) load(ctx context.Context, f staged) error {
	p := f.plan
	stage := quote(fmt.Sprintf("_synclet_stage_%d", p.index))
	cols := p.allColumns()
	defs := append([]string{quote(colSeq) + " bigint"}, columnDefs(cols)...)
	if err := w.w.exec(ctx, "DROP TABLE IF EXISTS "+stage); err != nil {
		return fmt.Errorf("stream %q: drop staging table: %w", p.desc.Name, err)
	}
	if err := w.w.exec(ctx, "CREATE TEMP TABLE "+stage+" ("+strings.Join(defs, ", ")+")"); err != nil {
		return fmt.Errorf("stream %q: create staging table: %w", p.desc.Name, err)
	}
	auth, err := w.st.copyAuth(ctx)
	if err != nil {
		return fmt.Errorf("stream %q: %w", p.desc.Name, err)
	}
	copySQL := "COPY " + stage + " (" + quote(colSeq) + ", " + identList(cols) + ") FROM " + literal(w.st.url(f.key)) +
		" " + auth + " REGION " + literal(w.st.region) + " " + copyOptions
	if err := w.w.exec(ctx, copySQL); err != nil {
		return w.copyError(ctx, p, f.key, err)
	}
	if p.mode == synclet.DestinationSyncModeAppendDedup {
		for _, sql := range dedupSQL(w.schema, p, stage) {
			if err := w.w.exec(ctx, sql); err != nil {
				return fmt.Errorf("stream %q: merge batch into %s: %w", p.desc.Name, quote(w.schema, p.table), err)
			}
		}
		return nil
	}
	target := quote(w.schema, p.target())
	if err := w.w.exec(ctx, "INSERT INTO "+target+" ("+identList(cols)+") SELECT "+identList(cols)+" FROM "+stage); err != nil {
		return fmt.Errorf("stream %q: insert batch into %s: %w", p.desc.Name, target, err)
	}
	return nil
}

// copyError names the stream, the record and the column of a value COPY
// rejected, from sys_load_error_detail; the failed transaction is rolled
// back first so the session can query it. Without a detail row the COPY
// error is returned as it is.
func (w *writer) copyError(ctx context.Context, p *streamPlan, key string, err error) error {
	msg := err.Error()
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		msg = pgErr.Message
	}
	base := fmt.Errorf("stream %q: load batch: %s", p.desc.Name, msg)
	w.inTx = false
	if rerr := w.w.exec(context.WithoutCancel(ctx), "ROLLBACK"); rerr != nil {
		return base
	}
	rows, qerr := w.w.query(ctx, "SELECT line_number, TRIM(column_name), TRIM(error_message) FROM sys_load_error_detail WHERE file_name LIKE "+
		literal("%"+key)+" ORDER BY start_time DESC LIMIT 1")
	if qerr != nil || len(rows) != 1 || len(rows[0]) != 3 {
		return base
	}
	line, _ := strconv.Atoi(fmt.Sprint(rows[0][0]))
	col, detail := fmt.Sprint(rows[0][1]), fmt.Sprint(rows[0][2])
	b := &w.batches[p.index]
	where := "a record"
	if line >= 1 && line <= len(b.records) {
		where = "record " + strconv.Itoa(b.records[line-1])
	}
	if orig, ok := p.byIdent[col]; ok {
		col = orig
	}
	if col == "" {
		return fmt.Errorf("stream %q: %s: %s", p.desc.Name, where, detail)
	}
	return fmt.Errorf("stream %q: %s: column %q: %s", p.desc.Name, where, col, detail)
}

// finish commits the remaining rows, replaces the tables of overwrite
// streams and echoes the held states.
func (w *writer) finish(ctx context.Context) error {
	if err := w.flush(ctx); err != nil {
		return err
	}
	if len(w.overwrite) > 0 {
		if err := swapTables(ctx, w.w, w.schema, w.plans); err != nil {
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

// warn logs, once per stream, the record fields the stream's schema does
// not have, which are not written, and the strings truncated to fit
// VARCHAR.
func (w *writer) warn(ctx context.Context) {
	log := synclet.Logger(ctx)
	for _, p := range w.plans {
		if len(w.unknown[p.index]) > 0 {
			names := make([]string, 0, len(w.unknown[p.index]))
			for n := range w.unknown[p.index] {
				names = append(names, n)
			}
			slices.Sort(names)
			log.Warn(fmt.Sprintf("stream %q: fields not in the catalog schema are not written: %s", p.desc.Name, strings.Join(names, ", ")))
		}
		if len(w.truncated[p.index]) > 0 {
			total := 0
			names := make([]string, 0, len(w.truncated[p.index]))
			for n, c := range w.truncated[p.index] {
				names = append(names, n)
				total += c
			}
			slices.Sort(names)
			noun := "values"
			if total == 1 {
				noun = "value"
			}
			log.Warn(fmt.Sprintf("stream %q: %d %s longer than %d bytes truncated (recorded in _airbyte_meta): %s",
				p.desc.Name, total, noun, maxVarcharBytes, strings.Join(names, ", ")))
		}
	}
}
