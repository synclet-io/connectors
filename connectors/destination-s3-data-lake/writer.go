package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"iter"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/google/uuid"
	synclet "github.com/synclet-io/synclet-go-sdk"
)

// buffer holds the records of one stream not yet written to data files.
// Append and overwrite streams append to builder as records arrive;
// append_dedup streams keep one row per primary key in rows, which is
// appended to a builder at flush. maxCursor holds, for an append_dedup
// stream with a cursor, the highest cursor accepted per primary key in this
// sync, across flushes.
type buffer struct {
	builder *array.RecordBuilder
	n       int
	rows    map[string][]any
	order   []string

	maxCursor map[string]any
	stale     int
}

// writer buffers records of every stream and writes them in batches. A
// STATE is echoed once every record received before it is committed; the
// states of an overwrite stream, and GLOBAL and LEGACY states while the
// catalog has an overwrite stream, are held until the overwrite snapshots
// are committed.
type writer struct {
	em     *synclet.Emitter
	limits Destination

	plans   []*streamPlan
	tables  []*streamTable
	byDesc  map[synclet.StreamDescriptor]*streamPlan
	buffers []buffer
	// pending are the data files of overwrite streams, written but not
	// committed.
	pending  [][]iceberg.DataFile
	received []int
	unknown  []map[string]struct{}
	keyLimit int
	rows     int
	size     int

	queued     []synclet.StateMessage
	held       []synclet.StateMessage
	overwrite  map[synclet.StreamDescriptor]bool
	lastCommit time.Time
}

func newWriter(em *synclet.Emitter, limits Destination, plans []*streamPlan, keyLimit int) *writer {
	w := &writer{
		keyLimit:   keyLimit,
		em:         em,
		limits:     limits,
		plans:      plans,
		tables:     make([]*streamTable, len(plans)),
		byDesc:     map[synclet.StreamDescriptor]*streamPlan{},
		buffers:    make([]buffer, len(plans)),
		pending:    make([][]iceberg.DataFile, len(plans)),
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
func (w *writer) prepare(ctx context.Context, l *lake) error {
	for _, p := range w.plans {
		t, err := prepareTable(ctx, l, p)
		if err != nil {
			return fmt.Errorf("stream %q: %w", p.desc.Name, err)
		}
		w.tables[p.index] = t
		w.resetBuffer(p.index)
		if p.mode == synclet.DestinationSyncModeAppendDedup && t.cursorField >= 0 {
			w.buffers[p.index].maxCursor = map[string]any{}
		}
	}
	return nil
}

func (w *writer) resetBuffer(i int) {
	b := &w.buffers[i]
	if b.builder != nil {
		b.builder.Release()
	}
	b.builder = array.NewRecordBuilder(memory.DefaultAllocator, w.tables[i].schema)
	b.n = 0
	if w.plans[i].mode == synclet.DestinationSyncModeAppendDedup {
		b.rows = map[string][]any{}
		b.order = nil
	}
}

func (w *writer) release() {
	for i := range w.buffers {
		if w.buffers[i].builder != nil {
			w.buffers[i].builder.Release()
			w.buffers[i].builder = nil
		}
	}
}

// addRecord validates one record, converts it into its stream's buffer and
// flushes when the buffers reach a limit.
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
		if idx, ok := p.byName[name]; !ok || idx < len(metaColumns()) {
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
	st := w.tables[p.index]
	values := make([]any, len(st.sources))
	for i, src := range st.sources {
		if src < 0 {
			continue
		}
		c := p.columns[src]
		switch c.kind {
		case kindRawID:
			values[i] = uuid.NewString()
			continue
		case kindExtractedAt:
			values[i] = arrow.Timestamp(extracted.UnixMicro())
			continue
		}
		raw, ok := fields[c.name]
		if !ok || string(bytes.TrimSpace(raw)) == "null" {
			continue
		}
		v, err := convert(c, bytes.TrimSpace(raw))
		if err != nil {
			return fail("column %q: value %s %v", c.name, truncate(raw), err)
		}
		values[i] = v
	}

	b := &w.buffers[p.index]
	if p.mode == synclet.DestinationSyncModeAppendDedup {
		key, err := w.dedupKey(st, values)
		if err != nil {
			return fail("%v", err)
		}
		if st.cursorField >= 0 {
			cur := values[st.cursorField]
			prev, seen := b.maxCursor[key]
			if seen && laterCursor(prev, cur) {
				b.stale++
				return nil
			}
			if !seen && len(b.maxCursor) >= w.keyLimit {
				return fail("more than %d distinct primary keys in one sync; raise dedup_key_limit", w.keyLimit)
			}
			b.maxCursor[key] = cur
		}
		if _, ok := b.rows[key]; !ok {
			b.order = append(b.order, key)
		}
		b.rows[key] = values
	} else if err := appendRow(b.builder, values); err != nil {
		return fail("%v", err)
	}
	b.n++
	w.rows++
	w.size += len(r.Data)
	return w.maybeFlush(ctx)
}

func (w *writer) maybeFlush(ctx context.Context) error {
	if w.rows >= w.limits.maxRows || w.size >= w.limits.maxBytes {
		return w.flush(ctx)
	}
	return nil
}

// dedupKey identifies a record by its primary key values, each written as
// its length and its typed text, so no two keys share an encoding; a null
// key fails.
func (w *writer) dedupKey(st *streamTable, values []any) (string, error) {
	var key strings.Builder
	for _, f := range st.pkFields {
		if values[f] == nil {
			return "", fmt.Errorf("primary key column %q is null", st.schema.Field(f).Name)
		}
		v := fmt.Sprintf("%T:%v", values[f], values[f])
		key.WriteString(strconv.Itoa(len(v)))
		key.WriteByte(':')
		key.WriteString(v)
	}
	return key.String(), nil
}

// laterCursor reports whether a stored row's cursor is higher than a new
// record's, so the stored row stays; a null cursor is the lowest, and the
// later record wins among equal cursors.
func laterCursor(stored, incoming any) bool {
	switch {
	case incoming == nil:
		return stored != nil
	case stored == nil:
		return false
	}
	return compareValues(stored, incoming) > 0
}

func appendRow(b *array.RecordBuilder, values []any) error {
	for i, v := range values {
		if err := appendValue(b.Field(i), v); err != nil {
			return err
		}
	}
	return nil
}

// addState echoes st when no record is pending; otherwise it queues st and
// flushes when the last commit is older than the age limit.
func (w *writer) addState(ctx context.Context, st synclet.StateMessage) error {
	if w.rows == 0 {
		return w.releaseStates([]synclet.StateMessage{st})
	}
	w.queued = append(w.queued, st)
	if time.Since(w.lastCommit) >= w.limits.maxAge {
		return w.flush(ctx)
	}
	return nil
}

// releaseStates echoes states whose records are committed in their final
// snapshot and holds the rest until the overwrite commits.
func (w *writer) releaseStates(states []synclet.StateMessage) error {
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

// flush writes every buffered record to data files, commits one snapshot
// per append and append_dedup stream, keeps the files of overwrite streams
// for the final commit, then echoes the queued states.
func (w *writer) flush(ctx context.Context) error {
	if w.rows > 0 {
		for _, p := range w.plans {
			if w.buffers[p.index].n == 0 {
				continue
			}
			if err := w.flushStream(ctx, p); err != nil {
				return err
			}
			w.resetBuffer(p.index)
		}
		w.rows, w.size = 0, 0
		w.lastCommit = time.Now()
	}
	queued := w.queued
	w.queued = nil
	return w.releaseStates(queued)
}

func (w *writer) flushStream(ctx context.Context, p *streamPlan) error {
	st := w.tables[p.index]
	b := &w.buffers[p.index]
	if p.mode == synclet.DestinationSyncModeAppendDedup {
		for _, key := range b.order {
			if err := appendRow(b.builder, b.rows[key]); err != nil {
				return fmt.Errorf("stream %q: %w", p.desc.Name, err)
			}
		}
	}
	rec := b.builder.NewRecordBatch()
	defer rec.Release()

	var deletes []iceberg.DataFile
	if p.mode == synclet.DestinationSyncModeAppendDedup {
		var err error
		deletes, err = writeEqualityDeletes(ctx, st, rec)
		if err != nil {
			return fmt.Errorf("stream %q: write equality deletes to %s: %w", p.desc.Name, st.name(), err)
		}
	}
	files, err := writeDataFiles(ctx, st, rec)
	if err != nil {
		st.removeFiles(ctx, deletes)
		return fmt.Errorf("stream %q: write data files to %s: %w", p.desc.Name, st.name(), err)
	}

	switch p.mode {
	case synclet.DestinationSyncModeOverwrite:
		w.pending[p.index] = append(w.pending[p.index], files...)
		return nil
	case synclet.DestinationSyncModeAppendDedup:
		err = st.commit(ctx, "commit", func(ctx context.Context, tx *table.Transaction, _ *table.Table) error {
			return tx.NewRowDelta(nil).AddRows(files...).AddDeletes(deletes...).Commit(ctx)
		})
	default:
		err = st.commit(ctx, "commit", func(ctx context.Context, tx *table.Transaction, _ *table.Table) error {
			return tx.AddDataFiles(ctx, files, nil)
		})
	}
	if err != nil && !isUnknown(err) {
		st.removeFiles(ctx, append(files, deletes...))
	}
	return err
}

// writeDataFiles writes a batch to Parquet data files of the table without
// committing them.
func writeDataFiles(ctx context.Context, st *streamTable, rec arrow.RecordBatch) ([]iceberg.DataFile, error) {
	rec.Retain()
	var files []iceberg.DataFile
	for f, err := range table.WriteRecords(ctx, st.tbl, st.schema, single(rec)) {
		if err != nil {
			st.removeFiles(ctx, files)
			return nil, err
		}
		files = append(files, f)
	}
	return files, nil
}

// writeEqualityDeletes writes the primary keys of a batch to equality delete
// files without committing them; committed with the batch's data files, they
// delete the rows of earlier snapshots with those keys.
func writeEqualityDeletes(ctx context.Context, st *streamTable, rec arrow.RecordBatch) ([]iceberg.DataFile, error) {
	fields := make([]arrow.Field, len(st.pkFields))
	cols := make([]arrow.Array, len(st.pkFields))
	for i, f := range st.pkFields {
		fields[i] = st.schema.Field(f)
		cols[i] = rec.Column(f)
	}
	keys := array.NewRecordBatch(arrow.NewSchema(fields, nil), cols, rec.NumRows())
	defer keys.Release()
	keys.Retain()
	return st.tbl.NewTransaction().WriteEqualityDeletes(ctx, st.pkIDs, single(keys))
}

func single(rec arrow.RecordBatch) iter.Seq2[arrow.RecordBatch, error] {
	return func(yield func(arrow.RecordBatch, error) bool) {
		yield(rec, nil)
	}
}

// commitOverwrites replaces the snapshot of every overwrite stream with one
// holding only the files this sync wrote.
func (w *writer) commitOverwrites(ctx context.Context) error {
	for _, p := range w.plans {
		if p.mode != synclet.DestinationSyncModeOverwrite {
			continue
		}
		st := w.tables[p.index]
		files := w.pending[p.index]
		err := st.commit(ctx, "overwrite", func(ctx context.Context, tx *table.Transaction, tbl *table.Table) error {
			old, err := currentDataFiles(ctx, tbl)
			if err != nil {
				return err
			}
			switch {
			case len(old) == 0 && len(files) == 0:
				return nil
			case len(old) == 0:
				return tx.AddDataFiles(ctx, files, nil)
			case len(files) == 0:
				return tx.Delete(ctx, iceberg.AlwaysTrue{}, nil)
			}
			return tx.ReplaceDataFilesWithDataFiles(ctx, old, files, nil)
		})
		if err != nil {
			if isUnknown(err) {
				w.pending[p.index] = nil
			}
			return err
		}
		w.pending[p.index] = nil
	}
	return nil
}

// currentDataFiles lists the data files of a table's current snapshot.
func currentDataFiles(ctx context.Context, tbl *table.Table) ([]iceberg.DataFile, error) {
	if tbl.CurrentSnapshot() == nil {
		return nil, nil
	}
	tasks, err := tbl.Scan().PlanFiles(ctx)
	if err != nil {
		return nil, fmt.Errorf("list data files: %w", err)
	}
	seen := map[string]bool{}
	var files []iceberg.DataFile
	for _, t := range tasks {
		if seen[t.File.FilePath()] {
			continue
		}
		seen[t.File.FilePath()] = true
		files = append(files, t.File)
	}
	return files, nil
}

// discardPending removes the uncommitted files of overwrite streams.
func (w *writer) discardPending(ctx context.Context) {
	for _, p := range w.plans {
		if t := w.tables[p.index]; t != nil {
			t.removeFiles(ctx, w.pending[p.index])
			w.pending[p.index] = nil
		}
	}
}

// finish writes the remaining records, commits the overwrite streams and
// echoes the held states.
func (w *writer) finish(ctx context.Context) error {
	if err := w.flush(ctx); err != nil {
		return err
	}
	if err := w.commitOverwrites(ctx); err != nil {
		return err
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

// warnStale logs, once per stream, how many records were skipped because
// their cursor is lower than one already accepted for the same primary key.
func (w *writer) warnStale(ctx context.Context) {
	for _, p := range w.plans {
		if n := w.buffers[p.index].stale; n > 0 {
			synclet.Logger(ctx).Warn(fmt.Sprintf("stream %q: skipped %d records whose cursor is lower than one already written for the same primary key in this sync", p.desc.Name, n))
		}
	}
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

func truncate(raw json.RawMessage) string {
	const limit = 80
	s := string(bytes.TrimSpace(raw))
	if len(s) > limit {
		return s[:limit] + "..."
	}
	return s
}
