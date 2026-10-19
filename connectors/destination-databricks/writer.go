package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	synclet "github.com/synclet-io/synclet-go-sdk"
)

// batch holds the rows of one stream not yet committed, as lines of a staged
// file, and the position of each row's record in the stream.
type batch struct {
	rows    bytes.Buffer
	records []int
}

// writer buffers records of every stream and commits them in batches. A
// batch is staged as one file per stream in the volume and copied into the
// stage tables; only when every stream's copy succeeded is each stage table
// appended or merged into its table, so a value the warehouse rejects
// commits nothing. A STREAM state is echoed once its stream's records
// received before it are committed, any other STATE once every record
// received before it is; the states of an overwrite stream, and GLOBAL and LEGACY states
// while the catalog has an overwrite stream, are held until the shadow
// tables replace the old ones.
type writer struct {
	run    sqlRunner
	files  *filesClient
	loc    location
	dir    string
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
	flushes  int
	staged   []string

	queued     []synclet.StateMessage
	held       []synclet.StateMessage
	overwrite  map[synclet.StreamDescriptor]bool
	lastCommit time.Time
}

func newWriter(run sqlRunner, files *filesClient, cfg Config, em *synclet.Emitter, limits Destination, plans []*streamPlan, runID string) *writer {
	w := &writer{
		run:        run,
		files:      files,
		loc:        location{catalog: cfg.Database, schema: cfg.Schema},
		dir:        cfg.volumePath() + "/_synclet/" + runID,
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
		if err := prepareTable(ctx, w.run, w.loc, p); err != nil {
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
		if idx, ok := p.byName[name]; !ok || idx < 2 {
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
	line := make([]byte, 0, len(data)+64)
	line = append(line, `{"s":"`...)
	line = strconv.AppendInt(line, w.seq, 10)
	line = append(line, '"')
	for i, c := range p.columns {
		var val string
		var null bool
		switch i {
		case 0:
			val = uuid.NewString()
		case 1:
			val = extracted.UTC().Format("2006-01-02T15:04:05.000000Z")
		default:
			var err error
			val, null, err = encodeValue(c.typ, fields[c.name])
			if err != nil {
				return fail("column %q: %v", c.name, err)
			}
		}
		line = append(line, ',', '"')
		line = append(line, c.key...)
		line = append(line, '"', ':')
		if null {
			line = append(line, "null"...)
			continue
		}
		enc, err := json.Marshal(val)
		if err != nil {
			return fail("column %q: %v", c.name, err)
		}
		line = append(line, enc...)
	}
	line = append(line, '}', '\n')

	b := &w.batches[p.index]
	b.rows.Write(line)
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

// releaseCommitted echoes the queued STREAM states of p, whose records are
// all committed now; the other states stay queued.
func (w *writer) releaseCommitted(p *streamPlan) error {
	var own []synclet.StateMessage
	rest := w.queued[:0]
	for _, st := range w.queued {
		if w.owner(st) == p {
			own = append(own, st)
		} else {
			rest = append(rest, st)
		}
	}
	w.queued = rest
	return w.release(own)
}

// owner returns the catalog stream a STREAM state belongs to: the one of its
// exact descriptor, failing that the only one of its name, and nil when
// there is none or more than one.
func (w *writer) owner(st synclet.StateMessage) *streamPlan {
	if st.Type != synclet.StateTypeStream || st.Stream == nil {
		return nil
	}
	desc := st.Stream.StreamDescriptor
	if p, ok := w.byDesc[desc]; ok {
		return p
	}
	var found *streamPlan
	for d, p := range w.byDesc {
		if d.Name != desc.Name {
			continue
		}
		if found != nil {
			return nil
		}
		found = p
	}
	return found
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

// flush commits every pending row and echoes the queued states. Every
// stream's batch is staged and copied first; the appends and merges into the
// tables follow once all copies succeeded. A STREAM state is echoed as soon
// as its stream's batch is committed; the other states once every stream's
// is.
func (w *writer) flush(ctx context.Context) error {
	if w.rows > 0 {
		w.flushes++
		var pending []*streamPlan
		for _, p := range w.plans {
			if len(w.batches[p.index].records) == 0 {
				continue
			}
			if err := w.stage(ctx, p); err != nil {
				return err
			}
			pending = append(pending, p)
		}
		for _, p := range pending {
			if err := w.commit(ctx, p); err != nil {
				return err
			}
			if err := w.releaseCommitted(p); err != nil {
				return err
			}
		}
		for i := range w.batches {
			w.batches[i].rows.Reset()
			w.batches[i].records = w.batches[i].records[:0]
		}
		w.rows, w.size = 0, 0
		w.lastCommit = time.Now()
		w.removeStaged(ctx)
	}
	queued := w.queued
	w.queued = nil
	return w.release(queued)
}

// stage uploads one stream's batch to the volume and copies it into the
// stage table, or the shadow table of an overwrite stream.
func (w *writer) stage(ctx context.Context, p *streamPlan) error {
	b := &w.batches[p.index]
	file := fmt.Sprintf("%s/%d-%d.json", w.dir, p.index, w.flushes)
	w.staged = append(w.staged, file)
	if err := w.files.put(ctx, file, b.rows.Bytes()); err != nil {
		return fmt.Errorf("stream %q: stage batch: %w", p.desc.Name, err)
	}
	if p.stage != "" {
		if err := w.run.exec(ctx, createStageSQL(w.loc, p)); err != nil {
			return fmt.Errorf("stream %q: create table %s: %w", p.desc.Name, w.loc.table(p.stage), err)
		}
	}
	n, err := w.count(ctx, copySQL(w.loc, p, file))
	if err != nil {
		return fmt.Errorf("stream %q: COPY INTO from %s: %w", p.desc.Name, file, err)
	}
	if n != len(b.records) {
		return fmt.Errorf("stream %q: COPY INTO loaded %d of %d rows from %s", p.desc.Name, n, len(b.records), file)
	}
	return nil
}

// commit appends or merges a stream's stage table into its table; an
// overwrite stream's batch is already in its shadow table.
func (w *writer) commit(ctx context.Context, p *streamPlan) error {
	switch p.mode {
	case synclet.DestinationSyncModeAppend:
		n, err := w.count(ctx, insertSQL(w.loc, p))
		if err != nil {
			return fmt.Errorf("stream %q: insert batch into %s: %w", p.desc.Name, w.loc.table(p.table), err)
		}
		if want := len(w.batches[p.index].records); n != want {
			return fmt.Errorf("stream %q: INSERT INTO %s added %d of %d rows", p.desc.Name, w.loc.table(p.table), n, want)
		}
	case synclet.DestinationSyncModeAppendDedup:
		if err := w.run.exec(ctx, mergeSQL(w.loc, p)); err != nil {
			return fmt.Errorf("stream %q: merge batch into %s: %w", p.desc.Name, w.loc.table(p.table), err)
		}
	}
	return nil
}

// count runs a statement and returns the num_inserted_rows it reports.
func (w *writer) count(ctx context.Context, stmt string) (int, error) {
	rows, err := w.run.query(ctx, stmt)
	if err != nil {
		return 0, err
	}
	if len(rows) != 1 {
		return 0, fmt.Errorf("expected one result row, got %d", len(rows))
	}
	n, err := strconv.Atoi(rows[0]["num_inserted_rows"])
	if err != nil {
		return 0, fmt.Errorf("num_inserted_rows %q: %w", rows[0]["num_inserted_rows"], err)
	}
	return n, nil
}

// finish commits the remaining rows, replaces the tables of overwrite
// streams and echoes the held states.
func (w *writer) finish(ctx context.Context) error {
	if err := w.flush(ctx); err != nil {
		return err
	}
	for _, p := range w.plans {
		if p.mode != synclet.DestinationSyncModeOverwrite {
			continue
		}
		if err := w.run.exec(ctx, swapSQL(w.loc, p)); err != nil {
			return fmt.Errorf("stream %q: replace table %s: %w", p.desc.Name, w.loc.table(p.table), err)
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

// cleanup drops the shadow and stage tables and deletes the staged files on
// a context of its own, since the sync's context may be cancelled. Data is
// committed or discarded by then, so failures are logged.
func (w *writer) cleanup(ctx context.Context) {
	log := synclet.Logger(ctx)
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cleanupTimeout)
	defer cancel()
	for _, p := range w.plans {
		for _, t := range []string{p.shadow, p.stage} {
			if t == "" {
				continue
			}
			if err := w.run.exec(ctx, dropSQL(w.loc, t)); err != nil {
				log.Warn(fmt.Sprintf("drop table %s: %v", w.loc.table(t), err))
			}
		}
	}
	w.removeStaged(ctx)
	if w.flushes > 0 {
		if err := w.files.deleteDir(ctx, w.dir); err != nil {
			log.Warn(fmt.Sprintf("delete staging directory %s: %v", w.dir, err))
		}
	}
}

// removeStaged deletes the files staged so far.
func (w *writer) removeStaged(ctx context.Context) {
	for _, f := range w.staged {
		if err := w.files.delete(ctx, f); err != nil {
			synclet.Logger(ctx).Warn(fmt.Sprintf("delete staged file %s: %v", f, err))
		}
	}
	w.staged = w.staged[:0]
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
