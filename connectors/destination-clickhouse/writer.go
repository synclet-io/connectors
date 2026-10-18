package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
	synclet "github.com/synclet-io/synclet-go-sdk"
)

// batch holds the rows of one stream not yet inserted, as driver values in
// the order of the plan's columns, and the position of each row's record in
// the stream.
type batch struct {
	rows    [][]any
	records []int
}

// writer buffers records of every stream and inserts them in batches. A
// STATE is echoed once every record received before it is inserted; the
// states of an overwrite stream, and GLOBAL and LEGACY states while the
// catalog has an overwrite stream, are held until the shadow tables replace
// the old ones.
type writer struct {
	conn   driver.Conn
	cfg    Config
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
	lastInsert time.Time
}

func newWriter(conn driver.Conn, cfg Config, em *synclet.Emitter, limits Destination, plans []*streamPlan) *writer {
	w := &writer{
		conn:       conn,
		cfg:        cfg,
		em:         em,
		limits:     limits,
		plans:      plans,
		byDesc:     map[synclet.StreamDescriptor]*streamPlan{},
		batches:    make([]batch, len(plans)),
		received:   make([]int, len(plans)),
		unknown:    make([]map[string]struct{}, len(plans)),
		overwrite:  map[synclet.StreamDescriptor]bool{},
		lastInsert: time.Now(),
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
		if err := prepareTable(ctx, w.conn, w.cfg.Database, p); err != nil {
			return fmt.Errorf("stream %q: %w", p.desc.Name, err)
		}
	}
	return nil
}

// addRecord validates one record, converts it into its stream's batch and
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
	if extracted.Before(minTime) || extracted.After(maxTimestamp) {
		return fail("emitted_at %d is outside the range 1900-01-01 to 2262-04-11 that column %s holds", r.EmittedAt, colExtractedAt)
	}
	row := make([]any, 0, len(metaColumns)+len(p.columns))
	row = append(row, uuid.New(), extracted.UTC())
	for _, c := range p.columns {
		val, err := encodeValue(c.kind, fields[c.name])
		if err != nil {
			return fail("column %q: %v", c.name, err)
		}
		if val == nil && !c.nullable {
			if slices.ContainsFunc(p.pk, func(i int) bool { return p.columns[i].name == c.name }) {
				return fail("primary key column %q is null", c.name)
			}
			return fail("cursor column %q is null", c.name)
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

// addState echoes st when no record is pending; otherwise it queues st and
// flushes when the last insert is older than the age limit.
func (w *writer) addState(ctx context.Context, st synclet.StateMessage) error {
	if w.rows == 0 {
		return w.release([]synclet.StateMessage{st})
	}
	w.queued = append(w.queued, st)
	if time.Since(w.lastInsert) >= w.limits.maxAge {
		return w.flush(ctx)
	}
	return nil
}

// release echoes states whose records are inserted in their final table and
// holds the rest until the swap.
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

// flush inserts every pending row, one INSERT per stream, then echoes the
// queued states. ClickHouse has no transaction across INSERTs: when one
// fails, the streams inserted before it keep their rows, and no queued state
// is echoed.
func (w *writer) flush(ctx context.Context) error {
	if w.rows > 0 {
		for _, p := range w.plans {
			if len(w.batches[p.index].rows) == 0 {
				continue
			}
			if err := w.insert(ctx, p); err != nil {
				return err
			}
		}
		for i := range w.batches {
			w.batches[i].rows = w.batches[i].rows[:0]
			w.batches[i].records = w.batches[i].records[:0]
		}
		w.rows, w.size = 0, 0
		w.lastInsert = time.Now()
	}
	queued := w.queued
	w.queued = nil
	return w.release(queued)
}

// insert sends one stream's batch in a single INSERT and returns once the
// server acknowledged it.
func (w *writer) insert(ctx context.Context, p *streamPlan) (rerr error) {
	target := quote(w.cfg.Database, p.target())
	b := &w.batches[p.index]
	ins, err := w.conn.PrepareBatch(ctx, "INSERT INTO "+target+" ("+identList(p.allColumns())+")")
	if err != nil {
		return fmt.Errorf("stream %q: prepare insert into %s: %w", p.desc.Name, target, err)
	}
	defer func() {
		if rerr != nil {
			_ = ins.Abort()
		}
	}()
	for i, row := range b.rows {
		if err := ins.Append(row...); err != nil {
			return fmt.Errorf("stream %q: record %d: %w", p.desc.Name, b.records[i], err)
		}
	}
	if err := ins.Send(); err != nil {
		return fmt.Errorf("stream %q: insert batch into %s: %w", p.desc.Name, target, err)
	}
	return nil
}

// finish inserts the remaining rows, replaces the tables of overwrite
// streams, optimizes append_dedup tables when configured and echoes the held
// states.
func (w *writer) finish(ctx context.Context) error {
	if err := w.flush(ctx); err != nil {
		return err
	}
	if len(w.overwrite) > 0 {
		if err := swapTables(ctx, w.conn, w.cfg.Database, w.plans); err != nil {
			return err
		}
	}
	if w.cfg.OptimizeFinal {
		for _, p := range w.plans {
			if p.mode != synclet.DestinationSyncModeAppendDedup {
				continue
			}
			table := quote(w.cfg.Database, p.table)
			if err := w.conn.Exec(ctx, "OPTIMIZE TABLE "+table+" FINAL"); err != nil {
				return fmt.Errorf("stream %q: optimize %s: %w", p.desc.Name, table, err)
			}
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
