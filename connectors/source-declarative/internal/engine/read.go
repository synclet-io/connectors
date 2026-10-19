package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	synclet "github.com/synclet-io/synclet-go-sdk"

	"source-declarative/internal/manifest"
)

// member is a configured stream of a read.
type member struct {
	st          *manifest.Stream
	desc        synclet.StreamDescriptor
	incremental bool
	tr          *tracker
}

func (r *run) stream(name string) *manifest.Stream {
	for i := range r.m.Streams {
		if r.m.Streams[i].Name == name {
			return &r.m.Streams[i]
		}
	}
	return nil
}

func (r *run) children(parent string) []*manifest.Stream {
	var out []*manifest.Stream
	for i := range r.m.Streams {
		if p := r.m.Streams[i].Partition; p != nil && p.Parent == parent {
			out = append(out, &r.m.Streams[i])
		}
	}
	return out
}

func parentOf(st *manifest.Stream) string {
	if st.Partition != nil {
		return st.Partition.Parent
	}
	return ""
}

func (r *run) read(ctx context.Context, catalog *synclet.ConfiguredCatalog, state *synclet.InputState) error {
	selected := map[string]*member{}
	var order []string
	for _, cs := range catalog.Streams {
		desc := cs.Stream.Descriptor()
		st := r.stream(desc.Name)
		if st == nil || desc.Namespace != "" {
			return synclet.ConfigError(fmt.Errorf("unknown stream %q", desc.Name))
		}
		if _, dup := selected[st.Name]; dup {
			return synclet.ConfigError(fmt.Errorf("stream %q is configured twice", st.Name))
		}
		inc := cs.SyncMode == synclet.SyncModeIncremental
		if inc && st.Incremental == nil {
			return synclet.ConfigError(fmt.Errorf("stream %s supports full refresh only", st.Name))
		}
		selected[st.Name] = &member{st: st, desc: desc, incremental: inc}
		order = append(order, st.Name)
	}
	done := map[string]bool{}
	for _, name := range order {
		root := name
		if p := parentOf(selected[name].st); p != "" {
			root = p
		}
		if done[root] {
			continue
		}
		done[root] = true
		var err error
		if kids := r.children(root); len(kids) > 0 {
			err = r.pass(ctx, r.stream(root), kids, selected, state)
		} else {
			err = r.single(ctx, selected[name], state)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// newTracker sets up m's cursor filter from its saved state, or nil for a
// stream without a cursor.
func (r *run) newTracker(m *member, tm *timing, saved *cursor) *tracker {
	inc := m.st.Incremental
	if inc == nil || tm == nil {
		return nil
	}
	t := &tracker{desc: m.desc, field: inc.CursorField, format: inc.CursorFormat, since: tm.start}
	if m.incremental {
		t.since = tm.since(saved)
	}
	return t
}

func stateErr(desc synclet.StreamDescriptor, err error) error {
	return synclet.ConfigError(fmt.Errorf("state of %s: %w", desc.Name, err))
}

func (r *run) logDropped(ctx context.Context, t *tracker) {
	if t != nil && t.dropped > 0 {
		r.logger(ctx).Info(fmt.Sprintf("%s: %d record(s) older than since %s were not emitted", t.desc.Name, t.dropped, t.since.Format(time.RFC3339)))
	}
}

// emitPage filters a page's records through t and emits them.
func (r *run) emitPage(m *member, t *tracker, pg page) error {
	for _, rec := range pg.records {
		if t != nil {
			ok, err := t.accept(rec, pg.resp.desc)
			if err != nil {
				return err
			}
			if !ok {
				continue
			}
		}
		if err := r.em.Record(m.desc, rec); err != nil {
			return err
		}
	}
	return nil
}

// advance is the cursor after window w: the greater of the greatest record
// cursor and the window's start.
func advance(t *tracker, w window) {
	t.observe(cursor{raw: stateValue(w.start, t.format), t: w.start})
}

// single reads a stream that is neither a parent nor a child.
func (r *run) single(ctx context.Context, m *member, state *synclet.InputState) error {
	tm, err := r.timing(m.st.Incremental)
	if err != nil {
		return err
	}
	if p := m.st.Partition; p != nil && p.Values != nil {
		return r.byValue(ctx, m, tm, state)
	}
	var saved *cursor
	if m.incremental {
		if saved, err = savedCursor(state.Stream(m.desc), m.st.Incremental.CursorField.Last(), m.st.Incremental.CursorFormat); err != nil {
			return stateErr(m.desc, err)
		}
	}
	t := r.newTracker(m, tm, saved)
	if err := r.windows(ctx, m, tm, t, r.scope(), func() error {
		return r.emitCursor(m, t)
	}); err != nil {
		return err
	}
	r.logDropped(ctx, t)
	if m.incremental && (m.st.Incremental.Window == nil) && t.max != nil {
		return r.emitCursor(m, t)
	}
	return nil
}

// windows reads m once per window of its timing; afterWindow runs after
// each window of a windowed incremental read.
func (r *run) windows(ctx context.Context, m *member, tm *timing, t *tracker, scope manifest.Scope, afterWindow func() error) error {
	wins := []window{{}}
	format := ""
	if tm != nil {
		wins = tm.windows(t.since)
		format = tm.sinceFormat(m.st.Incremental)
	}
	for _, w := range wins {
		err := r.pages(ctx, m.st, bounds(scope, w, format), false, func(pg page) error { return r.emitPage(m, t, pg) })
		if err != nil && !errors.Is(err, errSkipped) {
			return err
		}
		if m.incremental && tm.inc.Window != nil {
			advance(t, w)
			if err := afterWindow(); err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *run) emitCursor(m *member, t *tracker) error {
	if t.max == nil {
		return nil
	}
	return r.em.StreamState(m.desc, map[string]any{m.st.Incremental.CursorField.Last(): t.max.raw})
}

// byValue reads a partition.values stream value by value, with a cursor
// per value; entries of values no longer configured are kept.
func (r *run) byValue(ctx context.Context, m *member, tm *timing, state *synclet.InputState) error {
	vals, err := r.partitionValues(m.st)
	if err != nil {
		return err
	}
	parts := map[string]any{}
	savedByValue := map[string]*cursor{}
	if m.incremental {
		if raw := state.Stream(m.desc); raw != nil {
			var s struct {
				Partitions map[string]json.RawMessage `json:"partitions"`
			}
			if err := json.Unmarshal(raw, &s); err != nil || s.Partitions == nil {
				return stateErr(m.desc, errors.New(`want {"partitions": {...}}`))
			}
			key := m.st.Incremental.CursorField.Last()
			for v, entry := range s.Partitions {
				c, err := savedCursor(entry, key, m.st.Incremental.CursorFormat)
				if err != nil {
					return stateErr(m.desc, fmt.Errorf("partition %q: %w", v, err))
				}
				savedByValue[v] = c
				parts[v] = map[string]any{key: c.raw}
			}
		}
	}
	emit := func(v string, t *tracker) error {
		if !m.incremental || t.max == nil {
			return nil
		}
		parts[v] = map[string]any{m.st.Incremental.CursorField.Last(): t.max.raw}
		return r.em.StreamState(m.desc, map[string]any{"partitions": parts})
	}
	for _, v := range vals {
		scope := r.scope()
		scope.Partition = &v
		t := r.newTracker(m, tm, savedByValue[v])
		if err := r.windows(ctx, m, tm, t, scope, func() error { return emit(v, t) }); err != nil {
			return err
		}
		r.logDropped(ctx, t)
		if m.incremental && m.st.Incremental.Window == nil {
			if err := emit(v, t); err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *run) partitionValues(st *manifest.Stream) ([]string, error) {
	pv := st.Partition.Values
	if pv.Template == nil {
		return pv.List, nil
	}
	e, _ := pv.Template.Single()
	vals, err := manifest.ConfigList(r.cfg, e.Keys[0])
	if err != nil {
		return nil, synclet.ConfigError(err)
	}
	return vals, nil
}

// pass reads a parent and its children in one pass over the parent: each
// parent record is emitted when the parent is configured, then every
// configured child reads for it to its last page. The pass runs from the
// oldest since of the configured streams of the group.
func (r *run) pass(ctx context.Context, parent *manifest.Stream, kids []*manifest.Stream, selected map[string]*member, state *synclet.InputState) error {
	tm, err := r.timing(parent.Incremental)
	if err != nil {
		return err
	}
	pm := selected[parent.Name]
	var group []*member
	if pm != nil {
		group = append(group, pm)
	}
	var children []*member
	for _, k := range kids {
		if c := selected[k.Name]; c != nil {
			children = append(children, c)
			group = append(group, c)
		}
	}
	for _, m := range group {
		var saved *cursor
		if m.incremental {
			inc := m.st.Incremental
			if saved, err = savedCursor(state.Stream(m.desc), inc.CursorField.Last(), inc.CursorFormat); err != nil {
				return stateErr(m.desc, err)
			}
		}
		m.tr = r.newTracker(m, tm, saved)
	}
	parentMember := pm
	if parentMember == nil {
		parentMember = &member{st: parent, desc: synclet.StreamDescriptor{Name: parent.Name}}
	}
	// pt bounds the pass: a parent record older than every since is
	// outside it.
	var pt *tracker
	wins := []window{{}}
	format := ""
	if tm != nil {
		pt = &tracker{desc: parentMember.desc, field: parent.Incremental.CursorField, format: parent.Incremental.CursorFormat}
		first := true
		for _, m := range group {
			s := tm.start
			if m.tr != nil {
				s = m.tr.since
			}
			if first || s.Before(pt.since) {
				pt.since, first = s, false
			}
		}
		wins = tm.windows(pt.since)
		format = tm.sinceFormat(parent.Incremental)
	}
	emitStates := func() error {
		for _, m := range group {
			if m.incremental {
				if err := r.emitCursor(m, m.tr); err != nil {
					return err
				}
			}
		}
		return nil
	}
	for _, w := range wins {
		scope := bounds(r.scope(), w, format)
		err := r.pages(ctx, parent, scope, false, func(pg page) error {
			for _, rec := range pg.records {
				if pt != nil {
					ok, err := pt.accept(rec, pg.resp.desc)
					if err != nil {
						return err
					}
					if !ok {
						continue
					}
				}
				if pm != nil {
					if err := r.emitPage(pm, pm.tr, page{resp: pg.resp, records: []map[string]any{rec}}); err != nil {
						return err
					}
				}
				for _, c := range children {
					cs := scope
					cs.Parent = rec
					err := r.pages(ctx, c.st, cs, false, func(cp page) error { return r.emitPage(c, c.tr, cp) })
					if err != nil && !errors.Is(err, errSkipped) {
						return err
					}
				}
			}
			return nil
		})
		if err != nil && !errors.Is(err, errSkipped) {
			return err
		}
		if tm != nil && tm.inc.Window != nil {
			for _, m := range group {
				if m.incremental {
					advance(m.tr, w)
				}
			}
			if err := emitStates(); err != nil {
				return err
			}
		}
	}
	for _, m := range group {
		r.logDropped(ctx, m.tr)
	}
	if tm != nil && tm.inc.Window == nil {
		return emitStates()
	}
	return nil
}

// check sends the first request of check.stream: page size 1, the first
// partition value, the first parent record, the first window.
func (r *run) check(ctx context.Context) error {
	st := r.stream(r.m.Check.Stream)
	scope := r.scope()
	if p := st.Partition; p != nil && p.Values != nil {
		vals, err := r.partitionValues(st)
		if err != nil {
			return err
		}
		if len(vals) == 0 {
			return synclet.ConfigError(fmt.Errorf("check stream %s has no partition value to read", st.Name))
		}
		scope.Partition = &vals[0]
	}
	timed := st
	if p := parentOf(st); p != "" {
		timed = r.stream(p)
	}
	if timed.Incremental != nil {
		tm, err := r.timing(timed.Incremental)
		if err != nil {
			return err
		}
		w := window{start: tm.start, end: tm.now, set: true}
		if wins := tm.windows(tm.start); len(wins) > 0 {
			w = wins[0]
		} else if win := tm.inc.Window; win != nil {
			w.end = tm.start.Add(time.Duration(win.Step))
		}
		scope = bounds(scope, w, tm.sinceFormat(timed.Incremental))
	}
	if p := parentOf(st); p != "" {
		var first map[string]any
		err := r.pages(ctx, r.stream(p), scope, false, func(pg page) error {
			if len(pg.records) > 0 {
				first = pg.records[0]
			}
			return errStop
		})
		if err != nil && !errors.Is(err, errStop) {
			return err
		}
		if first == nil {
			return nil
		}
		scope.Parent = first
	}
	err := r.pages(ctx, st, scope, true, func(page) error { return nil })
	if errors.Is(err, errSkipped) {
		return nil
	}
	return err
}

var errStop = errors.New("stop")
