package engine

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"time"

	synclet "github.com/synclet-io/synclet-go-sdk"

	"source-declarative/internal/manifest"
)

const dateLayout = "2006-01-02"

// parseCursor parses a cursor value as a record or a state carries it.
func parseCursor(v any, format string) (time.Time, error) {
	switch format {
	case manifest.FormatUnixS, manifest.FormatUnixMS:
		var s string
		switch x := v.(type) {
		case json.Number:
			s = x.String()
		case string:
			s = x
		default:
			return time.Time{}, fmt.Errorf("%v is not a %s number", v, format)
		}
		f, err := strconv.ParseFloat(s, 64)
		if err != nil || math.IsInf(f, 0) || math.IsNaN(f) {
			return time.Time{}, fmt.Errorf("%q is not a %s number", s, format)
		}
		if format == manifest.FormatUnixMS {
			f /= 1000
		}
		sec, frac := math.Modf(f)
		return time.Unix(int64(sec), int64(frac*1e9)).UTC(), nil
	}
	s, ok := v.(string)
	if !ok {
		return time.Time{}, fmt.Errorf("%v is not a %s string", v, format)
	}
	layout := format
	switch format {
	case manifest.FormatRFC3339:
		layout = time.RFC3339Nano
	case manifest.FormatDate:
		layout = dateLayout
	}
	t, err := time.Parse(layout, s)
	if err != nil {
		return time.Time{}, fmt.Errorf("%q is not in cursor format %s", s, format)
	}
	return t.UTC(), nil
}

// formatTime renders t in a cursor format, as text.
func formatTime(t time.Time, format string) string {
	t = t.UTC()
	switch format {
	case manifest.FormatUnixS:
		return strconv.FormatInt(t.Unix(), 10)
	case manifest.FormatUnixMS:
		return strconv.FormatInt(t.UnixMilli(), 10)
	case manifest.FormatRFC3339:
		return t.Format(time.RFC3339)
	case manifest.FormatDate:
		return t.Format(dateLayout)
	}
	return t.Format(format)
}

// stateValue renders t as a state carries a cursor of format: a number for
// the unix formats, a string otherwise.
func stateValue(t time.Time, format string) any {
	s := formatTime(t, format)
	if format == manifest.FormatUnixS || format == manifest.FormatUnixMS {
		return json.Number(s)
	}
	return s
}

// parseStart parses incremental.start: RFC 3339 or YYYY-MM-DD.
func parseStart(s string) (time.Time, error) {
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t.UTC(), nil
	}
	if t, err := time.Parse(dateLayout, s); err == nil {
		return t, nil
	}
	return time.Time{}, fmt.Errorf("%q is neither RFC 3339 nor YYYY-MM-DD", s)
}

// cursor is a cursor value with its parsed time.
type cursor struct {
	raw any
	t   time.Time
}

// tracker drops a stream's records older than since and keeps the
// greatest cursor of those it lets through.
type tracker struct {
	desc    synclet.StreamDescriptor
	field   manifest.Field
	format  string
	since   time.Time
	max     *cursor
	dropped int
}

// accept reports whether rec is emitted.
func (t *tracker) accept(rec map[string]any, desc string) (bool, error) {
	v, ok := manifest.GetField(rec, t.field)
	if !ok || v == nil {
		return false, fmt.Errorf("a record of %s has no value at cursor field %s (%s)", t.desc.Name, t.field.Raw, desc)
	}
	ct, err := parseCursor(v, t.format)
	if err != nil {
		return false, fmt.Errorf("a record of %s: cursor field %s: %w (%s)", t.desc.Name, t.field.Raw, err, desc)
	}
	if ct.Before(t.since) {
		t.dropped++
		return false, nil
	}
	t.observe(cursor{raw: v, t: ct})
	return true, nil
}

func (t *tracker) observe(c cursor) {
	if t.max == nil || c.t.After(t.max.t) {
		t.max = &c
	}
}

// savedCursor reads a stream's saved cursor: state is {"<key>": value}.
func savedCursor(raw json.RawMessage, key, format string) (*cursor, error) {
	if raw == nil {
		return nil, nil
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, fmt.Errorf("not an object: %w", err)
	}
	v, ok := m[key]
	if !ok {
		return nil, fmt.Errorf("no %q", key)
	}
	return cursorFromJSON(v, format)
}

func cursorFromJSON(raw json.RawMessage, format string) (*cursor, error) {
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	var v any
	if err := d.Decode(&v); err != nil {
		return nil, err
	}
	if v == nil {
		return nil, errors.New("cursor is null")
	}
	t, err := parseCursor(v, format)
	if err != nil {
		return nil, err
	}
	return &cursor{raw: v, t: t}, nil
}

// window is one [start, end) span of an incremental read; set is false for
// a stream without bounds.
type window struct {
	start, end time.Time
	set        bool
}

// timing is how a stream, or a parent pass, bounds its reads: start,
// lookback and windows of the incremental block that owns them.
type timing struct {
	inc   *manifest.Incremental
	start time.Time
	now   time.Time
}

func (r *run) timing(inc *manifest.Incremental) (*timing, error) {
	if inc == nil {
		return nil, nil
	}
	s, err := inc.Start.Render(r.scope(), nil)
	if err != nil {
		return nil, synclet.ConfigError(fmt.Errorf("incremental.start: %w", err))
	}
	if s == "" {
		return nil, synclet.ConfigError(errors.New("incremental.start renders empty; set the config value it names"))
	}
	start, err := parseStart(s)
	if err != nil {
		return nil, synclet.ConfigError(fmt.Errorf("incremental.start: %w", err))
	}
	return &timing{inc: inc, start: start, now: r.o.Now().UTC()}, nil
}

func (tm *timing) lookback() time.Duration {
	if tm.inc.Lookback == nil {
		return 0
	}
	return time.Duration(*tm.inc.Lookback)
}

// since is the lower bound of a read that resumes from saved, or start.
func (tm *timing) since(saved *cursor) time.Time {
	if saved == nil {
		return tm.start
	}
	var s time.Time
	if w := tm.inc.Window; w != nil && w.ClosedOnly {
		step := time.Duration(w.Step)
		next := tm.floor(saved.t).Add(step)
		s = tm.floor(next.Add(-tm.lookback()))
	} else {
		s = saved.t.Add(-tm.lookback())
	}
	if s.Before(tm.start) {
		return tm.start
	}
	return s
}

// floor aligns t down to a window boundary, start plus whole steps.
func (tm *timing) floor(t time.Time) time.Time {
	step := time.Duration(tm.inc.Window.Step)
	if t.Before(tm.start) {
		return tm.start
	}
	return tm.start.Add(t.Sub(tm.start) / step * step)
}

// windows splits [since, now) by window.step; with closed_only only
// windows that ended by now.
func (tm *timing) windows(since time.Time) []window {
	w := tm.inc.Window
	if w == nil {
		return []window{{start: since, end: tm.now, set: true}}
	}
	step := time.Duration(w.Step)
	var out []window
	for s := since; s.Before(tm.now); s = s.Add(step) {
		e := s.Add(step)
		if e.After(tm.now) {
			if w.ClosedOnly {
				break
			}
			e = tm.now
		}
		out = append(out, window{start: s, end: e, set: true})
	}
	return out
}

func (tm *timing) sinceFormat(own *manifest.Incremental) string {
	if tm.inc.SinceFormat != "" {
		return tm.inc.SinceFormat
	}
	return own.CursorFormat
}

// bounds sets since and until of scope to w in format.
func bounds(scope manifest.Scope, w window, format string) manifest.Scope {
	if !w.set {
		return scope
	}
	s, u := formatTime(w.start, format), formatTime(w.end, format)
	scope.Since, scope.Until = &s, &u
	return scope
}
