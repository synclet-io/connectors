package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"time"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

// exportState is the state of export: the latest event time read.
type exportState struct {
	Time string `json:"time,omitempty"`
}

// rawEvent is one line of the Export API response.
type rawEvent struct {
	Event      string         `json:"event"`
	Properties map[string]any `json:"properties"`
}

// export streams the raw events of each date window. An incremental read
// emits only the events from export_lookback_window before the saved time
// on, starting at the project day of that moment, and emits the latest
// event time after each window.
func (r *reader) export(ctx context.Context, cs synclet.ConfiguredStream, inc bool, saved json.RawMessage) error {
	desc := cs.Stream.Descriptor()
	var st exportState
	if err := decodeState(desc, saved, &st); err != nil {
		return err
	}
	from := r.days.start
	var cursor, since time.Time
	if st.Time != "" {
		t, err := time.Parse(time.RFC3339Nano, st.Time)
		if err != nil {
			return synclet.ConfigError(fmt.Errorf("state of %s: time %q is not an RFC 3339 timestamp", desc.Name, st.Time))
		}
		cursor = t
		since = t.Add(-r.cfg.exportLookback())
		loc, _ := r.cfg.location()
		local := since.In(loc)
		if d := civil(local.Year(), local.Month(), local.Day()); d.After(from) {
			from = d
		}
	}
	ren, hints := exportRenamer(), schemaHints(cs.Stream.JSONSchema)
	for _, w := range r.windowsFrom(from) {
		params := url.Values{"from_date": {w.fromDate()}, "to_date": {w.toDate()}}
		if !since.IsZero() {
			params.Set("where", fmt.Sprintf(`properties["$time"]>=datetime(%d)`, since.Unix()))
		}
		err := r.c.send(ctx, request{api: exportAPI, method: http.MethodGet, path: "export", params: params}, func(body io.Reader) error {
			dec := json.NewDecoder(body)
			dec.UseNumber()
			for n := 1; ; n++ {
				if err := ctx.Err(); err != nil {
					return err
				}
				var ev rawEvent
				err := dec.Decode(&ev)
				if errors.Is(err, io.EOF) {
					return nil
				}
				if err != nil {
					return fmt.Errorf("event %d: %w", n, err)
				}
				rec, t, err := exportRecord(ev, ren, hints)
				if err != nil {
					return fmt.Errorf("event %d: %w", n, err)
				}
				if t.Before(since) {
					continue
				}
				if err := r.em.Record(desc, rec); err != nil {
					return err
				}
				if t.After(cursor) {
					cursor = t
				}
			}
		})
		if err != nil {
			return fmt.Errorf("%s to %s: %w", w.fromDate(), w.toDate(), err)
		}
		if inc && !cursor.IsZero() {
			if err := r.em.StreamState(desc, exportState{Time: cursor.Format(time.RFC3339Nano)}); err != nil {
				return err
			}
		}
	}
	return nil
}

// exportRecord flattens an event into its name and properties. time
// becomes an RFC 3339 timestamp in UTC; every other property is a string.
func exportRecord(ev rawEvent, ren renamer, hints map[string]bool) (map[string]any, time.Time, error) {
	rec := make(map[string]any, len(ev.Properties)+1)
	rec["event"] = ev.Event
	has := func(n string) bool {
		if hints[n] {
			return true
		}
		_, ok := ev.Properties[n]
		return ok
	}
	var t time.Time
	for k, v := range ev.Properties {
		if k == "time" {
			var err error
			if t, err = eventTime(v); err != nil {
				return nil, t, fmt.Errorf("event %q: %w", ev.Event, err)
			}
			rec["time"] = t.Format(time.RFC3339Nano)
			continue
		}
		rec[ren.field(k, has)] = text(v)
	}
	if t.IsZero() {
		return nil, t, fmt.Errorf("event %q has no time property", ev.Event)
	}
	return rec, t, nil
}

// eventTime converts the time property, seconds since the Unix epoch or,
// for values past the year 5138, milliseconds.
func eventTime(v any) (time.Time, error) {
	n, ok := v.(json.Number)
	if !ok {
		return time.Time{}, fmt.Errorf("time %v is not a number", v)
	}
	f, err := n.Float64()
	if err != nil || f <= 0 || math.IsInf(f, 0) {
		return time.Time{}, fmt.Errorf("time %s is not a positive number", n)
	}
	ms := f * 1000
	if f >= 1e11 {
		ms = f
	}
	return time.UnixMilli(int64(math.Round(ms))).UTC(), nil
}
