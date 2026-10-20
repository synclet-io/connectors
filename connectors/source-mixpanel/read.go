package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

// reader reads the streams of one sync. All streams share one client, so
// the hourly budget of each API holds across streams.
type reader struct {
	c       *apiClient
	cfg     Config
	em      *synclet.Emitter
	days    days
	cohorts []cohort
}

// Read emits the configured streams in catalog order. An incremental
// stream emits its state after each date window, or once its full scan is
// done for the streams Mixpanel cannot filter by change time.
func (s Source) Read(ctx context.Context, cfg Config, catalog *synclet.ConfiguredCatalog, state *synclet.InputState, em *synclet.Emitter) error {
	if err := cfg.validate(); err != nil {
		return synclet.ConfigError(err)
	}
	d, err := cfg.days(s.clock())
	if err != nil {
		return synclet.ConfigError(err)
	}
	r := &reader{c: s.apiClient(cfg), cfg: cfg, em: em, days: d}
	for _, cs := range catalog.Streams {
		desc := cs.Stream.Descriptor()
		inc := cs.SyncMode == synclet.SyncModeIncremental
		var saved json.RawMessage
		if inc {
			saved = state.Stream(desc)
		}
		switch desc.Name {
		case streamExport:
			err = r.export(ctx, cs, inc, saved)
		case streamEngage:
			err = r.engage(ctx, cs, inc, saved)
		case streamCohorts:
			err = r.cohortList(ctx, desc, inc, saved)
		case streamCohortMembers:
			err = r.cohortMembers(ctx, cs, inc, saved)
		case streamAnnotations:
			if inc {
				return synclet.ConfigError(fmt.Errorf("stream %s supports full refresh only", desc.Name))
			}
			err = r.annotations(ctx, desc)
		case streamFunnels:
			err = r.funnels(ctx, desc, inc, saved)
		case streamRevenue:
			err = r.revenue(ctx, desc, inc, saved)
		default:
			return synclet.ConfigError(fmt.Errorf("unknown stream %q", desc.Name))
		}
		if err != nil {
			return fmt.Errorf("%s: %w", desc.Name, err)
		}
	}
	return nil
}

// decodeState decodes the saved state of a stream into out; nil leaves out
// as it is.
func decodeState(desc synclet.StreamDescriptor, saved json.RawMessage, out any) error {
	if saved == nil {
		return nil
	}
	if err := json.Unmarshal(saved, out); err != nil {
		return synclet.ConfigError(fmt.Errorf("state of %s: %w", desc.Name, err))
	}
	return nil
}

// dayCursor is the state of a stream read by day: the last day read.
type dayCursor struct {
	Date string `json:"date,omitempty"`
}

// since returns the first day an incremental read of a day-ranged stream
// requests: attribution_window days before the saved day, never before
// the start.
func (r *reader) since(saved string) (time.Time, error) {
	from := r.days.start
	if saved == "" {
		return from, nil
	}
	d, err := optionalDate("saved date", saved)
	if err != nil {
		return from, synclet.ConfigError(err)
	}
	if d = d.AddDate(0, 0, -r.cfg.attributionWindow()); d.After(from) {
		from = d
	}
	return from, nil
}

// windowsFrom returns the date windows from day to the end of the sync.
func (r *reader) windowsFrom(day time.Time) []window {
	return windows(day, r.days.end, r.cfg.dateWindowSize())
}

// text returns a property value as a string: a string as it is, any other
// value as JSON, nil as nil.
func text(v any) any {
	switch t := v.(type) {
	case nil:
		return nil
	case string:
		return t
	case json.Number:
		return t.String()
	case bool:
		return strconv.FormatBool(t)
	}
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Sprint(v)
	}
	return string(b)
}
