package main

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"time"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

const (
	// requestWindowDays is how many days back, today included, the API
	// serves.
	requestWindowDays = 45
	// hourWindowDays is how many days back, today included, the advertiser
	// report has the hour column.
	hourWindowDays = 30
)

func (s Source) clock() time.Time {
	if s.now != nil {
		return s.now()
	}
	return time.Now()
}

// Discover returns both report streams; every key has the same columns, so
// nothing is requested.
func (Source) Discover(_ context.Context, cfg Config) (*synclet.Catalog, error) {
	if err := cfg.validate(); err != nil {
		return nil, synclet.ConfigError(err)
	}
	cat := &synclet.Catalog{}
	for _, d := range streamDefs {
		cat.Streams = append(cat.Streams, d.stream())
	}
	return cat, nil
}

// Read emits every configured stream in catalog order.
func (s Source) Read(ctx context.Context, cfg Config, catalog *synclet.ConfiguredCatalog, state *synclet.InputState, em *synclet.Emitter) error {
	if err := cfg.validate(); err != nil {
		return synclet.ConfigError(err)
	}
	for _, cs := range catalog.Streams {
		d, ok := streamDefByName(cs.Stream.Name)
		if !ok {
			return synclet.ConfigError(fmt.Errorf("unknown stream %q", cs.Stream.Name))
		}
		if err := s.readStream(ctx, cfg, d, cs, state, em); err != nil {
			return fmt.Errorf("%s: %w", d.name, err)
		}
	}
	return nil
}

// streamState is the STREAM state of a report stream: the last day read.
type streamState struct {
	Day string `json:"day"`
}

// readStream requests one day at a time from the start day up to today (UTC)
// and emits the rows of days not before the start. The start is start_date,
// in an incremental read with a saved day the saved day minus lookback_days
// if that is later, and never earlier than the API's request window. An
// incremental read saves each day after its rows.
func (s Source) readStream(ctx context.Context, cfg Config, d streamDef, cs synclet.ConfiguredStream, state *synclet.InputState, em *synclet.Emitter) error {
	log := synclet.Logger(ctx).With("stream", d.name)
	desc := cs.Stream.Descriptor()
	incremental := cs.SyncMode == synclet.SyncModeIncremental
	today := day(s.clock())
	start, err := cfg.start()
	if err != nil {
		return synclet.ConfigError(err)
	}
	if incremental {
		saved, err := savedDay(state, desc)
		if err != nil {
			return err
		}
		if resume := saved.AddDate(0, 0, -cfg.lookback()); !saved.IsZero() && resume.After(start) {
			start = resume
		}
	}
	if earliest := today.AddDate(0, 0, -(requestWindowDays - 1)); start.Before(earliest) {
		log.Warn(fmt.Sprintf("start %s is before the API's %d-day request window; reading from %s",
			start.Format(dateLayout), requestWindowDays, earliest.Format(dateLayout)))
		start = earliest
	}
	selected, err := selectedColumns(cs)
	if err != nil {
		return err
	}
	columns := d.requestColumns(selected)
	hourStart := today.AddDate(0, 0, -(hourWindowDays - 1))
	startDay := start.Format(dateLayout)
	warnedHour := false
	c := s.apiClient(cfg)
	for dt := start; !dt.After(today); dt = dt.AddDate(0, 0, 1) {
		dayStr := dt.Format(dateLayout)
		cols := columns
		if dt.Before(hourStart) && slices.Contains(cols, hourColumn) {
			cols = slices.DeleteFunc(slices.Clone(cols), func(c string) bool { return c == hourColumn })
			if !warnedHour {
				log.Warn(fmt.Sprintf("hour is available for the last %d days only; days before %s are read without it",
					hourWindowDays, hourStart.Format(dateLayout)))
				warnedHour = true
			}
		}
		rows, err := c.report(ctx, d, dayStr, cols)
		if err != nil {
			return fmt.Errorf("day %s: %w", dayStr, err)
		}
		for _, row := range rows {
			rec, err := d.record(row)
			if err != nil {
				return fmt.Errorf("day %s: %w", dayStr, err)
			}
			rowDay, _ := rec[cursorField].(string)
			if rowDay == "" {
				return fmt.Errorf("day %s: a row has no day", dayStr)
			}
			if rowDay < startDay {
				continue
			}
			if err := em.Record(desc, rec); err != nil {
				return err
			}
		}
		if incremental {
			if err := em.StreamState(desc, streamState{Day: dayStr}); err != nil {
				return err
			}
		}
	}
	return nil
}

// savedDay returns the day of the stream's saved state, or the zero time
// without one.
func savedDay(state *synclet.InputState, desc synclet.StreamDescriptor) (time.Time, error) {
	if state == nil {
		return time.Time{}, nil
	}
	raw := state.Stream(desc)
	if raw == nil {
		return time.Time{}, nil
	}
	var st streamState
	if err := json.Unmarshal(raw, &st); err != nil {
		return time.Time{}, fmt.Errorf("decode state: %w", err)
	}
	if st.Day == "" {
		return time.Time{}, nil
	}
	t, err := time.Parse(dateLayout, st.Day)
	if err != nil {
		return time.Time{}, fmt.Errorf("state day %q is not a YYYY-MM-DD date", st.Day)
	}
	return t, nil
}

// selectedColumns returns the property names of the configured stream's
// schema.
func selectedColumns(cs synclet.ConfiguredStream) (map[string]bool, error) {
	var schema struct {
		Properties map[string]json.RawMessage `json:"properties"`
	}
	if len(cs.Stream.JSONSchema) > 0 {
		if err := json.Unmarshal(cs.Stream.JSONSchema, &schema); err != nil {
			return nil, fmt.Errorf("decode configured schema: %w", err)
		}
	}
	selected := make(map[string]bool, len(schema.Properties))
	for name := range schema.Properties {
		selected[name] = true
	}
	return selected, nil
}
