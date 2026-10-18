package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

// windowDays is the longest date range of one report request. A range the
// API refuses with code 40067 is halved down to one day.
const windowDays = 30

// reportState is the STREAM state of a report stream.
type reportState struct {
	Advertisers map[string]advertiserState `json:"advertisers"`
}

// advertiserState is the saved progress of one advertiser: the last day of
// the last report window read.
type advertiserState struct {
	StatTimeDay string `json:"stat_time_day,omitempty"`
}

func loadReportState(desc synclet.StreamDescriptor, state *synclet.InputState) (reportState, error) {
	st := reportState{Advertisers: map[string]advertiserState{}}
	raw := state.Stream(desc)
	if raw == nil {
		return st, nil
	}
	var saved reportState
	if err := json.Unmarshal(raw, &saved); err != nil {
		return st, synclet.ConfigError(fmt.Errorf("state of %s: %w", desc.Name, err))
	}
	for id, a := range saved.Advertisers {
		st.Advertisers[id] = a
	}
	return st, nil
}

// reportRange returns the days to read for one advertiser: from the saved
// stat_time_day minus lookback days, never before start_date, or from
// start_date without a saved day; up to end_date, else today.
func reportRange(cfg Config, saved advertiserState, today time.Time) (since, until time.Time, err error) {
	since, err = cfg.start()
	if err != nil {
		return since, until, err
	}
	if saved.StatTimeDay != "" {
		d, err := parseDate("saved stat_time_day", saved.StatTimeDay)
		if err != nil {
			return since, until, err
		}
		if d = d.AddDate(0, 0, -cfg.lookback()); d.After(since) {
			since = d
		}
	}
	until = today
	end, err := cfg.end()
	if err != nil {
		return since, until, err
	}
	if !end.IsZero() && end.Before(until) {
		until = end
	}
	return since, until, nil
}

// readReport reads a report stream one advertiser at a time, in ascending
// windows of up to windowDays days. An incremental read saves the state of
// every advertiser after each window.
func (s Source) readReport(ctx context.Context, c *apiClient, cfg Config, d streamDef, cs synclet.ConfiguredStream, state *synclet.InputState, em *synclet.Emitter) error {
	desc := cs.Stream.Descriptor()
	incremental := cs.SyncMode == synclet.SyncModeIncremental
	st := reportState{Advertisers: map[string]advertiserState{}}
	if incremental {
		var err error
		if st, err = loadReportState(desc, state); err != nil {
			return err
		}
	}
	today := s.today()
	for _, adv := range cfg.advertisers() {
		since, until, err := reportRange(cfg, st.Advertisers[adv], today)
		if err != nil {
			return synclet.ConfigError(fmt.Errorf("%s: advertiser %s: %w", d.name, adv, err))
		}
		days := windowDays
		for start := since; !start.After(until); {
			end := minTime(start.AddDate(0, 0, days-1), until)
			first, last := start.Format(dateLayout), end.Format(dateLayout)
			err := readWindow(ctx, c, d, adv, first, last, func(rec map[string]any) error {
				return em.Record(desc, rec)
			})
			var ae *apiError
			if errors.As(err, &ae) && ae.code == codeDateRange && days > 1 {
				days /= 2
				continue
			}
			if err != nil {
				return fmt.Errorf("%s: advertiser %s %s..%s: %w", d.name, adv, first, last, err)
			}
			if incremental {
				st.Advertisers[adv] = advertiserState{StatTimeDay: last}
				if err := em.StreamState(desc, st); err != nil {
					return err
				}
			}
			start = end.AddDate(0, 0, 1)
		}
	}
	return nil
}

// readWindow calls fn with the record of every report row of one advertiser
// from first to last, both YYYY-MM-DD. A row without stat_time_day or outside
// that range fails the window.
func readWindow(ctx context.Context, c *apiClient, d streamDef, adv, first, last string, fn func(map[string]any) error) error {
	q := url.Values{
		"advertiser_id": {adv},
		"report_type":   {"BASIC"},
		"data_level":    {d.dataLevel},
		"dimensions":    {jsonList(d.dimension, cursorField)},
		"metrics":       {jsonList(reportMetrics...)},
		"start_date":    {first},
		"end_date":      {last},
	}
	return c.list(ctx, "report/integrated/get/", q, func(row map[string]any) error {
		rec := reportRecord(d, adv, row)
		day, ok := rec[cursorField].(string)
		switch {
		case !ok || day == "":
			return fmt.Errorf("report row has no %s", cursorField)
		case day < first || day > last:
			return fmt.Errorf("report row of day %s is outside the requested window", day)
		}
		return fn(rec)
	})
}

// reportRecord flattens the dimensions and metrics of one report row into a
// record of d, with advertiser_id and stat_time_day as YYYY-MM-DD.
func reportRecord(d streamDef, adv string, row map[string]any) map[string]any {
	flat := map[string]any{"advertiser_id": adv}
	for _, part := range []string{"metrics", "dimensions"} {
		if m, ok := row[part].(map[string]any); ok {
			for k, v := range m {
				flat[k] = v
			}
		}
	}
	if day, ok := flat[cursorField].(string); ok {
		day, _, _ = strings.Cut(day, " ")
		flat[cursorField] = day
	}
	return record(d.fields, flat)
}

func minTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}
