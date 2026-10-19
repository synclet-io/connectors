package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

// windowDays is the longest date range of one analytics request.
const windowDays = 30

// analyticsState is the STREAM state of an analytics stream.
type analyticsState struct {
	Accounts map[string]accountState `json:"accounts"`
}

// accountState is the saved progress of one account: the last day of the
// last window read.
type accountState struct {
	Date string `json:"date,omitempty"`
}

func loadAnalyticsState(desc synclet.StreamDescriptor, state *synclet.InputState) (analyticsState, error) {
	st := analyticsState{Accounts: map[string]accountState{}}
	raw := state.Stream(desc)
	if raw == nil {
		return st, nil
	}
	var saved analyticsState
	if err := json.Unmarshal(raw, &saved); err != nil {
		return st, synclet.ConfigError(fmt.Errorf("state of %s: %w", desc.Name, err))
	}
	for id, a := range saved.Accounts {
		st.Accounts[id] = a
	}
	return st, nil
}

// analyticsRange returns the days to read for one account: from the saved
// date minus lookback days, never before start_date, or from start_date
// without a saved date; up to end_date, else today.
func analyticsRange(cfg Config, saved accountState, today time.Time) (since, until time.Time, err error) {
	since, err = cfg.start()
	if err != nil {
		return since, until, err
	}
	if saved.Date != "" {
		d, err := parseDate("saved date", saved.Date)
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

// readAnalytics reads an analytics stream one account at a time, in
// ascending windows of up to windowDays days. An incremental read saves the
// state of every account after each window.
func (s Source) readAnalytics(ctx context.Context, c *apiClient, cfg Config, ids []string, d streamDef, cs synclet.ConfiguredStream, state *synclet.InputState, em *synclet.Emitter) error {
	desc := cs.Stream.Descriptor()
	incremental := cs.SyncMode == synclet.SyncModeIncremental
	st := analyticsState{Accounts: map[string]accountState{}}
	if incremental {
		var err error
		if st, err = loadAnalyticsState(desc, state); err != nil {
			return err
		}
	}
	today := s.today()
	for _, id := range ids {
		since, until, err := analyticsRange(cfg, st.Accounts[id], today)
		if err != nil {
			return synclet.ConfigError(fmt.Errorf("%s: account %s: %w", d.name, id, err))
		}
		for start := since; !start.After(until); {
			end := start.AddDate(0, 0, windowDays-1)
			if end.After(until) {
				end = until
			}
			if err := readWindow(ctx, c, d, id, start, end, func(rec map[string]any) error {
				return em.Record(desc, rec)
			}); err != nil {
				return fmt.Errorf("%s: account %s %s..%s: %w", d.name, id, start.Format(dateLayout), end.Format(dateLayout), err)
			}
			if incremental {
				st.Accounts[id] = accountState{Date: end.Format(dateLayout)}
				if err := em.StreamState(desc, st); err != nil {
					return err
				}
			}
			start = end.AddDate(0, 0, 1)
		}
	}
	return nil
}

// analyticsRow is one element of an analytics finder response.
type analyticsRow struct {
	DateRange struct {
		Start *struct{ Year, Month, Day int } `json:"start"`
	} `json:"dateRange"`
	PivotValues []string `json:"pivotValues"`
}

// readWindow calls fn with the record of every analytics row of one account
// from start to end. A row without a day or pivot, or outside that range,
// fails the window.
func readWindow(ctx context.Context, c *apiClient, d streamDef, account string, start, end time.Time, fn func(map[string]any) error) error {
	q := query{
		{key: "q", value: "analytics"},
		{key: "pivot", value: d.pivot},
		{key: "timeGranularity", value: "DAILY"},
		{key: "dateRange", value: restliDateRange(start, end), raw: true},
		{key: "accounts", value: restliList("urn:li:sponsoredAccount:" + account), raw: true},
		{key: "fields", value: "dateRange,pivotValues," + strings.Join(analyticsMetrics, ","), raw: true},
	}
	var resp struct {
		Elements []json.RawMessage `json:"elements"`
	}
	if err := c.get(ctx, "adAnalytics", q, &resp); err != nil {
		return err
	}
	first, last := start.Format(dateLayout), end.Format(dateLayout)
	for _, raw := range resp.Elements {
		rec, err := analyticsRecord(d, account, raw)
		if err != nil {
			return err
		}
		if day := rec[cursorField].(string); day < first || day > last {
			return fmt.Errorf("analytics row of day %s is outside the requested range", day)
		}
		if err := fn(rec); err != nil {
			return err
		}
	}
	return nil
}

// analyticsRecord flattens one analytics row into a record of d: the
// account, the pivot URN and its id, the day as YYYY-MM-DD and the metrics.
func analyticsRecord(d streamDef, account string, raw json.RawMessage) (map[string]any, error) {
	var row analyticsRow
	if err := json.Unmarshal(raw, &row); err != nil {
		return nil, fmt.Errorf("analytics row: %w", err)
	}
	if row.DateRange.Start == nil {
		return nil, fmt.Errorf("analytics row without dateRange.start: %s", snippet(raw))
	}
	if len(row.PivotValues) != 1 {
		return nil, fmt.Errorf("analytics row with %d pivot values: %s", len(row.PivotValues), snippet(raw))
	}
	s := row.DateRange.Start
	day := time.Date(s.Year, time.Month(s.Month), s.Day, 0, 0, 0, 0, time.UTC)
	if day.Year() != s.Year || int(day.Month()) != s.Month || day.Day() != s.Day {
		return nil, fmt.Errorf("analytics row with invalid date %d-%d-%d", s.Year, s.Month, s.Day)
	}
	urn := row.PivotValues[0]
	pivotID := urn[strings.LastIndex(urn, ":")+1:]
	if !isDigits(pivotID) {
		return nil, fmt.Errorf("analytics row pivot %q is not a URN with a numeric id", urn)
	}
	var obj map[string]any
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.UseNumber()
	if err := dec.Decode(&obj); err != nil {
		return nil, fmt.Errorf("analytics row: %w", err)
	}
	obj["account_id"] = account
	obj[d.pivotField] = pivotID
	obj["pivot_value"] = urn
	obj[cursorField] = day.Format(dateLayout)
	return record(d.fields, obj)
}
