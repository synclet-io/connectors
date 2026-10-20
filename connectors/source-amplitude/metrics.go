package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"
	"time"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

const (
	dateLayout = "2006-01-02"
	// dashboardDateLayout is the start and end format of the Dashboard REST
	// API.
	dashboardDateLayout = "20060102"
)

// metricDef is a daily metric stream of the Dashboard REST API.
type metricDef struct {
	path string
	// windowDays is how many days one request covers.
	windowDays int
	params     func(cfg Config) url.Values
	records    func(data seriesData) []map[string]any
}

var metricDefs = map[string]metricDef{
	streamActiveUsers: {
		path: "2/users", windowDays: 30,
		params: func(cfg Config) url.Values {
			q := url.Values{"m": {"active"}, "i": {"1"}}
			if cfg.groupByCountry() {
				q.Set("g", "country")
			}
			return q
		},
		records: activeUsersRecords,
	},
	streamAverageSessionLength: {
		path: "2/sessions/average", windowDays: 15,
		params:  func(Config) url.Values { return url.Values{} },
		records: averageSessionLengthRecords,
	},
}

// seriesData is the data of a Dashboard REST response: series[j][i] is the
// value of series j on day xValues[i].
type seriesData struct {
	Series       [][]any  `json:"series"`
	SeriesLabels []any    `json:"seriesLabels"`
	XValues      []string `json:"xValues"`
}

// activeUsersRecords returns one record per day whose statistics map each
// series label to the day's count.
func activeUsersRecords(data seriesData) []map[string]any {
	recs := make([]map[string]any, 0, len(data.XValues))
	for i, day := range data.XValues {
		stats := map[string]any{}
		for j, s := range data.Series {
			if i >= len(s) {
				continue
			}
			label := strconv.Itoa(j)
			if j < len(data.SeriesLabels) {
				label = seriesLabel(data.SeriesLabels[j])
			}
			stats[label] = s[i]
		}
		recs = append(recs, map[string]any{"date": day, "statistics": stats})
	}
	return recs
}

// seriesLabel names a series: a label that is a list, such as
// [0, "United States"], is named by its last element.
func seriesLabel(v any) string {
	if l, ok := v.([]any); ok && len(l) > 0 {
		v = l[len(l)-1]
	}
	switch x := v.(type) {
	case string:
		return x
	case json.Number:
		return x.String()
	}
	return fmt.Sprint(v)
}

// averageSessionLengthRecords returns one record per day with the value of
// the first series.
func averageSessionLengthRecords(data seriesData) []map[string]any {
	if len(data.Series) == 0 {
		return nil
	}
	recs := make([]map[string]any, 0, len(data.XValues))
	for i, day := range data.XValues {
		if i >= len(data.Series[0]) {
			break
		}
		recs = append(recs, map[string]any{"date": day, "length": data.Series[0][i]})
	}
	return recs
}

// metricState is the STREAM state of a daily metric stream: the last day
// read. Its counts may still change, so the next read starts on it.
type metricState struct {
	Date string `json:"date"`
}

// readMetric requests the stream in windows of windowDays from the start
// day up to today (UTC) and emits the days not before the start. The start
// is the day of start_date or, in an incremental read, the saved day if that
// is later. An incremental read saves the last day of each window after its
// records.
func (s Source) readMetric(ctx context.Context, cfg Config, c *apiClient, d metricDef, cs synclet.ConfiguredStream, state *synclet.InputState, em *synclet.Emitter) error {
	desc := cs.Stream.Descriptor()
	incremental := cs.SyncMode == synclet.SyncModeIncremental
	start, err := cfg.start()
	if err != nil {
		return synclet.ConfigError(err)
	}
	from := start.Truncate(24 * time.Hour)
	if incremental {
		var st metricState
		if err := savedState(state, desc, &st); err != nil {
			return err
		}
		if st.Date != "" {
			saved, err := time.Parse(dateLayout, st.Date)
			if err != nil {
				return fmt.Errorf("state date %q is not a YYYY-MM-DD date", st.Date)
			}
			if saved.After(from) {
				from = saved
			}
		}
	}
	today := s.clock().UTC().Truncate(24 * time.Hour)
	startDay := from.Format(dateLayout)
	for !from.After(today) {
		to := from.AddDate(0, 0, d.windowDays-1)
		if to.After(today) {
			to = today
		}
		q := d.params(cfg)
		q.Set("start", from.Format(dashboardDateLayout))
		q.Set("end", to.Format(dashboardDateLayout))
		var resp struct {
			Data seriesData `json:"data"`
		}
		if err := c.getJSON(ctx, d.path, q, &resp); err != nil {
			return fmt.Errorf("%s to %s: %w", from.Format(dateLayout), to.Format(dateLayout), err)
		}
		for _, rec := range d.records(resp.Data) {
			if day, _ := rec["date"].(string); day < startDay {
				continue
			}
			if err := em.Record(desc, rec); err != nil {
				return err
			}
		}
		if incremental {
			if err := em.StreamState(desc, metricState{Date: to.Format(dateLayout)}); err != nil {
				return err
			}
		}
		from = to.AddDate(0, 0, 1)
	}
	return nil
}
