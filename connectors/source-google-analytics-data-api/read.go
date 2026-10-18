package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

const (
	// defaultPageLimit is the most rows runReport returns in one response.
	defaultPageLimit = 250000
	// maxHourWaits is how many times in a row a property's hourly token
	// quota is waited out before the read fails.
	maxHourWaits = 2
)

// reportState is the STREAM state of a report: per property, the last day
// read.
type reportState struct {
	Properties map[string]propertyState `json:"properties"`
}

type propertyState struct {
	Date string `json:"date"`
}

func loadState(desc synclet.StreamDescriptor, state *synclet.InputState) (reportState, error) {
	st := reportState{Properties: map[string]propertyState{}}
	raw := state.Stream(desc)
	if raw == nil {
		return st, nil
	}
	var saved reportState
	if err := json.Unmarshal(raw, &saved); err != nil {
		return st, synclet.ConfigError(fmt.Errorf("state of %s: %w", desc.Name, err))
	}
	for id, p := range saved.Properties {
		st.Properties[propertyID(id)] = p
	}
	return st, nil
}

// dailyRange returns the days to read for one property: from the saved date
// minus lookback_days, never before start_date, or from start_date without a
// saved date; up to end_date, else today.
func dailyRange(cfg Config, saved string, today time.Time) (since, until time.Time, err error) {
	if since, err = cfg.start(); err != nil {
		return since, until, err
	}
	if saved != "" {
		d, err := parseDate("saved date", saved)
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

// propertyQuota is what the last response of a property said about its
// token quotas.
type propertyQuota struct {
	hourLow   bool
	hourWaits int
	dayOut    string
}

// reader reads reports one property and day at a time, pacing requests by
// the property quota each response returns.
type reader struct {
	src   Source
	c     *apiClient
	cfg   Config
	set   streamSet
	em    *synclet.Emitter
	log   *slog.Logger
	limit int
	quota map[string]*propertyQuota
}

// Read emits every configured stream in catalog order. Each stream reads
// every property day by day; an incremental read emits its state after each
// day.
func (s Source) Read(ctx context.Context, cfg Config, catalog *synclet.ConfiguredCatalog, state *synclet.InputState, em *synclet.Emitter) error {
	if err := cfg.validate(); err != nil {
		return synclet.ConfigError(err)
	}
	c, err := s.apiClient(ctx, cfg)
	if err != nil {
		return err
	}
	set, err := streams(ctx, c, cfg)
	if err != nil {
		return err
	}
	r := &reader{
		src: s, c: c, cfg: cfg, set: set, em: em, log: synclet.Logger(ctx),
		limit: s.pageLimit, quota: map[string]*propertyQuota{},
	}
	if r.limit <= 0 {
		r.limit = defaultPageLimit
	}
	for _, cs := range catalog.Streams {
		d, ok := set.byName(cs.Stream.Name)
		if !ok {
			return synclet.ConfigError(fmt.Errorf("unknown stream %q", cs.Stream.Name))
		}
		if err := r.stream(ctx, d, cs, state); err != nil {
			return err
		}
	}
	return nil
}

func (r *reader) stream(ctx context.Context, d reportDef, cs synclet.ConfiguredStream, state *synclet.InputState) error {
	desc := synclet.StreamDescriptor{Name: d.name, Namespace: namespace}
	incremental := cs.SyncMode == synclet.SyncModeIncremental
	st := reportState{Properties: map[string]propertyState{}}
	if incremental {
		var err error
		if st, err = loadState(desc, state); err != nil {
			return err
		}
	}
	today := r.src.today()
	for _, prop := range r.cfg.properties() {
		since, until, err := dailyRange(r.cfg, st.Properties[prop].Date, today)
		if err != nil {
			return synclet.ConfigError(fmt.Errorf("%s: property %s: %w", d.name, prop, err))
		}
		for day := since; !day.After(until); day = day.AddDate(0, 0, 1) {
			ds := day.Format(dateLayout)
			if err := r.day(ctx, d, desc, prop, ds); err != nil {
				return fmt.Errorf("%s: property %s, %s: %w", d.name, prop, ds, err)
			}
			if incremental {
				st.Properties[prop] = propertyState{Date: ds}
				if err := r.em.StreamState(desc, st); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// day reads one day of a report for one property, paging with offset and
// limit until rowCount rows are read.
func (r *reader) day(ctx context.Context, d reportDef, desc synclet.StreamDescriptor, prop, day string) error {
	req := reportRequest{
		DateRanges:          []dateRange{{StartDate: day, EndDate: day}},
		Limit:               r.limit,
		ReturnPropertyQuota: true,
	}
	for _, n := range d.dimensions {
		req.Dimensions = append(req.Dimensions, nameObj{Name: n})
	}
	for _, n := range d.metrics {
		req.Metrics = append(req.Metrics, nameObj{Name: n})
	}
	total := -1
	warned := false
	for {
		if err := r.pace(ctx, prop, day); err != nil {
			return err
		}
		resp, err := r.c.runReport(ctx, prop, req)
		if err != nil {
			var ae *apiError
			if errors.As(err, &ae) && ae.dailyQuota() {
				return fmt.Errorf("daily token quota of property %s is exhausted; the next sync continues from the saved state (%w)", prop, err)
			}
			return err
		}
		r.account(prop, day, resp)
		if total < 0 {
			total = resp.RowCount
		} else if resp.RowCount != total {
			return fmt.Errorf("rowCount changed from %d to %d at offset %d", total, resp.RowCount, req.Offset)
		}
		if len(resp.Rows) > 0 {
			if err := d.checkHeaders(r.set.kinds, resp); err != nil {
				return err
			}
		}
		q, err := responseQuality(resp)
		if err != nil {
			return err
		}
		if q.any() && !warned {
			warned = true
			r.log.Warn("report data may be incomplete", "property", prop, "report", d.name, "day", day,
				"sampled", q.sampled, "sampling_ratio", q.ratio, "subject_to_thresholding", q.thresholded,
				"data_loss_from_other_row", q.dataLoss)
		}
		var qp *quality
		if r.cfg.dataQuality() {
			qp = &q
		}
		for i, row := range resp.Rows {
			rec, err := d.record(r.set.kinds, prop, day, row.DimensionValues, row.MetricValues, qp)
			if err != nil {
				return fmt.Errorf("row %d: %w", req.Offset+i, err)
			}
			if err := r.em.Record(desc, rec); err != nil {
				return err
			}
		}
		req.Offset += len(resp.Rows)
		switch {
		case req.Offset == total:
			return nil
		case req.Offset > total:
			return fmt.Errorf("read %d rows, rowCount is %d", req.Offset, total)
		case len(resp.Rows) == 0:
			return fmt.Errorf("empty page at offset %d of %d rows", req.Offset, total)
		}
	}
}

// pace runs before each request of prop: it fails when the property's daily
// tokens are exhausted, and waits for the next full hour when its hourly
// tokens are below what the last request consumed, at most maxHourWaits
// times in a row.
func (r *reader) pace(ctx context.Context, prop, day string) error {
	q := r.quota[prop]
	if q == nil {
		return nil
	}
	if q.dayOut != "" {
		return fmt.Errorf("daily token quota of property %s is exhausted (last request for %s); the next sync continues from the saved state", prop, q.dayOut)
	}
	if !q.hourLow {
		return nil
	}
	if q.hourWaits == maxHourWaits {
		return fmt.Errorf("hourly token quota of property %s is still exhausted after waiting %d hours", prop, maxHourWaits)
	}
	q.hourWaits++
	q.hourLow = false
	now := r.src.clock()
	d := now.Truncate(time.Hour).Add(time.Hour).Sub(now)
	r.log.Warn("hourly token quota of the property is low, waiting for the next hour", "property", prop, "day", day, "wait", d.String())
	return r.c.wait(ctx, d)
}

// account records the quota a response reports.
func (r *reader) account(prop, day string, resp reportResponse) {
	q := r.quota[prop]
	if q == nil {
		q = &propertyQuota{}
		r.quota[prop] = q
	}
	pq := resp.PropertyQuota
	if pq == nil {
		return
	}
	if h := pq.TokensPerHour; h != nil && h.Remaining < h.Consumed {
		q.hourLow = true
	} else {
		q.hourWaits = 0
	}
	if dq := pq.TokensPerDay; dq != nil && dq.Remaining < dq.Consumed {
		q.dayOut = day
	}
}
