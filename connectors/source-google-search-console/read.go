package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

const (
	// defaultPageLimit is the most rows searchAnalytics.query returns in one
	// response.
	defaultPageLimit = 25000
	// rowCap is the most rows Search Console returns for one site, day and
	// search type; rows past it are not available.
	rowCap = 50000
)

// reportState is the STREAM state of a search analytics stream: per site,
// the last day read.
type reportState struct {
	Sites map[string]siteState `json:"sites"`
}

type siteState struct {
	Date string `json:"date"`
}

func loadState(desc synclet.StreamDescriptor, state *synclet.InputState) (reportState, error) {
	st := reportState{Sites: map[string]siteState{}}
	raw := state.Stream(desc)
	if raw == nil {
		return st, nil
	}
	var saved reportState
	if err := json.Unmarshal(raw, &saved); err != nil {
		return st, synclet.ConfigError(fmt.Errorf("state of %s: %w", desc.Name, err))
	}
	for u, s := range saved.Sites {
		st.Sites[u] = s
	}
	return st, nil
}

// dailyRange returns the days to read for one site: from the saved date
// minus lookback_days, never before the start, or from the start without a
// saved date; up to end_date, else today.
func dailyRange(cfg Config, saved string, today time.Time) (since, until time.Time, err error) {
	if since, err = cfg.start(today); err != nil {
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

// reader reads the streams of one sync, one site, day and search type at a
// time.
type reader struct {
	src   Source
	c     *apiClient
	cfg   Config
	em    *synclet.Emitter
	log   *slog.Logger
	limit int
}

// Read emits every configured stream in catalog order. A search analytics
// stream reads every site day by day; an incremental read emits its state
// after each site-day, and that state never goes below the saved date.
func (s Source) Read(ctx context.Context, cfg Config, catalog *synclet.ConfiguredCatalog, state *synclet.InputState, em *synclet.Emitter) error {
	if err := cfg.validate(); err != nil {
		return synclet.ConfigError(err)
	}
	defs, err := reportDefs(cfg)
	if err != nil {
		return synclet.ConfigError(err)
	}
	c, err := s.apiClient(ctx, cfg)
	if err != nil {
		return err
	}
	r := &reader{src: s, c: c, cfg: cfg, em: em, log: synclet.Logger(ctx), limit: s.pageLimit}
	if r.limit <= 0 {
		r.limit = defaultPageLimit
	}
	byName := map[string]reportDef{}
	for _, d := range defs {
		byName[d.name] = d
	}
	for _, cs := range catalog.Streams {
		switch name := cs.Stream.Name; name {
		case streamSites:
			err = r.sites(ctx)
		case streamSitemaps:
			err = r.sitemaps(ctx)
		default:
			d, ok := byName[name]
			if !ok {
				return synclet.ConfigError(fmt.Errorf("unknown stream %q", name))
			}
			err = r.stream(ctx, d, cs, state)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *reader) sites(ctx context.Context) error {
	desc := synclet.StreamDescriptor{Name: streamSites, Namespace: namespace}
	for _, u := range r.cfg.SiteURLs {
		st, err := r.c.site(ctx, u)
		if err != nil {
			return fmt.Errorf("%s: site %s: %w", streamSites, u, err)
		}
		if st.SiteURL == "" {
			st.SiteURL = u
		}
		if err := r.em.Record(desc, map[string]any{keySiteURL: st.SiteURL, "permission_level": st.PermissionLevel}); err != nil {
			return err
		}
	}
	return nil
}

// optionalInt converts an int64 the API sends as a string, nil when absent.
func optionalInt(field string, n json.Number) (any, error) {
	if n == "" {
		return nil, nil
	}
	return integral(field, n)
}

func (r *reader) sitemaps(ctx context.Context) error {
	desc := synclet.StreamDescriptor{Name: streamSitemaps, Namespace: namespace}
	for _, u := range r.cfg.SiteURLs {
		maps, err := r.c.sitemaps(ctx, u)
		if err != nil {
			return fmt.Errorf("%s: site %s: %w", streamSitemaps, u, err)
		}
		for _, m := range maps {
			rec, err := sitemapRecord(u, m)
			if err != nil {
				return fmt.Errorf("%s: site %s, sitemap %s: %w", streamSitemaps, u, m.Path, err)
			}
			if err := r.em.Record(desc, rec); err != nil {
				return err
			}
		}
	}
	return nil
}

func sitemapRecord(site string, m sitemap) (map[string]any, error) {
	rec := map[string]any{
		keySiteURL:          site,
		"path":              m.Path,
		"last_submitted":    m.LastSubmitted,
		"last_downloaded":   m.LastDownloaded,
		"is_pending":        m.IsPending,
		"is_sitemaps_index": m.IsSitemapsIndex,
		"type":              m.Type,
	}
	var err error
	if rec["warnings"], err = optionalInt("warnings", m.Warnings); err != nil {
		return nil, err
	}
	if rec["errors"], err = optionalInt("errors", m.Errors); err != nil {
		return nil, err
	}
	if m.Contents == nil {
		rec["contents"] = nil
		return rec, nil
	}
	contents := make([]map[string]any, 0, len(m.Contents))
	for _, c := range m.Contents {
		item := map[string]any{"type": c.Type}
		if item["submitted"], err = optionalInt("contents.submitted", c.Submitted); err != nil {
			return nil, err
		}
		if item["indexed"], err = optionalInt("contents.indexed", c.Indexed); err != nil {
			return nil, err
		}
		contents = append(contents, item)
	}
	rec["contents"] = contents
	return rec, nil
}

func (r *reader) stream(ctx context.Context, d reportDef, cs synclet.ConfiguredStream, state *synclet.InputState) error {
	desc := synclet.StreamDescriptor{Name: d.name, Namespace: namespace}
	incremental := cs.SyncMode == synclet.SyncModeIncremental
	st := reportState{Sites: map[string]siteState{}}
	if incremental {
		var err error
		if st, err = loadState(desc, state); err != nil {
			return err
		}
	}
	today := r.src.today()
	for _, u := range r.cfg.SiteURLs {
		since, until, err := dailyRange(r.cfg, st.Sites[u].Date, today)
		if err != nil {
			return synclet.ConfigError(fmt.Errorf("%s: site %s: %w", d.name, u, err))
		}
		for day := since; !day.After(until); day = day.AddDate(0, 0, 1) {
			ds := day.Format(dateLayout)
			for _, t := range d.searchTypes {
				if err := r.day(ctx, d, desc, u, ds, t); err != nil {
					return fmt.Errorf("%s: site %s, %s, search type %s: %w", d.name, u, ds, t, err)
				}
			}
			if incremental {
				if ds > st.Sites[u].Date {
					st.Sites[u] = siteState{Date: ds}
				}
				if err := r.em.StreamState(desc, st); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// day reads one day of a report for one site and search type, paging with
// startRow until a page shorter than rowLimit. A page is checked whole
// before any of its rows is emitted.
func (r *reader) day(ctx context.Context, d reportDef, desc synclet.StreamDescriptor, site, day, searchType string) error {
	req := analyticsRequest{
		StartDate:  day,
		EndDate:    day,
		Dimensions: d.dimensions,
		Type:       searchType,
		DataState:  r.cfg.dataState(),
		RowLimit:   r.limit,
	}
	seen := map[string]int{}
	for {
		resp, err := r.c.query(ctx, site, req)
		if err != nil {
			return err
		}
		if len(resp.Rows) > r.limit {
			return fmt.Errorf("page at startRow %d has %d rows, rowLimit is %d", req.StartRow, len(resp.Rows), r.limit)
		}
		recs := make([]map[string]any, 0, len(resp.Rows))
		for i, row := range resp.Rows {
			n := req.StartRow + i
			rec, err := d.record(site, searchType, day, row)
			if err != nil {
				return fmt.Errorf("row %d: %w", n, err)
			}
			k := strings.Join(row.Keys, "\x00")
			if first, ok := seen[k]; ok {
				return fmt.Errorf("row %d repeats the keys %q of row %d", n, row.Keys, first)
			}
			seen[k] = n
			recs = append(recs, rec)
		}
		for _, rec := range recs {
			if err := r.em.Record(desc, rec); err != nil {
				return err
			}
		}
		req.StartRow += len(resp.Rows)
		if len(resp.Rows) < r.limit {
			break
		}
	}
	if req.StartRow >= rowCap {
		r.log.Warn("day hit the 50,000-row cap of Search Console; rows past it are not available",
			"site", site, "report", d.name, "day", day, "search_type", searchType, "rows", req.StartRow)
	}
	return nil
}
