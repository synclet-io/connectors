package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
	"github.com/synclet-io/synclet-go-sdk/testkit"
)

func runRead(t *testing.T, src Source, cfg json.RawMessage, catalog *synclet.ConfiguredCatalog, state json.RawMessage) *testkit.Output {
	t.Helper()
	raw, err := json.Marshal(catalog)
	require.NoError(t, err)
	out, err := testkit.RunSource(t.Context(), src, synclet.Request{
		Command: synclet.CommandRead, Config: cfg, Catalog: raw, State: state,
	})
	require.NoError(t, err)
	return out
}

// records decodes the records of stream with numbers kept as json.Number.
func records(t *testing.T, out *testkit.Output, stream string) []map[string]any {
	t.Helper()
	var recs []map[string]any
	for _, r := range out.Records(synclet.StreamDescriptor{Name: stream, Namespace: namespace}) {
		dec := json.NewDecoder(strings.NewReader(string(r.Data)))
		dec.UseNumber()
		var m map[string]any
		require.NoError(t, dec.Decode(&m))
		recs = append(recs, m)
	}
	return recs
}

func streamStates(t *testing.T, out *testkit.Output) []string {
	t.Helper()
	var states []string
	for _, s := range out.States() {
		require.NotNil(t, s.Stream)
		states = append(states, string(s.Stream.StreamState))
	}
	return states
}

func warnings(out *testkit.Output) []string {
	var ws []string
	for _, l := range out.Logs() {
		if l.Level == synclet.LogLevelWarn {
			ws = append(ws, l.Message)
		}
	}
	return ws
}

// oneDay reads only site A on 2026-10-14.
func oneDay(c *Config) {
	c.SiteURLs = []string{testSiteA}
	c.StartDate, c.EndDate = "2026-10-14", "2026-10-14"
}

// webQueries is a custom report of web queries, so a test reads one search
// type.
var webQueries = CustomReport{Name: "web_queries", Dimensions: []string{"query"}}

func withWebQueries(c *Config) { oneDay(c); c.CustomReports = []CustomReport{webQueries} }

// startRows returns the startRow of each query of site for type and
// dimensions.
func startRows(h *hits, site, searchType string, dims ...string) []int {
	var out []int
	for _, q := range h.queries(site) {
		if q.Type == searchType && strings.Join(q.Dimensions, ",") == strings.Join(dims, ",") {
			out = append(out, q.StartRow)
		}
	}
	return out
}

func TestReadPaging(t *testing.T) {
	srv, h := fixtureServer(t, nil)
	src, _ := testSource(srv)
	src.pageLimit = 2
	cfg := testConfig(t, oneDay)
	out := runRead(t, src, cfg, configured(t, src, cfg, synclet.SyncModeFullRefresh, "search_analytics_by_page"), nil)
	require.NoError(t, out.Err)

	for _, q := range h.queries(testSiteA) {
		assert.Equal(t, 2, q.RowLimit)
		assert.Equal(t, "2026-10-14", q.StartDate)
		assert.Equal(t, "2026-10-14", q.EndDate)
		assert.Equal(t, []string{"date", "page"}, q.Dimensions)
		assert.Equal(t, "final", q.DataState)
	}
	assert.Equal(t, []int{0, 2, 4}, startRows(h, testSiteA, "web", "date", "page"), "pages of 2, 2 and 1 rows")
	for _, st := range []string{"image", "video", "news"} {
		assert.Equal(t, []int{0}, startRows(h, testSiteA, st, "date", "page"), "a day without rows of %s is one request", st)
	}

	recs := records(t, out, "search_analytics_by_page")
	require.Len(t, recs, 5)
	var pages []string
	for _, r := range recs {
		pages = append(pages, r["page"].(string))
		assert.Equal(t, "2026-10-14", r["date"])
		assert.Equal(t, testSiteA, r["site_url"])
		assert.Equal(t, "web", r["search_type"])
	}
	assert.Equal(t, []string{
		"https://www.example.com/", "https://www.example.com/pricing", "https://www.example.com/blog/widgets",
		"https://www.example.com/blog/repair", "https://www.example.com/signup",
	}, pages)
	assert.Empty(t, out.States(), "a full refresh emits no state")
}

func TestReadPagingExact(t *testing.T) {
	srv, h := fixtureServer(t, nil)
	src, _ := testSource(srv)
	src.pageLimit = 2
	cfg := testConfig(t, withWebQueries)
	out := runRead(t, src, cfg, configured(t, src, cfg, synclet.SyncModeFullRefresh, webQueries.Name), nil)
	require.NoError(t, out.Err)
	assert.Equal(t, []int{0, 2, 4}, startRows(h, testSiteA, "web", "date", "query"), "pages of 2, 2 and 0 rows")
	assert.Len(t, records(t, out, webQueries.Name), 4)
}

func TestReadInconsistent(t *testing.T) {
	tests := []struct {
		name  string
		pages []fixture
		err   string
	}{
		{
			name:  "more rows than rowLimit",
			pages: []fixture{{file: "responses/oversized"}},
			err:   "page at startRow 0 has 3 rows, rowLimit is 2",
		},
		{
			name:  "row of another day",
			pages: []fixture{{file: "responses/wrong_date"}},
			err:   "row 1: row of date 2026-10-13 in the report of 2026-10-14",
		},
		{
			name:  "duplicate keys within a page",
			pages: []fixture{{file: "responses/duplicate_keys"}},
			err:   `row 1 repeats the keys ["2026-10-14" "blue widgets"] of row 0`,
		},
		{
			name:  "duplicate keys across pages",
			pages: []fixture{{}, {file: "responses/repeat_page"}},
			err:   `row 2 repeats the keys ["2026-10-14" "blue widgets"] of row 0`,
		},
		{
			name:  "fractional clicks",
			pages: []fixture{{file: "responses/fractional_clicks"}},
			err:   `row 0: clicks "1.5" is not an integer`,
		},
		{
			name:  "keys other than the dimensions",
			pages: []fixture{{file: "responses/keys_mismatch"}},
			err:   "row 0: row has 1 keys, requested 2 dimensions [date query]",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv, _ := fixtureServer(t, map[string][]fixture{queryKey(testSiteA): tt.pages})
			src, _ := testSource(srv)
			src.pageLimit = 2
			cfg := testConfig(t, withWebQueries)
			out := runRead(t, src, cfg, configured(t, src, cfg, synclet.SyncModeIncremental, webQueries.Name), nil)
			require.Error(t, out.Err)
			assert.Equal(t, "web_queries: site https://www.example.com/, 2026-10-14, search type web: "+tt.err, out.Err.Error())
			assert.Empty(t, out.States(), "no state for a day that failed")
		})
	}
}

// generatedPage is a page of n distinct rows of web_queries on 2026-10-14,
// numbered from first.
func generatedPage(t *testing.T, first, n int) []byte {
	t.Helper()
	rows := make([]map[string]any, n)
	for i := range rows {
		rows[i] = map[string]any{
			"keys":   []string{"2026-10-14", fmt.Sprintf("synthetic query %d", first+i)},
			"clicks": 1, "impressions": 10, "ctr": 0.1, "position": 7.5,
		}
	}
	raw, err := json.Marshal(map[string]any{"rows": rows})
	require.NoError(t, err)
	return raw
}

func TestReadCapWarn(t *testing.T) {
	srv, h := fixtureServer(t, map[string][]fixture{queryKey(testSiteA): {
		{body: generatedPage(t, 0, defaultPageLimit)},
		{body: generatedPage(t, defaultPageLimit, defaultPageLimit)},
		{file: "responses/empty"},
	}})
	src, _ := testSource(srv)
	cfg := testConfig(t, withWebQueries)
	out := runRead(t, src, cfg, configured(t, src, cfg, synclet.SyncModeFullRefresh, webQueries.Name), nil)
	require.NoError(t, out.Err)
	assert.Equal(t, []int{0, 25000, 50000}, startRows(h, testSiteA, "web", "date", "query"))
	assert.Len(t, out.Records(synclet.StreamDescriptor{Name: webQueries.Name, Namespace: namespace}), rowCap)
	assert.Equal(t, []string{
		"day hit the 50,000-row cap of Search Console; rows past it are not available site=https://www.example.com/ report=web_queries day=2026-10-14 search_type=web rows=50000",
	}, warnings(out))
}

func TestReadBelowCapNoWarn(t *testing.T) {
	srv, _ := fixtureServer(t, map[string][]fixture{queryKey(testSiteA): {
		{body: generatedPage(t, 0, defaultPageLimit)},
		{body: generatedPage(t, defaultPageLimit, defaultPageLimit-1)},
	}})
	src, _ := testSource(srv)
	cfg := testConfig(t, withWebQueries)
	out := runRead(t, src, cfg, configured(t, src, cfg, synclet.SyncModeFullRefresh, webQueries.Name), nil)
	require.NoError(t, out.Err)
	assert.Empty(t, warnings(out))
}

func TestReadConversion(t *testing.T) {
	srv, h := fixtureServer(t, nil)
	src, _ := testSource(srv)
	cfg := testConfig(t, func(c *Config) { c.StartDate, c.EndDate, c.DataState = "2026-10-14", "2026-10-14", dataStateAll })
	out := runRead(t, src, cfg, configured(t, src, cfg, synclet.SyncModeFullRefresh, "search_analytics_by_query"), nil)
	require.NoError(t, out.Err)
	recs := records(t, out, "search_analytics_by_query")
	require.Len(t, recs, 4)
	assert.Equal(t, map[string]any{
		"site_url": testSiteA, "search_type": "web", "date": "2026-10-14", "query": "blue widgets",
		"clicks": json.Number("13"), "impressions": json.Number("89"),
		"ctr": json.Number("0.1460674157303371"), "position": json.Number("2.92857142857143"),
	}, recs[0], "ctr and position as returned")
	for _, q := range append(h.queries(testSiteA), h.queries(testSiteB)...) {
		assert.Equal(t, "all", q.DataState)
	}
	assert.Contains(t, h.keys(), "POST sites/sc-domain%3Aexample.org/searchAnalytics/query")
	assert.Contains(t, h.keys(), "POST sites/https%3A%2F%2Fwww.example.com%2F/searchAnalytics/query")
}

func TestReadWholeNumberFloats(t *testing.T) {
	srv, _ := fixtureServer(t, map[string][]fixture{queryKey(testSiteB): {{file: "responses/whole_number_floats"}}})
	src, _ := testSource(srv)
	cfg := testConfig(t, func(c *Config) {
		c.SiteURLs = []string{testSiteB}
		c.StartDate, c.EndDate = "2026-10-14", "2026-10-14"
		c.CustomReports = []CustomReport{{Name: "web_days"}}
	})
	out := runRead(t, src, cfg, configured(t, src, cfg, synclet.SyncModeFullRefresh, "web_days"), nil)
	require.NoError(t, out.Err)
	assert.Equal(t, []map[string]any{{
		"site_url": testSiteB, "search_type": "web", "date": "2026-10-14",
		"clicks": json.Number("2"), "impressions": json.Number("40"), "ctr": json.Number("0.05"), "position": json.Number("3.25"),
	}}, records(t, out, "web_days"))
}

func TestReadSearchTypes(t *testing.T) {
	srv, h := fixtureServer(t, nil)
	src, _ := testSource(srv)
	cfg := testConfig(t, func(c *Config) { oneDay(c); withDiscoverPages(c) })
	out := runRead(t, src, cfg, configured(t, src, cfg, synclet.SyncModeFullRefresh, "search_analytics_by_date", discoverPages.Name), nil)
	require.NoError(t, out.Err)
	var types []string
	for _, q := range h.queries(testSiteA) {
		types = append(types, q.Type)
	}
	assert.Equal(t, []string{"web", "image", "video", "news", "discover"}, types)
	byType := map[string]int{}
	for _, r := range records(t, out, "search_analytics_by_date") {
		byType[r["search_type"].(string)]++
	}
	assert.Equal(t, map[string]int{"web": 1, "image": 1}, byType)
	recs := records(t, out, discoverPages.Name)
	require.Len(t, recs, 2)
	assert.Equal(t, "discover", recs[0]["search_type"])
}

func TestReadSitesAndSitemaps(t *testing.T) {
	srv, h := fixtureServer(t, nil)
	src, _ := testSource(srv)
	cfg := testConfig(t, nil)
	out := runRead(t, src, cfg, configured(t, src, cfg, synclet.SyncModeIncremental, streamSites, streamSitemaps), nil)
	require.NoError(t, out.Err)
	assert.Equal(t, []map[string]any{
		{"site_url": testSiteA, "permission_level": "siteOwner"},
		{"site_url": testSiteB, "permission_level": "siteFullUser"},
	}, records(t, out, streamSites))
	sm := records(t, out, streamSitemaps)
	require.Len(t, sm, 2)
	assert.Equal(t, map[string]any{
		"site_url": testSiteA, "path": "https://www.example.com/sitemap.xml",
		"last_submitted": "2026-10-01T08:15:00.000Z", "last_downloaded": "2026-10-15T03:20:11.482Z",
		"is_pending": false, "is_sitemaps_index": true, "type": "sitemap",
		"warnings": json.Number("0"), "errors": json.Number("0"),
		"contents": []any{
			map[string]any{"type": "web", "submitted": json.Number("1240"), "indexed": json.Number("0")},
			map[string]any{"type": "image", "submitted": json.Number("310"), "indexed": nil},
		},
	}, sm[0])
	assert.Equal(t, map[string]any{
		"site_url": testSiteA, "path": "https://www.example.com/news.xml",
		"last_submitted": "2026-09-20T10:00:00.000Z", "last_downloaded": nil,
		"is_pending": true, "is_sitemaps_index": false, "type": "sitemap",
		"warnings": json.Number("2"), "errors": json.Number("1"), "contents": nil,
	}, sm[1])
	assert.Empty(t, out.States(), "sites and sitemaps are full refresh")
	assert.Equal(t, []string{
		"POST token",
		"GET sites/https%3A%2F%2Fwww.example.com%2F", "GET sites/sc-domain%3Aexample.org",
		"GET sites/https%3A%2F%2Fwww.example.com%2F/sitemaps", "GET sites/sc-domain%3Aexample.org/sitemaps",
	}, h.keys())
}

func TestReadQuota(t *testing.T) {
	rate := fixture{status: http.StatusTooManyRequests, file: "errors/quota_rate"}
	tests := []struct {
		name      string
		overrides []fixture
		waits     []time.Duration
		err       string
		requests  int
	}{
		{
			name:      "429 retries with backoff",
			overrides: []fixture{rate, {}},
			waits:     []time.Duration{time.Second + time.Millisecond},
			requests:  2,
		},
		{
			name:      "429 twice doubles the delay",
			overrides: []fixture{rate, rate, {}},
			waits:     []time.Duration{time.Second + time.Millisecond, 2*time.Second + time.Millisecond},
			requests:  3,
		},
		{
			name:      "5xx retries with backoff",
			overrides: []fixture{{status: http.StatusInternalServerError, file: "errors/internal"}, {}},
			waits:     []time.Duration{time.Second + time.Millisecond},
			requests:  2,
		},
		{
			name:      "5xx five times fails",
			overrides: []fixture{{status: http.StatusInternalServerError, file: "errors/internal"}},
			waits:     []time.Duration{time.Second + time.Millisecond, 2*time.Second + time.Millisecond, 4*time.Second + time.Millisecond, 8*time.Second + time.Millisecond},
			err:       "web_queries: site https://www.example.com/, 2026-10-14, search type web: sites/https%3A%2F%2Fwww.example.com%2F/searchAnalytics/query: HTTP 500 INTERNAL: Internal error encountered.",
			requests:  5,
		},
		{
			name:      "dailyLimitExceeded fails at once",
			overrides: []fixture{{status: http.StatusForbidden, file: "errors/quota_daily"}},
			err:       "web_queries: site https://www.example.com/, 2026-10-14, search type web: daily quota of Search Console API is exhausted; the next sync continues from the saved state (sites/https%3A%2F%2Fwww.example.com%2F/searchAnalytics/query: HTTP 403 dailyLimitExceeded",
			requests:  1,
		},
		{
			name:      "429 per day fails at once",
			overrides: []fixture{{status: http.StatusTooManyRequests, file: "errors/quota_daily_429"}},
			err:       "web_queries: site https://www.example.com/, 2026-10-14, search type web: daily quota of Search Console API is exhausted; the next sync continues from the saved state (sites/https%3A%2F%2Fwww.example.com%2F/searchAnalytics/query: HTTP 429 RESOURCE_EXHAUSTED",
			requests:  1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv, h := fixtureServer(t, map[string][]fixture{queryKey(testSiteA): tt.overrides})
			src, w := testSource(srv)
			cfg := testConfig(t, withWebQueries)
			out := runRead(t, src, cfg, configured(t, src, cfg, synclet.SyncModeIncremental, webQueries.Name), nil)
			if tt.err == "" {
				require.NoError(t, out.Err)
				assert.Len(t, records(t, out, webQueries.Name), 4)
			} else {
				require.Error(t, out.Err)
				assert.True(t, strings.HasPrefix(out.Err.Error(), tt.err), out.Err.Error())
				assert.Empty(t, out.States())
			}
			assert.Equal(t, tt.waits, w.list())
			assert.Len(t, h.queries(testSiteA), tt.requests)
		})
	}
}

func TestPacer(t *testing.T) {
	now := testNow
	var waited []time.Duration
	p := &pacer{
		interval: paceInterval,
		now:      func() time.Time { return now },
		wait: func(_ context.Context, d time.Duration) error {
			waited = append(waited, d)
			now = now.Add(d)
			return nil
		},
	}
	ctx := t.Context()
	require.NoError(t, p.next(ctx))
	require.NoError(t, p.next(ctx))
	now = now.Add(20 * time.Millisecond)
	require.NoError(t, p.next(ctx))
	now = now.Add(time.Second)
	require.NoError(t, p.next(ctx))
	assert.Equal(t, []time.Duration{50 * time.Millisecond, 30 * time.Millisecond}, waited)
}

func TestReadPaces(t *testing.T) {
	srv, h := fixtureServer(t, nil)
	src, _ := testSource(srv)
	var paced []time.Duration
	src.paceWait = func(_ context.Context, d time.Duration) error { paced = append(paced, d); return nil }
	cfg := testConfig(t, withWebQueries)
	out := runRead(t, src, cfg, configured(t, src, cfg, synclet.SyncModeFullRefresh, streamSites, webQueries.Name), nil)
	require.NoError(t, out.Err)
	require.Len(t, h.keys(), 3, "token, sites.get and one query")
	assert.Equal(t, []time.Duration{paceInterval}, paced, "the fixed test clock makes every request after the first wait the whole interval")
}

func TestReadRefreshesExpiredToken(t *testing.T) {
	srv, h := fixtureServer(t, map[string][]fixture{queryKey(testSiteA): {{status: http.StatusUnauthorized, file: "errors/unauthenticated"}, {}}})
	src, _ := testSource(srv)
	cfg := testConfig(t, withWebQueries)
	out := runRead(t, src, cfg, configured(t, src, cfg, synclet.SyncModeFullRefresh, webQueries.Name), nil)
	require.NoError(t, out.Err)
	assert.Len(t, records(t, out, webQueries.Name), 4)
	q := "POST sites/https%3A%2F%2Fwww.example.com%2F/searchAnalytics/query"
	assert.Equal(t, []string{"POST token", q, "POST token", q}, h.keys())
}

func TestReadSiteForbidden(t *testing.T) {
	srv, _ := fixtureServer(t, map[string][]fixture{queryKey(testSiteA): {{status: http.StatusForbidden, file: "errors/forbidden_site"}}})
	src, _ := testSource(srv)
	cfg := testConfig(t, withWebQueries)
	out := runRead(t, src, cfg, configured(t, src, cfg, synclet.SyncModeFullRefresh, webQueries.Name), nil)
	require.Error(t, out.Err)
	assert.Contains(t, out.Err.Error(), "site https://www.example.com/ is not verified for service account "+testClientEmail)
	require.Len(t, out.ErrorTraces(), 1)
	assert.Equal(t, synclet.FailureTypeConfigError, out.ErrorTraces()[0].FailureType)
}

func TestReadUnknownStream(t *testing.T) {
	srv, _ := fixtureServer(t, nil)
	src, _ := testSource(srv)
	cfg := testConfig(t, nil)
	cat := configured(t, src, cfg, synclet.SyncModeFullRefresh, "search_analytics_by_page")
	cat.Streams[0].Stream.Name = "search_analytics_by_page_v2"
	out := runRead(t, src, cfg, cat, nil)
	require.Error(t, out.Err)
	assert.Equal(t, `unknown stream "search_analytics_by_page_v2"`, out.Err.Error())
}

func TestReadAllStreams(t *testing.T) {
	srv, _ := fixtureServer(t, nil)
	src, _ := testSource(srv)
	cfg := testConfig(t, withDiscoverPages)
	out := runRead(t, src, cfg, configured(t, src, cfg, synclet.SyncModeFullRefresh, allNames()...), nil)
	require.NoError(t, out.Err)
	for _, name := range allNames() {
		sites := map[string]int{}
		for _, r := range records(t, out, name) {
			sites[r["site_url"].(string)]++
		}
		assert.Positive(t, sites[testSiteA], name)
		if name != streamSitemaps && name != discoverPages.Name {
			assert.Positive(t, sites[testSiteB], name)
		}
	}
}
