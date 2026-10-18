package main

import (
	"encoding/json"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
)

var (
	statusRunning   = fixture{file: "async/running"}
	statusDone      = fixture{file: "async/completed"}
	statusJobFailed = fixture{file: "async/failed"}
	statusJobSkip   = fixture{file: "async/skipped"}
)

func onlyAccountA(c *Config) { c.AccountIDs = []string{testAccountA} }

func requestsOf(h *hits, method, path string) []apiRequest {
	var out []apiRequest
	for _, r := range h.list() {
		if r.method == method && r.path == path {
			out = append(out, r)
		}
	}
	return out
}

func dates(recs []map[string]any) []string {
	var ds []string
	for _, r := range recs {
		ds = append(ds, r["date_start"].(string))
	}
	slices.Sort(ds)
	return slices.Compact(ds)
}

func TestReadInsights(t *testing.T) {
	runA := "run-" + testAccountA + "-ad"
	insightsA := "act_" + testAccountA + "/insights"

	t.Run("job polled until completed, results paged", func(t *testing.T) {
		srv, h := fixtureServer(t, map[string][]fixture{"GET " + runA: {statusRunning, statusRunning, statusDone}})
		src, w := testSource(srv)
		out := runRead(t, src, testConfig(t, onlyAccountA), configured(t, src, synclet.SyncModeFullRefresh, streamAdInsights), nil)
		require.NoError(t, out.Err)

		posts := requestsOf(h, http.MethodPost, insightsA)
		require.Len(t, posts, 1)
		form := posts[0].form
		assert.Equal(t, "ad", form.Get("level"))
		assert.Equal(t, "1", form.Get("time_increment"))
		assert.JSONEq(t, `{"since":"2026-10-14","until":"2026-10-18"}`, form.Get("time_range"))
		assert.False(t, form.Has("breakdowns"))
		assert.Contains(t, strings.Split(form.Get("fields"), ","), "spend")
		assert.NotContains(t, strings.Split(form.Get("fields"), ","), "date_start")

		assert.Len(t, requestsOf(h, http.MethodGet, runA), 3)
		assert.Equal(t, []time.Duration{2 * time.Second, 2 * time.Second}, w.list(), "polls wait pollInterval")
		pages := requestsOf(h, http.MethodGet, runA+"/insights")
		require.Len(t, pages, 2)
		assert.Equal(t, "500", pages[0].query.Get("limit"))
		assert.Equal(t, "p2", pages[1].query.Get("after"))

		recs := records(t, out, streamAdInsights)
		assert.Len(t, recs, 8, "two ads on four days; rows of 2026-10-13 and 2026-10-19 are outside the range")
		assert.Equal(t, []string{"2026-10-14", "2026-10-15", "2026-10-16", "2026-10-17"}, dates(recs))
		first := recs[0]
		assert.Equal(t, 20.5, first["spend"])
		assert.Equal(t, 200.0, first["impressions"])
		assert.Equal(t, testAccountA, first["account_id"])
		assert.NotContains(t, first, "date_stop", "only schema fields are emitted")
		assert.IsType(t, []any{}, first["actions"])
	})

	for name, status := range map[string]fixture{"failed": statusJobFailed, "skipped": statusJobSkip} {
		t.Run(name+" job is created again", func(t *testing.T) {
			srv, h := fixtureServer(t, map[string][]fixture{"GET " + runA: {status, statusDone}})
			src, _ := testSource(srv)
			out := runRead(t, src, testConfig(t, onlyAccountA), configured(t, src, synclet.SyncModeFullRefresh, streamAdInsights), nil)
			require.NoError(t, out.Err)
			assert.Len(t, requestsOf(h, http.MethodPost, insightsA), 2)
			assert.Len(t, records(t, out, streamAdInsights), 8)
		})
	}

	t.Run("job that keeps failing fails the stream naming account and range", func(t *testing.T) {
		srv, h := fixtureServer(t, map[string][]fixture{"GET " + runA: {statusJobFailed}})
		src, _ := testSource(srv)
		out := runRead(t, src, testConfig(t, onlyAccountA), configured(t, src, synclet.SyncModeFullRefresh, streamAdInsights), nil)
		require.Error(t, out.Err)
		assert.Contains(t, out.Err.Error(), "ad_insights: account act_1111111111 2026-10-14..2026-10-18")
		assert.Contains(t, out.Err.Error(), `"Job Failed" 4 times`)
		assert.Len(t, requestsOf(h, http.MethodPost, insightsA), 1+jobRetries)
		assert.Empty(t, records(t, out, streamAdInsights))
	})

	t.Run("breakdown stream", func(t *testing.T) {
		srv, h := fixtureServer(t, nil)
		src, _ := testSource(srv)
		out := runRead(t, src, testConfig(t, onlyAccountA), configured(t, src, synclet.SyncModeFullRefresh, streamAdInsightsAgeGender), nil)
		require.NoError(t, out.Err)
		posts := requestsOf(h, http.MethodPost, insightsA)
		require.Len(t, posts, 1)
		assert.Equal(t, "age,gender", posts[0].form.Get("breakdowns"))
		assert.NotContains(t, strings.Split(posts[0].form.Get("fields"), ","), "age")
		recs := records(t, out, streamAdInsightsAgeGender)
		assert.Len(t, recs, 16)
		assert.Equal(t, "25-34", recs[0]["age"])
		assert.Equal(t, "female", recs[0]["gender"])
	})

	t.Run("account level", func(t *testing.T) {
		srv, h := fixtureServer(t, nil)
		src, _ := testSource(srv)
		out := runRead(t, src, testConfig(t, nil), configured(t, src, synclet.SyncModeFullRefresh, streamAccountSpendDaily), nil)
		require.NoError(t, out.Err)
		assert.Equal(t, "account", requestsOf(h, http.MethodPost, insightsA)[0].form.Get("level"))
		recs := records(t, out, streamAccountSpendDaily)
		assert.Len(t, recs, 8, "four days of two accounts")
		assert.Equal(t, map[string]any{"account_id": testAccountA, "account_currency": "USD", "spend": 101.25, "date_start": "2026-10-14"}, recs[0])
	})
}

func day(s string) time.Time {
	d, err := time.Parse(dateLayout, s)
	if err != nil {
		panic(err)
	}
	return d
}

func TestInsightsLookback(t *testing.T) {
	lookback := func(n int) *int { return &n }
	tests := []struct {
		name         string
		cfg          Config
		saved        accountState
		since, until string
	}{
		{name: "no state reads from start_date", since: "2026-09-01", until: "2026-10-18"},
		{name: "default lookback of 28 days", saved: accountState{DateStart: "2026-10-10"}, since: "2026-09-12", until: "2026-10-18"},
		{name: "lookback clamped to start_date", saved: accountState{DateStart: "2026-09-20"}, since: "2026-09-01", until: "2026-10-18"},
		{name: "lookback 3", cfg: Config{LookbackDays: lookback(3)}, saved: accountState{DateStart: "2026-10-10"}, since: "2026-10-07", until: "2026-10-18"},
		{name: "lookback 0", cfg: Config{LookbackDays: lookback(0)}, saved: accountState{DateStart: "2026-10-10"}, since: "2026-10-10", until: "2026-10-18"},
		{name: "config end_date", cfg: Config{EndDate: "2026-10-05"}, since: "2026-09-01", until: "2026-10-05"},
		{name: "future end_date stops at today", cfg: Config{EndDate: "2026-12-01"}, since: "2026-09-01", until: "2026-10-18"},
		{
			name: "saved end_date re-syncs a past range", cfg: Config{EndDate: "2026-10-15"},
			saved: accountState{DateStart: "2026-09-29", EndDate: "2026-10-01"}, since: "2026-09-01", until: "2026-10-01",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := tt.cfg
			cfg.StartDate = "2026-09-01"
			since, until, err := insightsRange(cfg, tt.saved, day("2026-10-18"))
			require.NoError(t, err)
			assert.Equal(t, tt.since, since.Format(dateLayout))
			assert.Equal(t, tt.until, until.Format(dateLayout))
		})
	}

	t.Run("incremental read emits only rows in the range and saves every account", func(t *testing.T) {
		srv, h := fixtureServer(t, nil)
		src, _ := testSource(srv)
		state := json.RawMessage(`[{"type":"STREAM","stream":{"stream_descriptor":{"name":"ad_insights","namespace":"facebook_marketing"},` +
			`"stream_state":{"accounts":{"act_1111111111":{"date_start":"2026-10-16"}}}}}]`)
		cfg := testConfig(t, func(c *Config) { c.LookbackDays = lookback(1) })
		out := runRead(t, src, cfg, configured(t, src, synclet.SyncModeIncremental, streamAdInsights), state)
		require.NoError(t, out.Err)

		posts := requestsOf(h, http.MethodPost, "act_"+testAccountA+"/insights")
		require.Len(t, posts, 1)
		assert.JSONEq(t, `{"since":"2026-10-15","until":"2026-10-18"}`, posts[0].form.Get("time_range"))
		postsB := requestsOf(h, http.MethodPost, "act_"+testAccountB+"/insights")
		require.Len(t, postsB, 1)
		assert.JSONEq(t, `{"since":"2026-10-14","until":"2026-10-18"}`, postsB[0].form.Get("time_range"))

		var datesA []string
		for _, r := range records(t, out, streamAdInsights) {
			if r["account_id"] == testAccountA {
				datesA = append(datesA, r["date_start"].(string))
			}
		}
		slices.Sort(datesA)
		assert.Equal(t, []string{"2026-10-15", "2026-10-15", "2026-10-16", "2026-10-16", "2026-10-17", "2026-10-17"}, datesA)

		states := out.States()
		require.Len(t, states, 2, "one state per account")
		assert.JSONEq(t, `{"accounts":{"1111111111":{"date_start":"2026-10-17"}}}`, string(states[0].Stream.StreamState))
		assert.JSONEq(t, `{"accounts":{"1111111111":{"date_start":"2026-10-17"},"2222222222":{"date_start":"2026-10-16"}}}`,
			string(states[1].Stream.StreamState))
	})

	t.Run("saved end_date is used once", func(t *testing.T) {
		srv, h := fixtureServer(t, nil)
		src, _ := testSource(srv)
		state := json.RawMessage(`[{"type":"STREAM","stream":{"stream_descriptor":{"name":"account_spend_daily","namespace":"facebook_marketing"},` +
			`"stream_state":{"accounts":{"1111111111":{"date_start":"2026-10-15","end_date":"2026-10-15"}}}}}]`)
		cfg := testConfig(t, func(c *Config) { onlyAccountA(c); c.LookbackDays = lookback(0) })
		out := runRead(t, src, cfg, configured(t, src, synclet.SyncModeIncremental, streamAccountSpendDaily), state)
		require.NoError(t, out.Err)
		posts := requestsOf(h, http.MethodPost, "act_"+testAccountA+"/insights")
		require.Len(t, posts, 1)
		assert.JSONEq(t, `{"since":"2026-10-15","until":"2026-10-15"}`, posts[0].form.Get("time_range"))
		assert.Equal(t, []string{"2026-10-15"}, dates(records(t, out, streamAccountSpendDaily)))
		states := out.States()
		require.Len(t, states, 1)
		assert.JSONEq(t, `{"accounts":{"1111111111":{"date_start":"2026-10-15"}}}`, string(states[0].Stream.StreamState))
	})
}
