package main

import (
	"encoding/json"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
)

func onlyAdvertiserA(c *Config) { c.AdvertiserIDs = []string{testAdvertiserA} }

// windows returns the start_date..end_date of every report request of an
// advertiser, in order.
func windows(h *hits, adv string) []string {
	var out []string
	for _, r := range h.of(reportPath) {
		if r.query.Get("advertiser_id") == adv {
			out = append(out, r.query.Get("start_date")+".."+r.query.Get("end_date"))
		}
	}
	return out
}

func reportStateMsg(stream, state string) json.RawMessage {
	return json.RawMessage(fmt.Sprintf(`[{"type":"STREAM","stream":{"stream_descriptor":{"name":%q,"namespace":%q},"stream_state":%s}}]`,
		stream, namespace, state))
}

func cursors(recs []map[string]any) []string {
	var ds []string
	for _, r := range recs {
		ds = append(ds, r[cursorField].(string))
	}
	slices.Sort(ds)
	return slices.Compact(ds)
}

func TestReportIncremental(t *testing.T) {
	lookback := func(n int) *int { return &n }
	tests := []struct {
		name         string
		cfg          Config
		saved        string
		since, until string
	}{
		{name: "no state reads from start_date", since: "2026-08-01", until: "2026-10-18"},
		{name: "default lookback of 3 days", saved: "2026-10-10", since: "2026-10-07", until: "2026-10-18"},
		{name: "lookback clamped to start_date", saved: "2026-08-02", since: "2026-08-01", until: "2026-10-18"},
		{name: "lookback 0", cfg: Config{LookbackDays: lookback(0)}, saved: "2026-10-10", since: "2026-10-10", until: "2026-10-18"},
		{name: "lookback 10", cfg: Config{LookbackDays: lookback(10)}, saved: "2026-10-10", since: "2026-09-30", until: "2026-10-18"},
		{name: "config end_date", cfg: Config{EndDate: "2026-10-05"}, since: "2026-08-01", until: "2026-10-05"},
		{name: "future end_date stops at today", cfg: Config{EndDate: "2026-12-01"}, since: "2026-08-01", until: "2026-10-18"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := tt.cfg
			cfg.StartDate = testStartDate
			since, until, err := reportRange(cfg, advertiserState{StatTimeDay: tt.saved}, time.Date(2026, 10, 18, 0, 0, 0, 0, time.UTC))
			require.NoError(t, err)
			assert.Equal(t, tt.since, since.Format(dateLayout))
			assert.Equal(t, tt.until, until.Format(dateLayout))
		})
	}

	t.Run("first read walks 30-day windows from start_date and saves after each", func(t *testing.T) {
		srv, h := fixtureServer(t, nil)
		src, _ := testSource(srv)
		out := runRead(t, src, testConfig(t, onlyAdvertiserA), configured(t, src, synclet.SyncModeIncremental, streamAdsReportDaily), nil)
		require.NoError(t, out.Err)
		assert.Equal(t, []string{"2026-08-01..2026-08-30", "2026-08-31..2026-09-29", "2026-09-30..2026-10-18"}, windows(h, testAdvertiserA))
		req := h.of(reportPath)[0].query
		assert.Equal(t, "BASIC", req.Get("report_type"))
		assert.Equal(t, "AUCTION_AD", req.Get("data_level"))
		assert.JSONEq(t, `["ad_id","stat_time_day"]`, req.Get("dimensions"))

		states := out.States()
		require.Len(t, states, 3, "one state per window")
		for i, day := range []string{"2026-08-30", "2026-09-29", "2026-10-18"} {
			assert.JSONEq(t, `{"advertisers":{"1000000001":{"stat_time_day":"`+day+`"}}}`, string(states[i].Stream.StreamState))
		}

		recs := records(t, out, streamAdsReportDaily)
		assert.Equal(t, []string{"2026-08-01", "2026-08-15", "2026-08-30", "2026-08-31", "2026-09-15", "2026-09-29",
			"2026-09-30", "2026-10-07", "2026-10-10", "2026-10-16", "2026-10-17"}, cursors(recs))
		var row map[string]any
		for _, r := range recs {
			if r["ad_id"] == "1820000000000001" && r[cursorField] == "2026-08-30" {
				row = r
			}
		}
		assert.Equal(t, map[string]any{
			"advertiser_id": testAdvertiserA, "ad_id": "1820000000000001", "stat_time_day": "2026-08-30",
			"spend": 40.0, "impressions": 4000.0, "clicks": 80.0, "ctr": 2.0, "cpc": 0.5, "cpm": 10.0,
			"reach": 3200.0, "frequency": 1.25, "conversion": 0.0, "cost_per_conversion": nil, "conversion_rate": 0.0,
		}, row, "metrics become numbers, a dash becomes null and the record is kept")
	})

	t.Run("saved stat_time_day per advertiser minus the lookback", func(t *testing.T) {
		srv, h := fixtureServer(t, nil)
		src, _ := testSource(srv)
		state := reportStateMsg(streamCampaignsReportDaily, `{"advertisers":{"1000000001":{"stat_time_day":"2026-10-10"},"1000000002":{"stat_time_day":"2026-08-02"}}}`)
		out := runRead(t, src, testConfig(t, nil), configured(t, src, synclet.SyncModeIncremental, streamCampaignsReportDaily), state)
		require.NoError(t, out.Err)
		assert.Equal(t, []string{"2026-10-07..2026-10-18"}, windows(h, testAdvertiserA))
		assert.Equal(t, []string{"2026-08-01..2026-08-30", "2026-08-31..2026-09-29", "2026-09-30..2026-10-18"}, windows(h, testAdvertiserB))

		var daysA []string
		for _, r := range records(t, out, streamCampaignsReportDaily) {
			if r["advertiser_id"] == testAdvertiserA {
				daysA = append(daysA, r[cursorField].(string))
			}
		}
		assert.Equal(t, []string{"2026-10-07", "2026-10-10", "2026-10-16", "2026-10-17"}, daysA)

		states := out.States()
		require.Len(t, states, 4)
		assert.JSONEq(t, `{"advertisers":{"1000000001":{"stat_time_day":"2026-10-18"},"1000000002":{"stat_time_day":"2026-08-02"}}}`,
			string(states[0].Stream.StreamState))
		assert.JSONEq(t, `{"advertisers":{"1000000001":{"stat_time_day":"2026-10-18"},"1000000002":{"stat_time_day":"2026-10-18"}}}`,
			string(states[3].Stream.StreamState))
	})

	t.Run("full refresh ignores state and saves none", func(t *testing.T) {
		srv, h := fixtureServer(t, nil)
		src, _ := testSource(srv)
		state := reportStateMsg(streamAdvertisersReportDaily, `{"advertisers":{"1000000001":{"stat_time_day":"2026-10-10"}}}`)
		out := runRead(t, src, testConfig(t, onlyAdvertiserA), configured(t, src, synclet.SyncModeFullRefresh, streamAdvertisersReportDaily), state)
		require.NoError(t, out.Err)
		assert.Equal(t, []string{"2026-08-01..2026-08-30", "2026-08-31..2026-09-29", "2026-09-30..2026-10-18"}, windows(h, testAdvertiserA))
		assert.Empty(t, out.States())
		assert.Len(t, records(t, out, streamAdvertisersReportDaily), 11)
	})
}

func TestWindowHalving(t *testing.T) {
	dateRange := fixture{file: "errors/40067"}

	t.Run("40067 halves the window, which stays reduced for the advertiser", func(t *testing.T) {
		srv, h := fixtureServer(t, map[string][]fixture{reportPath: {dateRange, {}}})
		src, _ := testSource(srv)
		out := runRead(t, src, testConfig(t, nil), configured(t, src, synclet.SyncModeIncremental, streamAdGroupsReportDaily), nil)
		require.NoError(t, out.Err)
		assert.Equal(t, []string{
			"2026-08-01..2026-08-30", "2026-08-01..2026-08-15", "2026-08-16..2026-08-30", "2026-08-31..2026-09-14",
			"2026-09-15..2026-09-29", "2026-09-30..2026-10-14", "2026-10-15..2026-10-18",
		}, windows(h, testAdvertiserA))
		assert.Equal(t, []string{"2026-08-01..2026-08-30", "2026-08-31..2026-09-29", "2026-09-30..2026-10-18"}, windows(h, testAdvertiserB),
			"another advertiser starts at 30 days")
		assert.Len(t, records(t, out, streamAdGroupsReportDaily), 22)
	})

	t.Run("halving goes 30, 15, 7, 3, 1 days", func(t *testing.T) {
		srv, h := fixtureServer(t, map[string][]fixture{reportPath: {dateRange, dateRange, dateRange, dateRange, {}}})
		src, _ := testSource(srv)
		out := runRead(t, src, testConfig(t, onlyAdvertiserA), configured(t, src, synclet.SyncModeIncremental, streamAdGroupsReportDaily), nil)
		require.NoError(t, out.Err)
		assert.Equal(t, []string{"2026-08-01..2026-08-30", "2026-08-01..2026-08-15", "2026-08-01..2026-08-07", "2026-08-01..2026-08-03",
			"2026-08-01..2026-08-01", "2026-08-02..2026-08-02"}, windows(h, testAdvertiserA)[:6])
	})

	t.Run("40067 on a one-day window fails the stream", func(t *testing.T) {
		srv, h := fixtureServer(t, map[string][]fixture{reportPath: {dateRange}})
		src, _ := testSource(srv)
		out := runRead(t, src, testConfig(t, onlyAdvertiserA), configured(t, src, synclet.SyncModeIncremental, streamAdGroupsReportDaily), nil)
		require.EqualError(t, out.Err, "ad_groups_report_daily: advertiser 1000000001 2026-08-01..2026-08-01: "+
			"tiktok report/integrated/get/: code 40067: The date range of the report is too long.")
		assert.Len(t, h.of(reportPath), 5)
		assert.Empty(t, out.States())
	})
}

func TestReportRowOutsideWindow(t *testing.T) {
	tests := []struct {
		name, file, err string
	}{
		{
			name: "day before the window", file: "report/invalid/outside_window",
			err: "ads_report_daily: advertiser 1000000001 2026-08-01..2026-08-30: report row of day 2026-07-31 is outside the requested window",
		},
		{
			name: "row without a day", file: "report/invalid/no_day",
			err: "ads_report_daily: advertiser 1000000001 2026-08-01..2026-08-30: report row has no stat_time_day",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv, _ := fixtureServer(t, map[string][]fixture{reportPath: {{file: tt.file}}})
			src, _ := testSource(srv)
			out := runRead(t, src, testConfig(t, onlyAdvertiserA), configured(t, src, synclet.SyncModeIncremental, streamAdsReportDaily), nil)
			require.EqualError(t, out.Err, tt.err)
			assert.Empty(t, records(t, out, streamAdsReportDaily))
			assert.Empty(t, out.States())
		})
	}
}
