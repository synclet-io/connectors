package main

import (
	"encoding/json"
	"fmt"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
)

func lookback(n int) *int { return &n }

// dateRanges returns the date clause of each first-page search of stream d,
// by customer.
func dateRanges(t *testing.T, h *hits, d streamDef) map[string]string {
	t.Helper()
	out := map[string]string{}
	for cid, reqs := range streamSearches(h, d) {
		require.Len(t, reqs, 1, cid)
		out[cid] = reqs[0].query[len(d.query.text):]
	}
	return out
}

func between(since, until string) string {
	return fmt.Sprintf(" WHERE segments.date BETWEEN '%s' AND '%s'", since, until)
}

func datesOf(recs []map[string]any, cid float64) []string {
	var out []string
	for _, r := range recs {
		if r["customer_id"] == cid {
			out = append(out, r["segments_date"].(string))
		}
	}
	slices.Sort(out)
	return out
}

func campaignState(customers string) json.RawMessage {
	return json.RawMessage(`[{"type":"STREAM","stream":{"stream_descriptor":{"name":"campaign_performance_daily","namespace":"google_ads"},` +
		`"stream_state":{"customers":` + customers + `}}}]`)
}

func TestIncremental(t *testing.T) {
	d, _ := builtinByName(streamCampaignPerformance)
	tests := []struct {
		name   string
		edit   func(*Config)
		state  json.RawMessage
		ranges map[string]string
		datesA []string
		datesB []string
		final  string
	}{
		{
			name:   "no state reads from start_date",
			ranges: map[string]string{testClientA: between("2026-10-01", "2026-10-18"), testClientB: between("2026-10-01", "2026-10-18")},
			datesA: []string{"2026-10-01", "2026-10-16", "2026-10-17"},
			datesB: []string{"2026-10-16", "2026-10-17"},
			final:  `{"customers":{"1234567890":{"date":"2026-10-18"},"2345678901":{"date":"2026-10-18"}}}`,
		},
		{
			name:   "saved date minus the default lookback of 14 days",
			edit:   func(c *Config) { c.StartDate = "2026-09-01" },
			state:  campaignState(`{"1234567890":{"date":"2026-10-15"}}`),
			ranges: map[string]string{testClientA: between("2026-10-01", "2026-10-18"), testClientB: between("2026-09-01", "2026-10-18")},
			datesA: []string{"2026-10-01", "2026-10-16", "2026-10-17"},
			datesB: []string{"2026-10-16", "2026-10-17"},
			final:  `{"customers":{"1234567890":{"date":"2026-10-18"},"2345678901":{"date":"2026-10-18"}}}`,
		},
		{
			name:   "configured lookback",
			edit:   func(c *Config) { c.LookbackDays = lookback(3) },
			state:  campaignState(`{"1234567890":{"date":"2026-10-15"},"2345678901":{"date":"2026-10-18"}}`),
			ranges: map[string]string{testClientA: between("2026-10-12", "2026-10-18"), testClientB: between("2026-10-15", "2026-10-18")},
			datesA: []string{"2026-10-16", "2026-10-17"},
			datesB: []string{"2026-10-16", "2026-10-17"},
			final:  `{"customers":{"1234567890":{"date":"2026-10-18"},"2345678901":{"date":"2026-10-18"}}}`,
		},
		{
			name:   "never earlier than start_date",
			edit:   func(c *Config) { c.StartDate = "2026-10-10" },
			state:  campaignState(`{"1234567890":{"date":"2026-10-15"}}`),
			ranges: map[string]string{testClientA: between("2026-10-10", "2026-10-18"), testClientB: between("2026-10-10", "2026-10-18")},
			datesA: []string{"2026-10-16", "2026-10-17"},
			datesB: []string{"2026-10-16", "2026-10-17"},
			final:  `{"customers":{"1234567890":{"date":"2026-10-18"},"2345678901":{"date":"2026-10-18"}}}`,
		},
		{
			name:   "never earlier than 37 months ago",
			edit:   func(c *Config) { c.StartDate = "2023-01-01" },
			ranges: map[string]string{testClientA: between("2023-09-18", "2026-10-18"), testClientB: between("2023-09-18", "2026-10-18")},
			datesA: []string{"2026-09-30", "2026-10-01", "2026-10-16", "2026-10-17"},
			datesB: []string{"2026-10-16", "2026-10-17"},
			final:  `{"customers":{"1234567890":{"date":"2026-10-18"},"2345678901":{"date":"2026-10-18"}}}`,
		},
		{
			name:   "lookback from an old state stops at 37 months ago",
			edit:   func(c *Config) { c.StartDate = "2023-01-01" },
			state:  campaignState(`{"1234567890":{"date":"2023-09-20"},"2345678901":{"date":"2023-09-20"}}`),
			ranges: map[string]string{testClientA: between("2023-09-18", "2026-10-18"), testClientB: between("2023-09-18", "2026-10-18")},
			datesA: []string{"2026-09-30", "2026-10-01", "2026-10-16", "2026-10-17"},
			datesB: []string{"2026-10-16", "2026-10-17"},
			final:  `{"customers":{"1234567890":{"date":"2026-10-18"},"2345678901":{"date":"2026-10-18"}}}`,
		},
		{
			name:   "end_date ends the range",
			edit:   func(c *Config) { c.EndDate = "2026-10-16" },
			ranges: map[string]string{testClientA: between("2026-10-01", "2026-10-16"), testClientB: between("2026-10-01", "2026-10-16")},
			datesA: []string{"2026-10-01", "2026-10-16"},
			datesB: []string{"2026-10-16"},
			final:  `{"customers":{"1234567890":{"date":"2026-10-16"},"2345678901":{"date":"2026-10-16"}}}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv, h := fixtureServer(t, nil)
			src, _ := testSource(srv)
			cfg := testConfig(t, tt.edit)
			out := runRead(t, src, cfg, configured(t, src, cfg, synclet.SyncModeIncremental, streamCampaignPerformance), tt.state)
			require.NoError(t, out.Err)
			assert.Equal(t, tt.ranges, dateRanges(t, h, d))
			recs := records(t, out, streamCampaignPerformance)
			assert.Equal(t, tt.datesA, datesOf(recs, 1234567890))
			assert.Equal(t, tt.datesB, datesOf(recs, 2345678901))
			states := out.States()
			require.Len(t, states, 2, "one state per customer")
			assert.JSONEq(t, tt.final, string(states[1].Stream.StreamState))
		})
	}

	t.Run("full refresh reads from start_date without state", func(t *testing.T) {
		srv, h := fixtureServer(t, nil)
		src, _ := testSource(srv)
		cfg := testConfig(t, nil)
		out := runRead(t, src, cfg, configured(t, src, cfg, synclet.SyncModeFullRefresh, streamCampaignPerformance),
			campaignState(`{"1234567890":{"date":"2026-10-15"}}`))
		require.NoError(t, out.Err)
		assert.Equal(t, map[string]string{testClientA: between("2026-10-01", "2026-10-18"), testClientB: between("2026-10-01", "2026-10-18")},
			dateRanges(t, h, d))
		assert.Empty(t, out.States())
		assert.Len(t, records(t, out, streamCampaignPerformance), 5)
	})

	t.Run("record of a daily stream", func(t *testing.T) {
		srv, _ := fixtureServer(t, nil)
		src, _ := testSource(srv)
		cfg := testConfig(t, func(c *Config) { c.ChildCustomerIDs = []string{testClientB} })
		out := runRead(t, src, cfg, configured(t, src, cfg, synclet.SyncModeIncremental, streamAdPerformance), nil)
		require.NoError(t, out.Err)
		recs := records(t, out, streamAdPerformance)
		require.Len(t, recs, 2)
		assert.Equal(t, map[string]any{
			"customer_id": float64(2345678901), "campaign_id": float64(200001), "ad_group_id": float64(400001), "ad_group_ad_ad_id": float64(600001),
			"segments_date": "2026-10-16", "metrics_impressions": float64(2000), "metrics_clicks": float64(100), "metrics_cost_micros": float64(24680000),
			"metrics_conversions": float64(4), "metrics_conversions_value": float64(201), "metrics_all_conversions": float64(5),
			"metrics_ctr": 0.05, "metrics_average_cpc": float64(246800),
		}, recs[0])
	})
}

func TestIncrementalRange(t *testing.T) {
	cfg := Config{StartDate: "2026-01-01"}
	since, until, err := dailyRange(cfg, "", testNow.Truncate(24*3600e9))
	require.NoError(t, err)
	assert.Equal(t, "2026-01-01", since.Format(dateLayout))
	assert.Equal(t, "2026-10-18", until.Format(dateLayout))

	cfg.LookbackDays = lookback(0)
	since, _, err = dailyRange(cfg, "2026-10-10", testNow.Truncate(24*3600e9))
	require.NoError(t, err)
	assert.Equal(t, "2026-10-10", since.Format(dateLayout))

	_, _, err = dailyRange(cfg, "10/10/2026", testNow)
	assert.ErrorContains(t, err, `saved date "10/10/2026" is not a YYYY-MM-DD date`)
}
