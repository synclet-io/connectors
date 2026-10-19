package main

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
)

func onlyAccountA(c *Config) { c.AccountIDs = []string{testAccountA} }

func TestAnalyticsFinderQuery(t *testing.T) {
	srv, h := fixtureServer(t, nil)
	src, _ := testSource(srv)
	out := runRead(t, src, testConfig(t, onlyAccountA), configured(t, src, synclet.SyncModeFullRefresh, streamAnalyticsCampaign), nil)
	require.NoError(t, out.Err)

	reqs := h.of("adAnalytics")
	require.Len(t, reqs, 3, "start_date to today in windows of 30 days")
	p := reqs[0].params
	assert.Equal(t, "analytics", p["q"])
	assert.Equal(t, "CAMPAIGN", p["pivot"])
	assert.Equal(t, "DAILY", p["timeGranularity"])
	assert.Equal(t, "List(urn%3Ali%3AsponsoredAccount%3A1001)", p["accounts"])
	assert.Equal(t, "(start:(year:2026,month:8,day:1),end:(year:2026,month:8,day:30))", p["dateRange"])
	assert.Equal(t, "dateRange,pivotValues,"+strings.Join(analyticsMetrics, ","), p["fields"])
	assert.LessOrEqual(t, len(strings.Split(p["fields"], ",")), 20, "LinkedIn takes at most 20 fields")
	assert.Equal(t, "(start:(year:2026,month:8,day:31),end:(year:2026,month:9,day:29))", reqs[1].params["dateRange"])
	assert.Equal(t, "(start:(year:2026,month:9,day:30),end:(year:2026,month:10,day:18))", reqs[2].params["dateRange"])

	recs := records(t, out, streamAnalyticsCampaign)
	require.Len(t, recs, 10)
	assert.Equal(t, map[string]any{
		"account_id": testAccountA, "campaign_id": "3001", "pivot_value": "urn:li:sponsoredCampaign:3001", "date": "2026-08-01",
		"impressions": 1000.0, "clicks": 10.0, "costInLocalCurrency": 5.0, "costInUsd": 5.0, "landingPageClicks": 8.0,
		"externalWebsiteConversions": 1.0, "externalWebsitePostClickConversions": 1.0, "externalWebsitePostViewConversions": 0.0,
		"conversionValueInLocalCurrency": 2.0, "oneClickLeads": 0.0, "totalEngagements": 12.0, "likes": 1.0,
		"comments": 0.0, "shares": 0.0, "follows": 0.0, "videoViews": 0.0, "videoCompletions": 0.0,
	}, recs[0])
	assert.Empty(t, out.States(), "a full refresh saves no state")
}

func TestAnalyticsByCreative(t *testing.T) {
	srv, h := fixtureServer(t, nil)
	src, _ := testSource(srv)
	out := runRead(t, src, testConfig(t, onlyAccountA), configured(t, src, synclet.SyncModeFullRefresh, streamAnalyticsCreative), nil)
	require.NoError(t, out.Err)
	assert.Equal(t, "CREATIVE", h.of("adAnalytics")[0].params["pivot"])
	recs := records(t, out, streamAnalyticsCreative)
	require.NotEmpty(t, recs)
	assert.Equal(t, "4001", recs[0]["creative_id"])
	assert.Equal(t, "urn:li:sponsoredCreative:4001", recs[0]["pivot_value"])
	assert.NotContains(t, recs[0], "campaign_id")
}

func TestAnalyticsResumeFromSavedDateMinusLookback(t *testing.T) {
	srv, h := fixtureServer(t, nil)
	src, _ := testSource(srv)
	state := json.RawMessage(`[{"type":"STREAM","stream":{"stream_descriptor":{"name":"ad_analytics_by_campaign","namespace":"linkedin_ads"},
		"stream_state":{"accounts":{"1001":{"date":"2026-10-12"}}}}}]`)
	out := runRead(t, src, testConfig(t, nil), configured(t, src, synclet.SyncModeIncremental, streamAnalyticsCampaign), state)
	require.NoError(t, out.Err)

	var a, b []string
	for _, r := range h.of("adAnalytics") {
		if strings.HasSuffix(r.params["accounts"], "3A1001)") {
			a = append(a, r.params["dateRange"])
		} else {
			b = append(b, r.params["dateRange"])
		}
	}
	assert.Equal(t, []string{"(start:(year:2026,month:10,day:5),end:(year:2026,month:10,day:18))"}, a, "saved 2026-10-12 minus 7 days")
	require.Len(t, b, 3, "an account without saved state starts at start_date")
	assert.Equal(t, "(start:(year:2026,month:8,day:1),end:(year:2026,month:8,day:30))", b[0])

	var daysA []any
	for _, r := range records(t, out, streamAnalyticsCampaign) {
		if r["account_id"] == testAccountA {
			daysA = append(daysA, r["date"])
		}
	}
	assert.Equal(t, []any{"2026-10-12", "2026-10-12", "2026-10-16", "2026-10-16"}, daysA)

	states := out.States()
	require.NotEmpty(t, states)
	assert.JSONEq(t, `{"accounts":{"1001":{"date":"2026-10-18"},"1002":{"date":"2026-10-18"}}}`, string(states[len(states)-1].Stream.StreamState))
}

func TestAnalyticsResumeNeverBeforeStartDate(t *testing.T) {
	srv, h := fixtureServer(t, nil)
	src, _ := testSource(srv)
	state := json.RawMessage(`[{"type":"STREAM","stream":{"stream_descriptor":{"name":"ad_analytics_by_campaign","namespace":"linkedin_ads"},
		"stream_state":{"accounts":{"1001":{"date":"2026-08-03"}}}}}]`)
	out := runRead(t, src, testConfig(t, onlyAccountA), configured(t, src, synclet.SyncModeIncremental, streamAnalyticsCampaign), state)
	require.NoError(t, out.Err)
	assert.Equal(t, "(start:(year:2026,month:8,day:1),end:(year:2026,month:8,day:30))", h.of("adAnalytics")[0].params["dateRange"])
}

func TestAnalyticsSavesStateAfterEveryWindow(t *testing.T) {
	srv, _ := fixtureServer(t, nil)
	src, _ := testSource(srv)
	out := runRead(t, src, testConfig(t, onlyAccountA), configured(t, src, synclet.SyncModeIncremental, streamAnalyticsCampaign), nil)
	require.NoError(t, out.Err)
	var got []string
	for _, s := range out.States() {
		got = append(got, string(s.Stream.StreamState))
	}
	assert.Equal(t, []string{
		`{"accounts":{"1001":{"date":"2026-08-30"}}}`,
		`{"accounts":{"1001":{"date":"2026-09-29"}}}`,
		`{"accounts":{"1001":{"date":"2026-10-18"}}}`,
	}, got)
}

func TestAnalyticsRowOutsideWindowFails(t *testing.T) {
	srv, _ := fixtureServer(t, map[string][]fixture{"adAnalytics": {{file: "analytics/outside_window"}}})
	src, _ := testSource(srv)
	out := runRead(t, src, testConfig(t, onlyAccountA), configured(t, src, synclet.SyncModeFullRefresh, streamAnalyticsCampaign), nil)
	require.EqualError(t, out.Err, "ad_analytics_by_campaign: account 1001 2026-08-01..2026-08-30: analytics row of day 2026-07-01 is outside the requested range")
}

func TestAnalyticsBadStateIsConfigError(t *testing.T) {
	srv, _ := fixtureServer(t, nil)
	src, _ := testSource(srv)
	state := json.RawMessage(`[{"type":"STREAM","stream":{"stream_descriptor":{"name":"ad_analytics_by_campaign","namespace":"linkedin_ads"},
		"stream_state":{"accounts":{"1001":{"date":"12/10/2026"}}}}}]`)
	out := runRead(t, src, testConfig(t, onlyAccountA), configured(t, src, synclet.SyncModeIncremental, streamAnalyticsCampaign), state)
	require.ErrorContains(t, out.Err, `saved date "12/10/2026" is not a YYYY-MM-DD date`)
}
