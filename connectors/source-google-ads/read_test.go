package main

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
	"github.com/synclet-io/synclet-go-sdk/testkit"
)

// runRead reads catalog and returns the output; the read's error is in out.Err.
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

func records(t *testing.T, out *testkit.Output, stream string) []map[string]any {
	t.Helper()
	var recs []map[string]any
	for _, r := range out.Records(synclet.StreamDescriptor{Name: stream, Namespace: namespace}) {
		var m map[string]any
		require.NoError(t, json.Unmarshal(r.Data, &m))
		recs = append(recs, m)
	}
	return recs
}

func TestReadEntities(t *testing.T) {
	srv, _ := fixtureServer(t, nil)
	src, _ := testSource(srv)
	cfg := testConfig(t, nil)
	out := runRead(t, src, cfg, configured(t, src, cfg, synclet.SyncModeFullRefresh,
		streamCustomer, streamCampaign, streamAdGroupAd), nil)
	require.NoError(t, out.Err)

	customers := records(t, out, streamCustomer)
	require.Len(t, customers, 2)
	assert.Equal(t, map[string]any{
		"customer_id": float64(1234567890), "customer_descriptive_name": "Example Shop A", "customer_currency_code": "USD",
		"customer_time_zone": "Etc/UTC", "customer_status": "ENABLED",
	}, customers[0])

	campaigns := records(t, out, streamCampaign)
	require.Len(t, campaigns, 3, "two pages of client A and one of client B")
	assert.Equal(t, map[string]any{
		"customer_id": float64(1234567890), "campaign_id": float64(100002), "campaign_name": "Example Brand Campaign",
		"campaign_status": "PAUSED", "campaign_advertising_channel_type": "SEARCH", "campaign_bidding_strategy_type": "MANUAL_CPC",
		"campaign_campaign_budget": "customers/1234567890/campaignBudgets/9100002",
	}, campaigns[1])

	ads := records(t, out, streamAdGroupAd)
	require.Len(t, ads, 2)
	assert.Equal(t, []any{"https://example.com/shop"}, ads[0]["ad_group_ad_ad_final_urls"])
	assert.Equal(t, float64(500001), ads[0]["ad_group_ad_ad_id"])
	assert.Empty(t, out.States(), "full refresh emits no state")
}

func TestReadCustomQuery(t *testing.T) {
	srv, h := fixtureServer(t, nil)
	src, _ := testSource(srv)
	cfg := testConfig(t, func(c *Config) { withKeywords(c); c.ChildCustomerIDs = []string{testClientA} })
	out := runRead(t, src, cfg, configured(t, src, cfg, synclet.SyncModeIncremental, "my_keywords"), nil)
	require.NoError(t, out.Err)

	reqs := h.searches("keyword_view")
	require.Len(t, reqs, 1)
	assert.Equal(t, "SELECT customer.id, ad_group.id, ad_group_criterion.keyword.text, ad_group_criterion.keyword.match_type, "+
		"metrics.clicks, metrics.cost_micros, segments.date FROM keyword_view WHERE ad_group_criterion.status = 'ENABLED' "+
		"AND segments.date BETWEEN '2026-10-01' AND '2026-10-18' ORDER BY metrics.clicks DESC", reqs[0].query)

	recs := records(t, out, "my_keywords")
	require.Len(t, recs, 3)
	assert.Equal(t, map[string]any{
		"customer_id": float64(1234567890), "ad_group_id": float64(300001),
		"ad_group_criterion_keyword_text": "example shoes", "ad_group_criterion_keyword_match_type": "PHRASE",
		"metrics_clicks": float64(10), "metrics_cost_micros": float64(2500000), "segments_date": "2026-10-01",
	}, recs[0])
	require.Len(t, out.States(), 1)
	assert.JSONEq(t, `{"customers":{"1234567890":{"date":"2026-10-18"}}}`, string(out.States()[0].Stream.StreamState))
}

func TestReadUnknownStream(t *testing.T) {
	srv, _ := fixtureServer(t, nil)
	src, _ := testSource(srv)
	cat := &synclet.ConfiguredCatalog{Streams: []synclet.ConfiguredStream{{
		Stream: synclet.Stream{Name: "nope", Namespace: namespace}, SyncMode: synclet.SyncModeFullRefresh,
	}}}
	out := runRead(t, src, testConfig(t, nil), cat, nil)
	require.Error(t, out.Err)
	assert.Contains(t, out.Err.Error(), `unknown stream "nope"`)
}
