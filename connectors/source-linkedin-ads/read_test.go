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

func pluck(recs []map[string]any, key string) []any {
	out := make([]any, len(recs))
	for i, r := range recs {
		out[i] = r[key]
	}
	return out
}

func TestReadEntities(t *testing.T) {
	srv, h := fixtureServer(t, nil)
	src, _ := testSource(srv)
	out := runRead(t, src, testConfig(t, nil),
		configured(t, src, synclet.SyncModeFullRefresh, streamAdAccounts, streamCampaignGroups, streamCampaigns, streamCreatives), nil)
	require.NoError(t, out.Err)

	accounts := records(t, out, streamAdAccounts)
	require.Len(t, accounts, 2)
	assert.Equal(t, []any{1001.0, 1002.0}, pluck(accounts, "id"))
	assert.Equal(t, "Example Account A", accounts[0]["name"])
	assert.Equal(t, true, accounts[0]["test"])
	assert.Equal(t, []any{"RUNNABLE"}, accounts[0]["servingStatuses"])
	assert.Len(t, h.of("adAccounts/1001"), 1, "configured accounts are read by id")
	assert.Empty(t, h.of("adAccounts"), "configured accounts are not searched")

	groups := records(t, out, streamCampaignGroups)
	assert.Equal(t, []any{2001.0, 2101.0}, pluck(groups, "id"))
	assert.Equal(t, map[string]any{"amount": "1000.00", "currencyCode": "USD"}, groups[0]["totalBudget"])

	campaigns := records(t, out, streamCampaigns)
	assert.Equal(t, []any{"Example Campaign A", "Example Campaign B", "Example Campaign C", "Example Campaign D"}, pluck(campaigns, "name"))
	assert.Equal(t, "urn:li:sponsoredCampaignGroup:2001", campaigns[0]["campaignGroup"])
	campaignReqs := h.of("adAccounts/1001/adCampaigns")
	require.Len(t, campaignReqs, 2)
	assert.Equal(t, "search", campaignReqs[0].params["q"])
	assert.Equal(t, "(status:(values:List(ACTIVE,PAUSED,ARCHIVED,COMPLETED,CANCELED,DRAFT,PENDING_DELETION,REMOVED)))", campaignReqs[0].params["search"])
	assert.Equal(t, "p2", campaignReqs[1].params["pageToken"])
	assert.Equal(t, "(status:(values:List(ACTIVE,PAUSED,ARCHIVED,CANCELED,DRAFT,PENDING_DELETION,REMOVED)))",
		h.of("adAccounts/1001/adCampaignGroups")[0].params["search"])

	creatives := records(t, out, streamCreatives)
	assert.Equal(t, []any{"urn:li:sponsoredCreative:4001", "urn:li:sponsoredCreative:4002", "urn:li:sponsoredCreative:4101"}, pluck(creatives, "id"))
	assert.Equal(t, "criteria", h.of("adAccounts/1001/creatives")[0].params["q"])
	assert.Empty(t, out.States(), "full refresh entity streams save no state")
}

func TestReadSearchesAccountsWithoutAccountIDs(t *testing.T) {
	srv, h := fixtureServer(t, nil)
	src, _ := testSource(srv)
	out := runRead(t, src, testConfig(t, func(c *Config) { c.AccountIDs = nil }),
		configured(t, src, synclet.SyncModeFullRefresh, streamAdAccounts, streamCampaigns), nil)
	require.NoError(t, out.Err)
	assert.Equal(t, []any{1001.0, 1002.0}, pluck(records(t, out, streamAdAccounts), "id"))
	assert.Len(t, h.of("adAccounts"), 2, "the search is paged once and its result reused")
	assert.Len(t, records(t, out, streamCampaigns), 4)
}

func TestReadUnknownStream(t *testing.T) {
	srv, _ := fixtureServer(t, nil)
	src, _ := testSource(srv)
	cat := configured(t, src, synclet.SyncModeFullRefresh, streamCampaigns)
	cat.Streams[0].Stream.Name = "conversions"
	out := runRead(t, src, testConfig(t, nil), cat, nil)
	require.ErrorContains(t, out.Err, `unknown stream "conversions"`)
}

func TestReadFailsLoudlyOnAPIError(t *testing.T) {
	srv, _ := fixtureServer(t, map[string][]fixture{
		"adAccounts/1002/adCampaigns": {{status: 403, file: "errors/403_account"}},
	})
	src, _ := testSource(srv)
	out := runRead(t, src, testConfig(t, nil), configured(t, src, synclet.SyncModeFullRefresh, streamCampaigns), nil)
	require.EqualError(t, out.Err, "campaigns: account 1002: linkedin GET adAccounts/1002/adCampaigns: HTTP 403 ACCESS_DENIED (100): Not enough permissions to access: adAccounts.GET.20260901")
}
