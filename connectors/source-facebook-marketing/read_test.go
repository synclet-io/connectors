package main

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
	"github.com/synclet-io/synclet-go-sdk/testkit"
)

// discoverStreams returns the discovered streams by name.
func discoverStreams(t *testing.T, src Source) map[string]synclet.Stream {
	t.Helper()
	out, err := testkit.RunSource(t.Context(), src, synclet.Request{Command: synclet.CommandDiscover, Config: testConfig(t, nil)})
	require.NoError(t, err)
	require.NoError(t, out.Err)
	streams := map[string]synclet.Stream{}
	for _, s := range out.Catalog().Streams {
		streams[s.Name] = s
	}
	return streams
}

func configured(t *testing.T, src Source, mode synclet.SyncMode, names ...string) *synclet.ConfiguredCatalog {
	t.Helper()
	streams := discoverStreams(t, src)
	cat := &synclet.ConfiguredCatalog{}
	for _, n := range names {
		s, ok := streams[n]
		require.True(t, ok, "stream %s", n)
		dest := synclet.DestinationSyncModeOverwrite
		if mode == synclet.SyncModeIncremental {
			dest = synclet.DestinationSyncModeAppendDedup
		}
		cat.Streams = append(cat.Streams, synclet.ConfiguredStream{Stream: s, SyncMode: mode, DestinationSyncMode: dest})
	}
	return cat
}

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
	srv, h := fixtureServer(t, nil)
	src, _ := testSource(srv)
	out := runRead(t, src, testConfig(t, nil), configured(t, src, synclet.SyncModeFullRefresh,
		streamAdAccounts, streamCampaigns, streamAdSets, streamAds, streamAdCreatives), nil)
	require.NoError(t, out.Err)

	accounts := records(t, out, streamAdAccounts)
	require.Len(t, accounts, 2)
	assert.Equal(t, map[string]any{
		"id": "act_" + testAccountA, "account_id": testAccountA, "name": "Example Shop A", "account_status": float64(1),
		"currency": "USD", "timezone_name": "Etc/UTC", "created_time": "2026-01-05T09:00:00Z",
	}, accounts[0])

	campaigns := records(t, out, streamCampaigns)
	require.Len(t, campaigns, 3, "two pages of account A and one of account B")
	assert.Equal(t, "2026-10-01T08:30:00Z", campaigns[0]["updated_time"])
	assert.Len(t, records(t, out, streamAdSets), 3)
	assert.Len(t, records(t, out, streamAds), 3)
	assert.Len(t, records(t, out, streamAdCreatives), 2)

	var cursors []string
	for _, r := range h.list() {
		if r.path == "act_"+testAccountA+"/campaigns" {
			assert.Equal(t, "500", r.query.Get("limit"))
			cursors = append(cursors, r.query.Get("after"))
		}
	}
	assert.Equal(t, []string{"", "cmp2"}, cursors)
}

func TestReadBusinessCredits(t *testing.T) {
	srv, _ := fixtureServer(t, nil)
	src, _ := testSource(srv)
	out := runRead(t, src, testConfig(t, nil), configured(t, src, synclet.SyncModeFullRefresh, streamBusinessCredits), nil)
	require.NoError(t, out.Err)
	recs := records(t, out, streamBusinessCredits)
	require.Len(t, recs, 2)
	first := recs[0]
	assert.Equal(t, "66000001", first["id"])
	assert.Equal(t, 1500.0, first["balance_amount"], "1,500.00")
	assert.Equal(t, 3500.0, first["credit_available_amount"], "3500,00 with a comma decimal separator")
	assert.Equal(t, 5000.0, first["max_balance_amount"], "5 000,00")
	assert.Equal(t, "EUR", first["credit_available_currency"])
	assert.Equal(t, "150000", first["balance_amount_in_hundredths"])
	assert.Equal(t, "0.00", first["allocated_amount_offsetted_amount"])
	assert.Equal(t, testBusiness, first["owner_business_id"])
	assert.Equal(t, "Example Business", first["owner_business_name"])
	assert.Equal(t, false, first["is_automated_experience"])
	assert.Equal(t, "2026-10-18T12:00:00Z", first["date_created"])
	assert.Equal(t, true, recs[1]["is_automated_experience"])
}

func TestParseAmount(t *testing.T) {
	for in, want := range map[string]string{
		"1,500.00": "1500.00", "3500,00": "3500.00", "5 000,00": "5000.00", "5 000,00": "5000.00", "12": "12",
	} {
		got, ok := parseAmount(in)
		assert.True(t, ok, in)
		assert.Equal(t, json.Number(want), got, in)
	}
	for _, in := range []string{"", "n/a", "1,2,3"} {
		_, ok := parseAmount(in)
		assert.False(t, ok, in)
	}
}
