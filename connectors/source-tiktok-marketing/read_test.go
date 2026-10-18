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
	out := runRead(t, src, testConfig(t, nil),
		configured(t, src, synclet.SyncModeFullRefresh, streamAdvertisers, streamCampaigns, streamAdGroups, streamAds), nil)
	require.NoError(t, out.Err)

	advertisers := records(t, out, streamAdvertisers)
	require.Len(t, advertisers, 2)
	assert.Equal(t, map[string]any{
		"advertiser_id": testAdvertiserA, "name": "Example Advertiser A", "company": "Example Company A",
		"status": "STATUS_ENABLE", "role": "ROLE_ADVERTISER", "currency": "USD", "timezone": "Etc/GMT",
		"display_timezone": "Etc/GMT", "country": "US", "industry": "290101",
		"description": "Synthetic test advertiser", "balance": 1000.0, "create_time": 1767225600.0,
	}, advertisers[0])
	infos := h.of("advertiser/info/")
	require.Len(t, infos, 1)
	assert.JSONEq(t, `["1000000001","1000000002"]`, infos[0].query.Get("advertiser_ids"))

	campaigns := records(t, out, streamCampaigns)
	var names []any
	for _, c := range campaigns {
		names = append(names, c["campaign_name"])
		assert.NotContains(t, c, "is_new_structure", "only schema fields are emitted")
	}
	assert.Equal(t, []any{"Example Campaign A", "Example Campaign C", "Example Campaign B"}, names)
	assert.Equal(t, "2026-09-01 10:30:00", campaigns[0]["modify_time"])

	gets := h.of("campaign/get/")
	require.Len(t, gets, 3)
	var fieldNames []string
	require.NoError(t, json.Unmarshal([]byte(gets[0].query.Get("fields")), &fieldNames))
	assert.Contains(t, fieldNames, "campaign_name")
	assert.Equal(t, testAdvertiserA, gets[0].query.Get("advertiser_id"))
	assert.Equal(t, testAdvertiserB, gets[2].query.Get("advertiser_id"))

	assert.NotEmpty(t, records(t, out, streamAdGroups))
	assert.NotEmpty(t, records(t, out, streamAds))
	assert.Empty(t, out.States(), "full refresh entity streams save no state")
}

func TestReadUnknownStream(t *testing.T) {
	srv, _ := fixtureServer(t, nil)
	src, _ := testSource(srv)
	cat := configured(t, src, synclet.SyncModeFullRefresh, streamCampaigns)
	cat.Streams[0].Stream.Name = "pixels"
	out := runRead(t, src, testConfig(t, nil), cat, nil)
	require.ErrorContains(t, out.Err, `unknown stream "pixels"`)
}
