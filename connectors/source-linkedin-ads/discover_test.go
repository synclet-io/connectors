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

func TestDiscover(t *testing.T) {
	srv, h := fixtureServer(t, nil)
	src, _ := testSource(srv)
	streams := discoverStreams(t, src)
	want := map[string]struct {
		pk     [][]string
		cursor []string
	}{
		streamAdAccounts:        {pk: [][]string{{"id"}}},
		streamCampaignGroups:    {pk: [][]string{{"id"}}},
		streamCampaigns:         {pk: [][]string{{"id"}}},
		streamCreatives:         {pk: [][]string{{"id"}}},
		streamAnalyticsCampaign: {pk: [][]string{{"campaign_id"}, {"date"}}, cursor: []string{"date"}},
		streamAnalyticsCreative: {pk: [][]string{{"creative_id"}, {"date"}}, cursor: []string{"date"}},
	}
	assert.Len(t, streams, len(want))
	for name, w := range want {
		s, ok := streams[name]
		if !assert.True(t, ok, "stream %s is missing", name) {
			continue
		}
		assert.Equal(t, namespace, s.Namespace)
		assert.Equal(t, w.pk, s.SourceDefinedPrimaryKey, name)
		var schema struct {
			Properties map[string]any `json:"properties"`
		}
		require.NoError(t, json.Unmarshal(s.JSONSchema, &schema))
		for _, k := range w.pk {
			assert.Contains(t, schema.Properties, k[0], "%s: primary key in the schema", name)
		}
		if w.cursor == nil {
			assert.Equal(t, []synclet.SyncMode{synclet.SyncModeFullRefresh}, s.SupportedSyncModes, name)
			continue
		}
		assert.Equal(t, []synclet.SyncMode{synclet.SyncModeFullRefresh, synclet.SyncModeIncremental}, s.SupportedSyncModes, name)
		assert.Equal(t, w.cursor, s.DefaultCursorField, name)
		assert.True(t, s.SourceDefinedCursor, name)
	}
	assert.Empty(t, h.list(), "discover sends no request")
}
