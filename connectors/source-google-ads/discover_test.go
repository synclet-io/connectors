package main

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
	"github.com/synclet-io/synclet-go-sdk/testkit"
)

// discoverStreams returns the discovered streams of cfg by name.
func discoverStreams(t *testing.T, src Source, cfg json.RawMessage) map[string]synclet.Stream {
	t.Helper()
	out, err := testkit.RunSource(t.Context(), src, synclet.Request{Command: synclet.CommandDiscover, Config: cfg})
	require.NoError(t, err)
	require.NoError(t, out.Err)
	streams := map[string]synclet.Stream{}
	for _, s := range out.Catalog().Streams {
		streams[s.Name] = s
	}
	return streams
}

func configured(t *testing.T, src Source, cfg json.RawMessage, mode synclet.SyncMode, names ...string) *synclet.ConfiguredCatalog {
	t.Helper()
	streams := discoverStreams(t, src, cfg)
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

// schemaProps returns the properties of a stream's JSON schema.
func schemaProps(t *testing.T, s synclet.Stream) map[string]map[string]any {
	t.Helper()
	var schema struct {
		Properties map[string]map[string]any `json:"properties"`
	}
	require.NoError(t, json.Unmarshal(s.JSONSchema, &schema))
	return schema.Properties
}

func TestDiscoverBuiltins(t *testing.T) {
	srv, h := fixtureServer(t, nil)
	src, _ := testSource(srv)
	streams := discoverStreams(t, src, testConfig(t, nil))
	assert.Empty(t, h.keys(), "built-in streams need no request")
	require.Len(t, streams, 9)

	for _, name := range []string{streamCustomer, streamCustomerClient, streamCampaign, streamAdGroup, streamAdGroupAd} {
		assert.Equal(t, []synclet.SyncMode{synclet.SyncModeFullRefresh}, streams[name].SupportedSyncModes, name)
		assert.Contains(t, schemaProps(t, streams[name]), "customer_id", name)
	}
	for _, name := range []string{streamCampaignPerformance, streamAdGroupPerformance, streamAdPerformance, streamGeographicViewDaily} {
		s := streams[name]
		assert.Equal(t, []synclet.SyncMode{synclet.SyncModeFullRefresh, synclet.SyncModeIncremental}, s.SupportedSyncModes, name)
		assert.True(t, s.SourceDefinedCursor, name)
		assert.Equal(t, []string{"segments_date"}, s.DefaultCursorField, name)
		assert.Contains(t, schemaProps(t, s), "customer_id", name)
	}
	assert.Equal(t, [][]string{{"ad_group_id"}, {"ad_group_ad_ad_id"}}, streams[streamAdGroupAd].SourceDefinedPrimaryKey)
	assert.Equal(t, [][]string{{"customer_id"}, {"customer_client_id"}}, streams[streamCustomerClient].SourceDefinedPrimaryKey)
	assert.Equal(t, [][]string{{"ad_group_id"}, {"geographic_view_country_criterion_id"}, {"geographic_view_location_type"}, {"segments_date"}},
		streams[streamGeographicViewDaily].SourceDefinedPrimaryKey)

	props := schemaProps(t, streams[streamAdPerformance])
	assert.Equal(t, "integer", props["metrics_cost_micros"]["type"])
	assert.Equal(t, "number", props["metrics_ctr"]["type"])
	assert.Equal(t, "date", props["segments_date"]["format"])
	assert.Equal(t, "array", schemaProps(t, streams[streamAdGroupAd])["ad_group_ad_ad_final_urls"]["type"])
	assert.Equal(t, "boolean", schemaProps(t, streams[streamCustomer])["customer_manager"]["type"])
}

func TestDiscoverCustomQuery(t *testing.T) {
	srv, h := fixtureServer(t, nil)
	src, _ := testSource(srv)
	streams := discoverStreams(t, src, testConfig(t, withKeywords))
	require.Len(t, streams, 10)

	s, ok := streams["my_keywords"]
	require.True(t, ok)
	assert.Equal(t, namespace, s.Namespace)
	assert.Equal(t, []synclet.SyncMode{synclet.SyncModeFullRefresh, synclet.SyncModeIncremental}, s.SupportedSyncModes)
	assert.Equal(t, []string{"segments_date"}, s.DefaultCursorField)
	assert.Empty(t, s.SourceDefinedPrimaryKey)
	props := schemaProps(t, s)
	assert.Len(t, props, 7)
	for key, typ := range map[string]string{
		"customer_id":                           "integer",
		"ad_group_id":                           "integer",
		"ad_group_criterion_keyword_text":       "string",
		"ad_group_criterion_keyword_match_type": "string",
		"metrics_clicks":                        "integer",
		"metrics_cost_micros":                   "integer",
		"segments_date":                         "string",
	} {
		assert.Equal(t, typ, props[key]["type"], key)
	}

	fieldReqs := 0
	for _, r := range h.list() {
		if r.path == "googleAdsFields:search" {
			fieldReqs++
			assert.Equal(t, "SELECT name, data_type, is_repeated WHERE name IN ('ad_group_criterion.keyword.match_type', 'ad_group_criterion.keyword.text')", r.query)
		}
	}
	assert.Equal(t, 1, fieldReqs, "one GoogleAdsFields request for the fields knownFields lacks")

	t.Run("unknown field", func(t *testing.T) {
		out, err := testkit.RunSource(t.Context(), src, synclet.Request{Command: synclet.CommandDiscover, Config: testConfig(t, func(c *Config) {
			c.CustomQueries = []CustomQuery{{Name: "typo", Query: "SELECT ad_group_criterion.keyword.txt FROM keyword_view"}}
		})})
		require.NoError(t, err)
		require.Error(t, out.Err)
		assert.Contains(t, out.Err.Error(), `custom query typo: unknown field ad_group_criterion.keyword.txt`)
	})
}
