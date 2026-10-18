package main

import (
	"encoding/json"
	"maps"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
	"github.com/synclet-io/synclet-go-sdk/testkit"
)

// discoverStreams returns the discovered streams of cfg by name, in order.
func discoverStreams(t *testing.T, src Source, cfg json.RawMessage) (map[string]synclet.Stream, []string) {
	t.Helper()
	out, err := testkit.RunSource(t.Context(), src, synclet.Request{Command: synclet.CommandDiscover, Config: cfg})
	require.NoError(t, err)
	require.NoError(t, out.Err)
	streams := map[string]synclet.Stream{}
	var names []string
	for _, s := range out.Catalog().Streams {
		streams[s.Name] = s
		names = append(names, s.Name)
	}
	return streams, names
}

// configured returns a catalog of the named streams in mode; a stream that
// supports full refresh only is configured as full refresh.
func configured(t *testing.T, src Source, cfg json.RawMessage, mode synclet.SyncMode, names ...string) *synclet.ConfiguredCatalog {
	t.Helper()
	streams, _ := discoverStreams(t, src, cfg)
	cat := &synclet.ConfiguredCatalog{}
	for _, n := range names {
		s, ok := streams[n]
		require.True(t, ok, "stream %s", n)
		m := mode
		if !slices.Contains(s.SupportedSyncModes, m) {
			m = synclet.SyncModeFullRefresh
		}
		dest := synclet.DestinationSyncModeOverwrite
		if m == synclet.SyncModeIncremental {
			dest = synclet.DestinationSyncModeAppendDedup
		}
		cat.Streams = append(cat.Streams, synclet.ConfiguredStream{Stream: s, SyncMode: m, DestinationSyncMode: dest})
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

func builtinNames() []string {
	var out []string
	for _, d := range builtinDefs {
		out = append(out, d.name)
	}
	return out
}

// allNames is every stream of a configuration with discoverPages.
func allNames() []string {
	return slices.Concat([]string{streamSites, streamSitemaps}, builtinNames(), []string{discoverPages.Name})
}

func TestDiscover(t *testing.T) {
	srv, h := fixtureServer(t, nil)
	src, _ := testSource(srv)
	streams, names := discoverStreams(t, src, testConfig(t, withDiscoverPages))

	assert.Equal(t, allNames(), names)
	assert.Equal(t, []string{
		"search_analytics_by_date", "search_analytics_by_query", "search_analytics_by_page", "search_analytics_by_country",
		"search_analytics_by_device", "search_analytics_by_search_appearance", "search_analytics_by_query_page",
	}, builtinNames())
	assert.Empty(t, h.keys(), "discover calls no API")

	for _, name := range names {
		assert.Equal(t, namespace, streams[name].Namespace, name)
	}
	for _, name := range []string{streamSites, streamSitemaps} {
		s := streams[name]
		assert.Equal(t, []synclet.SyncMode{synclet.SyncModeFullRefresh}, s.SupportedSyncModes, name)
		assert.False(t, s.SourceDefinedCursor, name)
	}
	assert.Equal(t, [][]string{{"site_url"}}, streams[streamSites].SourceDefinedPrimaryKey)
	assert.Equal(t, [][]string{{"site_url"}, {"path"}}, streams[streamSitemaps].SourceDefinedPrimaryKey)
	sm := schemaProps(t, streams[streamSitemaps])
	assert.Equal(t, []any{"integer", "null"}, sm["warnings"]["type"])
	assert.Equal(t, "date-time", sm["last_submitted"]["format"])
	assert.Equal(t, []string{"contents", "errors", "is_pending", "is_sitemaps_index", "last_downloaded", "last_submitted", "path", "site_url", "type", "warnings"}, sortedKeys(sm))

	for _, name := range names[2:] {
		s := streams[name]
		assert.Equal(t, []synclet.SyncMode{synclet.SyncModeFullRefresh, synclet.SyncModeIncremental}, s.SupportedSyncModes, name)
		assert.True(t, s.SourceDefinedCursor, name)
		assert.Equal(t, []string{"date"}, s.DefaultCursorField, name)
		props := schemaProps(t, s)
		assert.Equal(t, map[string]any{"type": "string", "format": "date", "description": "dimension date"}, props["date"], name)
		assert.Equal(t, "integer", props["clicks"]["type"], name)
		assert.Equal(t, "integer", props["impressions"]["type"], name)
		assert.Equal(t, "number", props["ctr"]["type"], name)
		assert.Equal(t, "number", props["position"]["type"], name)
	}

	qp := streams["search_analytics_by_query_page"]
	assert.Equal(t, [][]string{{"site_url"}, {"search_type"}, {"date"}, {"query"}, {"page"}}, qp.SourceDefinedPrimaryKey)
	assert.Equal(t, []string{"clicks", "ctr", "date", "impressions", "page", "position", "query", "search_type", "site_url"}, sortedKeys(schemaProps(t, qp)))
	sa := streams["search_analytics_by_search_appearance"]
	assert.Equal(t, [][]string{{"site_url"}, {"search_type"}, {"date"}, {"search_appearance"}}, sa.SourceDefinedPrimaryKey)
	assert.Equal(t, [][]string{{"site_url"}, {"search_type"}, {"date"}, {"page"}}, streams[discoverPages.Name].SourceDefinedPrimaryKey)
}

func TestDiscoverInvalidConfig(t *testing.T) {
	srv, _ := fixtureServer(t, nil)
	src, _ := testSource(srv)
	out, err := testkit.RunSource(t.Context(), src, synclet.Request{Command: synclet.CommandDiscover, Config: testConfig(t, func(c *Config) { c.SiteURLs = nil })})
	require.NoError(t, err)
	require.Error(t, out.Err)
	require.Len(t, out.ErrorTraces(), 1)
	assert.Equal(t, synclet.FailureTypeConfigError, out.ErrorTraces()[0].FailureType)
}

func sortedKeys[V any](m map[string]V) []string {
	return slices.Sorted(maps.Keys(m))
}
