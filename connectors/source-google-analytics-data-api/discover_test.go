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

// configured returns a catalog of the named streams in mode.
func configured(t *testing.T, src Source, cfg json.RawMessage, mode synclet.SyncMode, names ...string) *synclet.ConfiguredCatalog {
	t.Helper()
	streams, _ := discoverStreams(t, src, cfg)
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

func builtinNames() []string {
	var out []string
	for _, d := range builtinDefs {
		out = append(out, d.name)
	}
	return out
}

func TestDiscover(t *testing.T) {
	srv, h := fixtureServer(t, nil)
	src, _ := testSource(srv)
	streams, names := discoverStreams(t, src, testConfig(t, withMyReport))

	assert.Equal(t, append(builtinNames(), "my_report"), names)
	assert.Equal(t, []string{
		"website_overview", "daily_active_users", "traffic_sources", "pages", "events", "devices", "locations", "key_events",
	}, builtinNames())
	assert.Equal(t, []string{"POST token", "GET properties/123456789/metadata", "GET properties/987654321/metadata"}, h.keys())

	for _, name := range names {
		s := streams[name]
		assert.Equal(t, namespace, s.Namespace, name)
		assert.Equal(t, []synclet.SyncMode{synclet.SyncModeFullRefresh, synclet.SyncModeIncremental}, s.SupportedSyncModes, name)
		assert.True(t, s.SourceDefinedCursor, name)
		assert.Equal(t, []string{"date"}, s.DefaultCursorField, name)
		props := schemaProps(t, s)
		assert.Equal(t, map[string]any{"type": "string", "format": "date", "description": "dimension date"}, props["date"], name)
		assert.Equal(t, []any{"number", "null"}, props["sampling_ratio"]["type"], name)
		for _, k := range []string{"sampled", "subject_to_thresholding", "data_loss_from_other_row"} {
			assert.Equal(t, "boolean", props[k]["type"], "%s %s", name, k)
		}
	}

	custom := streams["my_report"]
	assert.Equal(t, [][]string{{"property_id"}, {"date"}, {"customEvent_plan_tier"}, {"country"}}, custom.SourceDefinedPrimaryKey)
	props := schemaProps(t, custom)
	assert.Equal(t, map[string]any{"type": "string", "description": "dimension customEvent:plan_tier"}, props["customEvent_plan_tier"])
	assert.Equal(t, "number", props["totalRevenue"]["type"])
	assert.Equal(t, "integer", props["sessions"]["type"])
	assert.Equal(t, "string", props["property_id"]["type"])
	assert.Len(t, props, 1+3+2+4)

	pages := streams["pages"]
	assert.Equal(t, [][]string{{"property_id"}, {"date"}, {"hostName"}, {"pagePathPlusQueryString"}}, pages.SourceDefinedPrimaryKey)
	pp := schemaProps(t, pages)
	assert.Equal(t, "integer", pp["screenPageViews"]["type"])
	assert.Equal(t, "number", pp["bounceRate"]["type"])
	overview := schemaProps(t, streams["website_overview"])
	assert.Equal(t, "number", overview["averageSessionDuration"]["type"], "TYPE_SECONDS is a number")
	assert.Equal(t, "integer", overview["totalUsers"]["type"])
}

func TestDiscoverWithoutDataQualityFields(t *testing.T) {
	srv, _ := fixtureServer(t, nil)
	src, _ := testSource(srv)
	streams, _ := discoverStreams(t, src, testConfig(t, func(c *Config) { c.DataQualityFields = boolp(false) }))
	props := schemaProps(t, streams["daily_active_users"])
	assert.Equal(t, []string{"active1DayUsers", "date", "property_id"}, sortedKeys(props))
}

func TestDiscoverMetricTypeMismatch(t *testing.T) {
	srv, _ := fixtureServer(t, map[string][]fixture{
		"GET properties/" + testPropB + "/metadata": {{file: "responses/metadata_revenue_integer"}},
	})
	src, _ := testSource(srv)
	out, err := testkit.RunSource(t.Context(), src, synclet.Request{Command: synclet.CommandDiscover, Config: testConfig(t, withMyReport)})
	require.NoError(t, err)
	require.Error(t, out.Err)
	assert.Equal(t, "custom report my_report: metric totalRevenue is number in property 123456789 and integer in property 987654321", out.Err.Error())
	require.Len(t, out.ErrorTraces(), 1)
	assert.Equal(t, synclet.FailureTypeConfigError, out.ErrorTraces()[0].FailureType)
}

func sortedKeys[V any](m map[string]V) []string {
	return slices.Sorted(maps.Keys(m))
}
