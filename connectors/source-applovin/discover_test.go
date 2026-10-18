package main

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
	"github.com/synclet-io/synclet-go-sdk/testkit"
)

const testConfig = `{"api_key":"` + testReportKey + `","start_date":"2026-10-14"}`

// discoverStreams returns the discovered streams by name.
func discoverStreams(t *testing.T, src Source) map[string]synclet.Stream {
	t.Helper()
	out, err := testkit.RunSource(t.Context(), src, synclet.Request{Command: synclet.CommandDiscover, Config: json.RawMessage(testConfig)})
	require.NoError(t, err)
	require.NoError(t, out.Err)
	streams := map[string]synclet.Stream{}
	for _, s := range out.Catalog().Streams {
		streams[s.Name] = s
	}
	return streams
}

// configured returns a catalog of names in mode.
func configured(t *testing.T, src Source, mode synclet.SyncMode, names ...string) *synclet.ConfiguredCatalog {
	t.Helper()
	streams := discoverStreams(t, src)
	cat := &synclet.ConfiguredCatalog{}
	for _, name := range names {
		st, ok := streams[name]
		require.True(t, ok, name)
		dst := synclet.DestinationSyncModeAppendDedup
		if mode == synclet.SyncModeFullRefresh {
			dst = synclet.DestinationSyncModeOverwrite
		}
		cat.Streams = append(cat.Streams, synclet.ConfiguredStream{Stream: st, SyncMode: mode, DestinationSyncMode: dst})
	}
	return cat
}

// parityStream is the part of a stream testdata/expected_catalog.json pins:
// advertiser_report as the earlier private connector discovered it and the
// added max_revenue_report, each property's whole schema with descriptions
// left out.
type parityStream struct {
	Name                 string         `json:"name"`
	Namespace            string         `json:"namespace"`
	SyncModes            []string       `json:"sync_modes"`
	PrimaryKey           [][]string     `json:"primary_key"`
	Cursor               []string       `json:"cursor"`
	AdditionalProperties any            `json:"additional_properties"`
	Properties           map[string]any `json:"properties"`
}

// withoutDescriptions returns v with every description key removed, nested
// ones included.
func withoutDescriptions(v any) any {
	switch x := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(x))
		for k, e := range x {
			if k != "description" {
				out[k] = withoutDescriptions(e)
			}
		}
		return out
	case []any:
		out := make([]any, len(x))
		for i, e := range x {
			out[i] = withoutDescriptions(e)
		}
		return out
	}
	return v
}

func toParity(t *testing.T, s synclet.Stream) parityStream {
	t.Helper()
	var schema struct {
		AdditionalProperties any            `json:"additionalProperties"`
		Properties           map[string]any `json:"properties"`
	}
	require.NoError(t, json.Unmarshal(s.JSONSchema, &schema))
	p := parityStream{
		Name: s.Name, Namespace: s.Namespace, PrimaryKey: s.SourceDefinedPrimaryKey, Cursor: s.DefaultCursorField,
		AdditionalProperties: schema.AdditionalProperties,
		Properties:           withoutDescriptions(schema.Properties).(map[string]any),
	}
	for _, m := range s.SupportedSyncModes {
		p.SyncModes = append(p.SyncModes, string(m))
	}
	return p
}

func TestCatalogParity(t *testing.T) {
	raw, err := os.ReadFile("testdata/expected_catalog.json")
	require.NoError(t, err)
	var expected struct {
		Streams []parityStream `json:"streams"`
	}
	require.NoError(t, json.Unmarshal(raw, &expected))
	require.Len(t, expected.Streams, 2)

	srv, _ := fixtureServer(t, nil)
	streams := discoverStreams(t, testSource(srv))
	for _, want := range expected.Streams {
		got, ok := streams[want.Name]
		if !assert.True(t, ok, "stream %s is missing", want.Name) {
			continue
		}
		assert.Equal(t, want, toParity(t, got), want.Name)
		assert.True(t, got.SourceDefinedCursor, want.Name)
	}
	assert.Len(t, streams, 2)
}

// TestSchemaShape pins the schema style of the earlier connector: only
// properties at the root, one type per property in an array, and a fresh
// description on every property.
func TestSchemaShape(t *testing.T) {
	for _, d := range streamDefs {
		var schema map[string]map[string]map[string]any
		require.NoError(t, json.Unmarshal(d.stream().JSONSchema, &schema))
		assert.Len(t, schema, 1, d.name)
		for name, p := range schema["properties"] {
			assert.Len(t, p["type"], 1, "%s.%s", d.name, name)
			assert.NotEmpty(t, p["description"], "%s.%s", d.name, name)
		}
	}
}

func TestCohortColumns(t *testing.T) {
	var names []string
	for _, c := range cohortColumns() {
		names = append(names, c.name)
		assert.Equal(t, typeNumber, c.typ, c.name)
	}
	var want []string
	for _, p := range []string{"ad_rev", "ad_roas", "iap_rev", "iap_roas", "roas", "total_rev"} {
		for _, s := range []string{"0d", "1d", "2d", "3d", "7d", "14d", "28d", "30d", "90d", "1y"} {
			want = append(want, p+"_"+s)
		}
	}
	for _, p := range []string{"cost_per_target_event", "cpp", "sales", "target_event_count", "unique_purchasers"} {
		for _, s := range []string{"0d", "1d", "2d", "3d", "7d", "14d", "30d", "90d", "1y"} {
			want = append(want, p+"_"+s)
		}
	}
	for _, s := range []string{"1d", "3d", "7d", "14d", "28d"} {
		want = append(want, "ret_"+s)
	}
	assert.Equal(t, want, names)
	assert.Len(t, advertiserColumns(), 147)
}
