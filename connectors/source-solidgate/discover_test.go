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

const testConfig = `{"accounts":[{"name":"acme_test","public_key":"` + testPublicKey + `","secret_key":"` + testSecretKey + `"}],"start_date":"2026-10-16"}`

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
// the streams of the earlier private connector this one replaces.
type parityStream struct {
	Name       string                `json:"name"`
	Namespace  string                `json:"namespace"`
	SyncModes  []string              `json:"sync_modes"`
	PrimaryKey [][]string            `json:"primary_key"`
	Cursor     []string              `json:"cursor"`
	Properties map[string]parityProp `json:"properties"`
}

type parityProp struct {
	Type        synclet.PropTypes `json:"type"`
	Format      string            `json:"format,omitempty"`
	AirbyteType string            `json:"airbyte_type,omitempty"`
}

func toParity(t *testing.T, s synclet.Stream) parityStream {
	t.Helper()
	var schema struct {
		Properties map[string]parityProp `json:"properties"`
	}
	require.NoError(t, json.Unmarshal(s.JSONSchema, &schema))
	p := parityStream{Name: s.Name, Namespace: s.Namespace, PrimaryKey: s.SourceDefinedPrimaryKey, Cursor: s.DefaultCursorField, Properties: schema.Properties}
	if p.Cursor == nil {
		p.Cursor = []string{}
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
	require.Len(t, expected.Streams, 9)

	srv, _ := fixtureServer(t, nil)
	streams := discoverStreams(t, testSource(srv))
	for _, want := range expected.Streams {
		got, ok := streams[want.Name]
		if !assert.True(t, ok, "stream %s is missing", want.Name) {
			continue
		}
		assert.Equal(t, want, toParity(t, got), want.Name)
	}
	assert.Len(t, streams, 9)
}
