package main

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
)

// parityStream is the part of a stream testdata/expected_catalog.json pins:
// the streams of the earlier private connector this one replaces.
type parityStream struct {
	Name       string            `json:"name"`
	Namespace  string            `json:"namespace"`
	SyncModes  []string          `json:"sync_modes"`
	PrimaryKey [][]string        `json:"primary_key"`
	Cursor     []string          `json:"cursor"`
	Properties map[string]string `json:"properties"`
}

func toParity(t *testing.T, s synclet.Stream) parityStream {
	t.Helper()
	var schema struct {
		Properties map[string]struct {
			Type string `json:"type"`
		} `json:"properties"`
	}
	require.NoError(t, json.Unmarshal(s.JSONSchema, &schema))
	p := parityStream{Name: s.Name, Namespace: s.Namespace, PrimaryKey: s.SourceDefinedPrimaryKey, Cursor: s.DefaultCursorField, Properties: map[string]string{}}
	if p.Cursor == nil {
		p.Cursor = []string{}
	}
	for _, m := range s.SupportedSyncModes {
		p.SyncModes = append(p.SyncModes, string(m))
	}
	for name, prop := range schema.Properties {
		p.Properties[name] = prop.Type
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
	require.Len(t, expected.Streams, 8)

	srv, _ := fixtureServer(t, nil)
	src, _ := testSource(srv)
	streams := discoverStreams(t, src)
	for _, want := range expected.Streams {
		got, ok := streams[want.Name]
		if !assert.True(t, ok, "stream %s is missing", want.Name) {
			continue
		}
		assert.Equal(t, want, toParity(t, got), want.Name)
	}
	for _, added := range []string{streamAdAccounts, streamCampaigns, streamAdSets, streamAds, streamAdCreatives} {
		assert.Contains(t, streams, added)
	}
	assert.Len(t, streams, 13)
}
