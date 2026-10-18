package main

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
)

// catalogStream is the part of a stream testdata/expected_catalog.json pins.
type catalogStream struct {
	Name       string                 `json:"name"`
	Namespace  string                 `json:"namespace"`
	SyncModes  []string               `json:"sync_modes"`
	PrimaryKey [][]string             `json:"primary_key"`
	Cursor     []string               `json:"cursor"`
	Properties map[string]catalogProp `json:"properties"`
}

type catalogProp struct {
	Type   any    `json:"type"`
	Format string `json:"format,omitempty"`
}

func toCatalogStream(t *testing.T, s synclet.Stream) catalogStream {
	t.Helper()
	var schema struct {
		Properties map[string]catalogProp `json:"properties"`
	}
	require.NoError(t, json.Unmarshal(s.JSONSchema, &schema))
	c := catalogStream{Name: s.Name, Namespace: s.Namespace, PrimaryKey: s.SourceDefinedPrimaryKey, Cursor: s.DefaultCursorField, Properties: schema.Properties}
	if c.Cursor == nil {
		c.Cursor = []string{}
	}
	for _, m := range s.SupportedSyncModes {
		c.SyncModes = append(c.SyncModes, string(m))
	}
	return c
}

func TestDiscover(t *testing.T) {
	raw, err := os.ReadFile("testdata/expected_catalog.json")
	require.NoError(t, err)
	var expected struct {
		Streams []catalogStream `json:"streams"`
	}
	require.NoError(t, json.Unmarshal(raw, &expected))
	require.Len(t, expected.Streams, 8)

	srv, h := fixtureServer(t, nil)
	src, _ := testSource(srv)
	streams := discoverStreams(t, src)
	assert.Len(t, streams, 8)
	for _, want := range expected.Streams {
		got, ok := streams[want.Name]
		if !assert.True(t, ok, "stream %s is missing", want.Name) {
			continue
		}
		assert.Equal(t, want, toCatalogStream(t, got), want.Name)
		assert.Equal(t, len(want.Cursor) > 0, got.SourceDefinedCursor, "%s: a report cursor is source-defined", want.Name)
	}
	assert.Empty(t, h.list(), "discover sends no request")
}
