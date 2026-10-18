package main

import (
	"encoding/json"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
	"github.com/synclet-io/synclet-go-sdk/testkit"
)

const testConfig = `{"access_token":"` + testToken + `"}`

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

func schemaKeys(t *testing.T, s synclet.Stream) []string {
	t.Helper()
	var schema struct {
		Properties map[string]json.RawMessage `json:"properties"`
	}
	require.NoError(t, json.Unmarshal(s.JSONSchema, &schema))
	var out []string
	for k := range schema.Properties {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func TestDiscoverStreams(t *testing.T) {
	srv, h := fixtureServer(t, nil)
	streams := discoverStreams(t, testSource(srv))
	assert.Empty(t, h.list())
	require.Len(t, streams, 4)

	users := streams[usersStream]
	assert.Equal(t, []synclet.SyncMode{synclet.SyncModeFullRefresh}, users.SupportedSyncModes)
	assert.Equal(t, [][]string{{"id"}}, users.SourceDefinedPrimaryKey)
	assert.Empty(t, users.DefaultCursorField)
	assert.Equal(t, []string{"avatar_url", "bot", "id", "name", "object", "person", "type"}, schemaKeys(t, users))

	audit := []string{"archived", "created_by", "created_time", "in_trash", "last_edited_by", "last_edited_time"}
	want := map[string][]string{
		databasesStream: {"cover", "database_id", "database_parent", "description", "icon", "id", "is_inline", "object", "parent", "properties", "public_url", "title", "url"},
		pagesStream:     {"cover", "icon", "id", "is_locked", "object", "parent", "properties", "public_url", "url"},
		blocksStream:    {"content", "depth", "has_children", "id", "object", "page_id", "parent", "type"},
	}
	for name, cols := range want {
		s := streams[name]
		assert.Equal(t, []synclet.SyncMode{synclet.SyncModeFullRefresh, synclet.SyncModeIncremental}, s.SupportedSyncModes, name)
		assert.True(t, s.SourceDefinedCursor, name)
		assert.Equal(t, []string{"last_edited_time"}, s.DefaultCursorField, name)
		assert.Equal(t, [][]string{{"id"}}, s.SourceDefinedPrimaryKey, name)
		all := append(append([]string{}, cols...), audit...)
		sort.Strings(all)
		assert.Equal(t, all, schemaKeys(t, s), name)
	}

	var blocks struct {
		Properties map[string]map[string]any `json:"properties"`
	}
	require.NoError(t, json.Unmarshal(streams[blocksStream].JSONSchema, &blocks))
	assert.Equal(t, []any{"null", "integer"}, blocks.Properties["depth"]["type"])
	assert.Equal(t, []any{"null", "object"}, blocks.Properties["content"]["type"])
	assert.Equal(t, "timestamp_with_timezone", blocks.Properties["last_edited_time"]["airbyte_type"])
}
