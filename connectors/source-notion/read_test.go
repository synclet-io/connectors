package main

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
	"github.com/synclet-io/synclet-go-sdk/testkit"
)

func catalogOf(t *testing.T, src Source, mode synclet.SyncMode, names ...string) *synclet.ConfiguredCatalog {
	t.Helper()
	streams := discoverStreams(t, src)
	cat := &synclet.ConfiguredCatalog{}
	for _, n := range names {
		s, ok := streams[n]
		require.True(t, ok, n)
		cs := synclet.ConfiguredStream{Stream: s, SyncMode: mode, DestinationSyncMode: synclet.DestinationSyncModeAppendDedup}
		if mode == synclet.SyncModeIncremental {
			cs.CursorField = s.DefaultCursorField
		}
		cat.Streams = append(cat.Streams, cs)
	}
	return cat
}

func streamState(name, state string) json.RawMessage {
	return json.RawMessage(`[{"type":"STREAM","stream":{"stream_descriptor":{"name":"` + name + `"},"stream_state":` + state + `}}]`)
}

func runRead(t *testing.T, src Source, config string, catalog *synclet.ConfiguredCatalog, state json.RawMessage) *testkit.Output {
	t.Helper()
	raw, err := json.Marshal(catalog)
	require.NoError(t, err)
	out, err := testkit.RunSource(t.Context(), src, synclet.Request{
		Command: synclet.CommandRead, Config: json.RawMessage(config), Catalog: raw, State: state,
	})
	require.NoError(t, err)
	return out
}

func records(t *testing.T, out *testkit.Output, stream string) []map[string]any {
	t.Helper()
	var recs []map[string]any
	for _, r := range out.Records(synclet.StreamDescriptor{Name: stream}) {
		var rec map[string]any
		require.NoError(t, json.Unmarshal(r.Data, &rec))
		recs = append(recs, rec)
	}
	return recs
}

func ids(recs []map[string]any) []string {
	var out []string
	for _, r := range recs {
		out = append(out, r["id"].(string))
	}
	return out
}

// states returns the stream_state of every STATE message, in order.
func states(out *testkit.Output) []string {
	var s []string
	for _, st := range out.States() {
		s = append(s, string(st.Stream.StreamState))
	}
	return s
}

func cursors(hs []hit) []string {
	var out []string
	for _, h := range hs {
		out = append(out, h.cursor)
	}
	return out
}

func TestReadUsers(t *testing.T) {
	srv, h := fixtureServer(t, nil)
	src := testSource(srv)
	out := runRead(t, src, testConfig, catalogOf(t, src, synclet.SyncModeFullRefresh, usersStream), nil)
	require.NoError(t, out.Err)
	recs := records(t, out, usersStream)
	assert.Equal(t, []string{"00000000-0000-4000-8000-000000000001", "00000000-0000-4000-8000-0000000000b0", "00000000-0000-4000-8000-000000000002"}, ids(recs))
	assert.Equal(t, map[string]any{"email": "ada@example.com"}, recs[0]["person"])
	assert.Equal(t, []string{"", "cursor-u2"}, cursors(h.to("users")))
	assert.Empty(t, states(out))
}

func TestReadDatabases(t *testing.T) {
	srv, h := fixtureServer(t, nil)
	src := testSource(srv)
	out := runRead(t, src, testConfig, catalogOf(t, src, synclet.SyncModeFullRefresh, databasesStream), nil)
	require.NoError(t, out.Err)
	recs := records(t, out, databasesStream)
	assert.Equal(t, []string{dsA, dsB}, ids(recs))
	assert.Equal(t, dbA, recs[0]["database_id"])
	assert.Contains(t, recs[0]["properties"], "Amount")
	assert.Equal(t, []string{"", "cursor-d2"}, cursors(h.to("search")))
	for _, s := range h.to("search") {
		assert.Equal(t, map[string]any{"property": "object", "value": "data_source"}, s.body["filter"])
		assert.Equal(t, float64(100), s.body["page_size"])
	}
}

func TestReadPages(t *testing.T) {
	srv, h := fixtureServer(t, nil)
	src := testSource(srv)
	out := runRead(t, src, testConfig, catalogOf(t, src, synclet.SyncModeFullRefresh, pagesStream), nil)
	require.NoError(t, out.Err)
	recs := records(t, out, pagesStream)
	// Rows of the shared data sources come from their queries; search adds
	// the other pages, among them a row of a data source not shared, once.
	assert.Equal(t, []string{rowA2, rowA1, rowB1, pageP1, pagePU, pageP2, pageOld}, ids(recs))

	row := recs[1]
	props := row["properties"].(map[string]any)
	assert.Equal(t, map[string]any{"type": "number", "id": "am%3D1", "number": float64(10)}, props["Amount"])
	assert.Equal(t, map[string]any{"type": "number", "number": float64(20)}, props["Total"].(map[string]any)["formula"])
	assert.Equal(t, "data_source_id", row["parent"].(map[string]any)["type"])

	var searchCursors []string
	for _, s := range h.to("search") {
		if s.body["filter"].(map[string]any)["value"] == "page" {
			searchCursors = append(searchCursors, s.cursor)
			assert.Equal(t, map[string]any{"direction": "descending", "timestamp": "last_edited_time"}, s.body["sort"])
		}
	}
	assert.Equal(t, []string{"", "cursor-p2"}, searchCursors)
	queries := h.to("data_sources/" + dsA + "/query")
	assert.Equal(t, []string{"", "cursor-q2"}, cursors(queries))
	assert.NotContains(t, queries[0].body, "filter")
	assert.Equal(t, []any{map[string]any{"timestamp": "last_edited_time", "direction": "ascending"}}, queries[0].body["sorts"])
	assert.Len(t, h.to("data_sources/"+dsB+"/query"), 1)
	assert.Empty(t, states(out))
}

func TestReadBlocks(t *testing.T) {
	srv, h := fixtureServer(t, nil)
	src := testSource(srv)
	out := runRead(t, src, testConfig, catalogOf(t, src, synclet.SyncModeFullRefresh, blocksStream), nil)
	require.NoError(t, out.Err)
	recs := records(t, out, blocksStream)
	assert.Equal(t, []string{blockB1, block11, blockBA, pageP2, blockB3, blockB4}, ids(recs))
	depths := map[string]float64{}
	for _, r := range recs {
		depths[r["id"].(string)] = r["depth"].(float64)
		assert.Equal(t, pageP1, r["page_id"])
	}
	assert.Equal(t, map[string]float64{blockB1: 1, block11: 2, blockBA: 3, pageP2: 1, blockB3: 1, blockB4: 1}, depths)
	assert.Equal(t, "Goals for the quarter", recs[0]["content"].(map[string]any)["rich_text"].([]any)[0].(map[string]any)["plain_text"])
	assert.Equal(t, "child_page", recs[3]["type"])
	assert.Equal(t, map[string]any{"title": "Q4 notes"}, recs[3]["content"])

	assert.Equal(t, []string{"", "cursor-b2"}, cursors(h.to("blocks/"+pageP1+"/children")))
	assert.Len(t, h.to("blocks/"+blockB1+"/children"), 1)
	assert.Len(t, h.to("blocks/"+block11+"/children"), 1)
	// The child page's blocks are read once, as the blocks of page P2, and
	// the synced copy's children are not read.
	assert.Len(t, h.to("blocks/"+pageP2+"/children"), 1)
	assert.Empty(t, h.to("blocks/"+blockB3+"/children"))
	for _, p := range []string{pagePU, pageOld, rowA1, rowA2, rowB1} {
		assert.Len(t, h.to("blocks/"+p+"/children"), 1, p)
	}
}

func TestReadBlocksLimits(t *testing.T) {
	tests := []struct {
		name   string
		config string
		err    string
	}{
		{
			name:   "depth",
			config: `{"access_token":"` + testToken + `","max_block_depth":2}`,
			err:    "stream blocks: page " + pageP1 + ": block " + block11 + " at depth 2 has children below max_block_depth (2): raise max_block_depth",
		},
		{
			name:   "blocks per page",
			config: `{"access_token":"` + testToken + `","max_blocks_per_page":4}`,
			err:    "stream blocks: page " + pageP1 + " has more than max_blocks_per_page (4) blocks; block " + blockB3 + " is over the limit: raise max_blocks_per_page",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv, _ := fixtureServer(t, nil)
			src := testSource(srv)
			out := runRead(t, src, tt.config, catalogOf(t, src, synclet.SyncModeIncremental, blocksStream), nil)
			require.Error(t, out.Err)
			assert.Equal(t, tt.err, out.Err.Error())
			// Only the checkpoints after the two data sources precede the
			// failure; no state covers the failing page.
			for _, s := range states(out) {
				assert.NotContains(t, s, `"last_edited_time"`)
			}
			assert.Len(t, states(out), 2)
		})
	}
}

func TestIncremental(t *testing.T) {
	const saved = `{"last_edited_time":"2026-10-18T10:30:00Z","data_sources":{"` + dsA + `":"2026-10-18T08:00:00Z"}}`

	t.Run("pages", func(t *testing.T) {
		srv, h := fixtureServer(t, nil)
		src := testSource(srv)
		out := runRead(t, src, testConfig, catalogOf(t, src, synclet.SyncModeIncremental, pagesStream), streamState(pagesStream, saved))
		require.NoError(t, out.Err)
		assert.Equal(t, []string{rowA2, rowA1, rowB1, pageP1}, ids(records(t, out, pagesStream)))

		qa := h.to("data_sources/" + dsA + "/query")
		require.Len(t, qa, 2)
		for _, q := range qa {
			assert.Equal(t, map[string]any{"timestamp": "last_edited_time", "last_edited_time": map[string]any{"on_or_after": "2026-10-18T07:55:00Z"}}, q.body["filter"])
		}
		qb := h.to("data_sources/" + dsB + "/query")
		require.Len(t, qb, 1)
		assert.NotContains(t, qb[0].body, "filter", "a data source without a saved cursor is read in full")

		var pageSearch []string
		for _, s := range h.to("search") {
			if s.body["filter"].(map[string]any)["value"] == "page" {
				pageSearch = append(pageSearch, s.cursor)
			}
		}
		assert.Equal(t, []string{""}, pageSearch, "search stops at the first page edited before the lookback window")

		assert.Equal(t, []string{
			`{"last_edited_time":"2026-10-18T10:30:00Z","data_sources":{"` + dsA + `":"2026-10-18T10:30:00Z"}}`,
			`{"last_edited_time":"2026-10-18T10:30:00Z","data_sources":{"` + dsA + `":"2026-10-18T10:30:00Z","` + dsB + `":"2026-10-18T07:00:00Z"}}`,
			`{"last_edited_time":"2026-10-18T11:00:00Z","data_sources":{"` + dsA + `":"2026-10-18T10:30:00Z","` + dsB + `":"2026-10-18T07:00:00Z"}}`,
		}, states(out))
	})

	t.Run("pages lookback 0", func(t *testing.T) {
		srv, h := fixtureServer(t, nil)
		src := testSource(srv)
		cfg := `{"access_token":"` + testToken + `","lookback_minutes":0}`
		out := runRead(t, src, cfg, catalogOf(t, src, synclet.SyncModeIncremental, pagesStream), streamState(pagesStream, saved))
		require.NoError(t, out.Err)
		assert.Equal(t, []string{rowA2, rowA1, rowB1, pageP1}, ids(records(t, out, pagesStream)))
		assert.Equal(t, "2026-10-18T08:00:00Z", h.to("data_sources/" + dsA + "/query")[0].body["filter"].(map[string]any)["last_edited_time"].(map[string]any)["on_or_after"])
	})

	t.Run("blocks", func(t *testing.T) {
		srv, h := fixtureServer(t, nil)
		src := testSource(srv)
		out := runRead(t, src, testConfig, catalogOf(t, src, synclet.SyncModeIncremental, blocksStream), streamState(blocksStream, saved))
		require.NoError(t, out.Err)
		// Only blocks of pages edited since, and of those only blocks
		// edited at or after 10:25; the walk still reaches every level.
		assert.Equal(t, []string{blockB1, blockBA, blockB4}, ids(records(t, out, blocksStream)))
		assert.Len(t, h.to("blocks/"+block11+"/children"), 1)
		assert.Empty(t, h.to("blocks/"+pagePU+"/children"))
		assert.Empty(t, h.to("blocks/"+pageP2+"/children"))
		st := states(out)
		require.Len(t, st, 3)
		assert.Contains(t, st[2], `"last_edited_time":"2026-10-18T11:00:00Z"`)
	})

	t.Run("databases", func(t *testing.T) {
		srv, _ := fixtureServer(t, nil)
		src := testSource(srv)
		out := runRead(t, src, testConfig, catalogOf(t, src, synclet.SyncModeIncremental, databasesStream),
			streamState(databasesStream, `{"last_edited_time":"2026-10-17T12:00:00Z"}`))
		require.NoError(t, out.Err)
		assert.Equal(t, []string{dsA}, ids(records(t, out, databasesStream)))
		assert.Equal(t, []string{`{"last_edited_time":"2026-10-17T12:00:00Z"}`}, states(out))
	})

	t.Run("first read", func(t *testing.T) {
		srv, _ := fixtureServer(t, nil)
		src := testSource(srv)
		out := runRead(t, src, testConfig, catalogOf(t, src, synclet.SyncModeIncremental, pagesStream), nil)
		require.NoError(t, out.Err)
		assert.Len(t, records(t, out, pagesStream), 7)
		st := states(out)
		assert.Equal(t, `{"last_edited_time":"2026-10-18T11:00:00Z","data_sources":{"`+dsA+`":"2026-10-18T10:30:00Z","`+dsB+`":"2026-10-18T07:00:00Z"}}`, st[len(st)-1])
	})

	t.Run("bad state", func(t *testing.T) {
		srv, _ := fixtureServer(t, nil)
		src := testSource(srv)
		out := runRead(t, src, testConfig, catalogOf(t, src, synclet.SyncModeIncremental, pagesStream), streamState(pagesStream, `{"last_edited_time":"yesterday"}`))
		require.Error(t, out.Err)
		assert.Contains(t, out.Err.Error(), `state of pages: "yesterday" is not an RFC 3339 time`)
	})
}

func TestReadUsersIncrementalRejected(t *testing.T) {
	srv, _ := fixtureServer(t, nil)
	src := testSource(srv)
	cat := catalogOf(t, src, synclet.SyncModeFullRefresh, usersStream)
	cat.Streams[0].SyncMode = synclet.SyncModeIncremental
	out := runRead(t, src, testConfig, cat, nil)
	require.Error(t, out.Err)
	assert.Equal(t, "stream users supports full refresh only", out.Err.Error())
}
