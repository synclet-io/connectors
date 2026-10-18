package main

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
	"github.com/synclet-io/synclet-go-sdk/testkit"
)

// modifiedA is the Drive modifiedTime of spreadsheet A in testdata.
const modifiedA = "2026-10-18T09:15:00Z"

func streamState(t *testing.T, desc synclet.StreamDescriptor, modified string) json.RawMessage {
	t.Helper()
	raw, err := json.Marshal([]synclet.StateMessage{{
		Type: synclet.StateTypeStream,
		Stream: &synclet.StreamState{
			StreamDescriptor: desc,
			StreamState:      json.RawMessage(`{"spreadsheet_modified_time":"` + modified + `"}`),
		},
	}})
	require.NoError(t, err)
	return raw
}

func states(t *testing.T, out *testkit.Output) []string {
	t.Helper()
	var ss []string
	for _, s := range out.States() {
		require.NotNil(t, s.Stream)
		ss = append(ss, s.Stream.StreamDescriptor.Name+" "+string(s.Stream.StreamState))
	}
	return ss
}

func TestIncrementalDiscover(t *testing.T) {
	srv, h := fixtureServer(t, nil)
	src, _ := testSource(srv)
	out := runDiscover(t, src, testConfig(t, withIncremental))
	require.NoError(t, out.Err)
	for _, s := range out.Catalog().Streams {
		assert.Equal(t, []synclet.SyncMode{synclet.SyncModeFullRefresh, synclet.SyncModeIncremental}, s.SupportedSyncModes, s.Name)
		assert.True(t, s.SourceDefinedCursor, s.Name)
		assert.Equal(t, []string{cursorColumn}, s.DefaultCursorField, s.Name)
	}
	assert.JSONEq(t, `{"type":"object","properties":{
		"Full Name":{"type":["null","string"]},"Age":{"type":["null","string"]},"E-mail":{"type":["null","string"]},
		"_spreadsheet_modified_time":{"type":"string","airbyte_type":"timestamp_with_timezone","format":"date-time"}}}`,
		string(out.Catalog().Streams[0].JSONSchema))
	assert.Contains(t, h.keys(), driveKey(testSheetA))
}

func TestIncrementalDiscoverWithoutDrive(t *testing.T) {
	srv, _ := fixtureServer(t, map[string][]fixture{driveKey(testSheetA): {{status: http.StatusNotFound, file: "errors/drive_not_found"}}})
	src, _ := testSource(srv)
	out := runDiscover(t, src, testConfig(t, withIncremental))
	require.NoError(t, out.Err)
	for _, s := range out.Catalog().Streams {
		assert.Equal(t, []synclet.SyncMode{synclet.SyncModeFullRefresh}, s.SupportedSyncModes, s.Name)
		assert.NotContains(t, string(s.JSONSchema), cursorColumn)
	}
	assert.Contains(t, warnings(out), "spreadsheet "+testSheetA+": Google Drive does not return its modifiedTime, which incremental sync needs (grant the https://www.googleapis.com/auth/drive.metadata.readonly scope and access to the file): /drive/v3/files/"+testSheetA+": HTTP 404 notFound: File not found: .; its streams offer full refresh only")
}

func TestIncremental(t *testing.T) {
	people := stream("People")
	tests := []struct {
		name      string
		state     json.RawMessage
		overrides map[string][]fixture
		records   int
		rowReads  []string
		states    []string
		err       string
	}{
		{
			name:     "first read saves modifiedTime",
			records:  4,
			rowReads: []string{"'People'!1:1", "'People'!2:7"},
			states:   []string{`People {"spreadsheet_modified_time":"2026-10-18T09:15:00Z"}`},
		},
		{
			name:     "unchanged spreadsheet is skipped",
			state:    streamState(t, people, "2026-10-18T09:15:00.000Z"),
			rowReads: nil,
			states:   []string{`People {"spreadsheet_modified_time":"2026-10-18T09:15:00.000Z"}`},
		},
		{
			name:     "saved time after modifiedTime is skipped",
			state:    streamState(t, people, "2026-10-18T10:00:00Z"),
			rowReads: nil,
			states:   []string{`People {"spreadsheet_modified_time":"2026-10-18T10:00:00Z"}`},
		},
		{
			name:     "moved modifiedTime reads the whole sheet",
			state:    streamState(t, people, "2026-10-17T00:00:00Z"),
			records:  4,
			rowReads: []string{"'People'!1:1", "'People'!2:7"},
			states:   []string{`People {"spreadsheet_modified_time":"2026-10-18T09:15:00Z"}`},
		},
		{
			name:      "Drive forbidden fails the read",
			overrides: map[string][]fixture{driveKey(testSheetA): {{status: http.StatusForbidden, file: "errors/drive_scope_insufficient"}}},
			err:       "spreadsheet " + testSheetA + ": Google Drive does not return its modifiedTime, which incremental sync needs",
		},
		{
			name:  "bad state",
			state: streamState(t, people, "yesterday"),
			err:   "state of People: spreadsheet_modified_time",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			discoverSrv, _ := fixtureServer(t, nil)
			discoverSrc, _ := testSource(discoverSrv)
			cfg := testConfig(t, withIncremental)
			cat := configured(t, discoverSrc, cfg, synclet.SyncModeIncremental, "People")

			srv, h := fixtureServer(t, tt.overrides)
			src, _ := testSource(srv)
			out := runRead(t, src, cfg, cat, tt.state)
			if tt.err != "" {
				require.Error(t, out.Err)
				assert.Contains(t, out.Err.Error(), tt.err)
				assert.Empty(t, out.Records(people))
				return
			}
			require.NoError(t, out.Err)
			assert.Equal(t, tt.rowReads, h.ranges(testSheetA))
			recs := records(t, out, people)
			assert.Len(t, recs, tt.records)
			for _, r := range recs {
				assert.Equal(t, modifiedA, r[cursorColumn])
			}
			assert.Equal(t, tt.states, states(t, out))
			assert.Equal(t, driveKey(testSheetA), h.keys()[1], "modifiedTime is read before any sheet")
		})
	}
}

func TestIncrementalFullRefreshCarriesCursor(t *testing.T) {
	srv, _ := fixtureServer(t, nil)
	src, _ := testSource(srv)
	cfg := testConfig(t, withIncremental)
	out := runRead(t, src, cfg, configured(t, src, cfg, synclet.SyncModeFullRefresh, "People"), nil)
	require.NoError(t, out.Err)
	recs := records(t, out, stream("People"))
	require.Len(t, recs, 4)
	assert.Equal(t, modifiedA, recs[0][cursorColumn], "the schema declares the cursor column, so full refresh records carry it too")
	assert.Empty(t, out.States())
}

func TestIncrementalNeedsConfig(t *testing.T) {
	srv, _ := fixtureServer(t, nil)
	src, _ := testSource(srv)
	cat := configured(t, src, testConfig(t, withIncremental), synclet.SyncModeIncremental, "People")
	out := runRead(t, src, testConfig(t, nil), cat, nil)
	require.Error(t, out.Err)
	assert.Contains(t, out.Err.Error(), "stream People syncs incrementally, but incremental is not enabled in the configuration: enable it or refresh the source schema")
}

func TestIncrementalPerSpreadsheet(t *testing.T) {
	srv, h := fixtureServer(t, nil)
	src, _ := testSource(srv)
	cfg := testConfig(t, func(c *Config) { withIncremental(c); c.AdditionalSpreadsheetIDs = []string{testSheetB} })
	cat := configured(t, src, cfg, synclet.SyncModeIncremental)
	orders := synclet.StreamDescriptor{Name: "Orders", Namespace: testSheetB}
	before, valuesB := len(h.list()), len(h.ranges(testSheetB))
	out := runRead(t, src, cfg, cat, streamState(t, orders, "2026-10-17T08:00:00Z"))
	require.NoError(t, out.Err)
	assert.Empty(t, out.Records(orders), "spreadsheet B has not changed")
	assert.Len(t, out.Records(stream("People")), 4)
	var driveReads int
	for _, k := range h.keys()[before:] {
		if k == driveKey(testSheetA) || k == driveKey(testSheetB) {
			driveReads++
		}
	}
	assert.Equal(t, 2, driveReads, "one modifiedTime per spreadsheet")
	assert.Len(t, h.ranges(testSheetB), valuesB, "no values request for spreadsheet B")
}
