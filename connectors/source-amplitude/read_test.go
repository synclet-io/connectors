package main

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
)

func TestFullRefreshStreams(t *testing.T) {
	tests := []struct {
		stream, path string
		ids          []any
		first        string
	}{
		{
			stream: streamCohorts, path: "3/cohorts", ids: []any{"coh0001", "coh0002"},
			first: `{"id":"coh0001","appId":100001,"archived":false,"name":"Example power users","description":"Users with 10 sessions in a week",
				"size":1200,"type":"redshift","owners":["owner@example.com"],"published":true,"lastMod":1760000000,"lastComputed":1760003600,
				"definition":{"version":3,"andClauses":[]},"hidden":false,"finished":true}`,
		},
		{
			stream: streamAnnotations, path: "2/annotations", ids: []any{float64(101), float64(102)},
			first: `{"id":101,"date":"2026-10-01","label":"Release 2.0","details":"Example release of the app."}`,
		},
		{
			stream: streamEventsList, path: "2/events/list", ids: []any{"app_open", "purchase"},
			first: `{"name":"app_open","value":"app_open","display":"App Open","id":5001,"hidden":false,"deleted":false,"non_active":false,"totals":4200,"totals_delta":120}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.stream, func(t *testing.T) {
			srv, h := fixtureServer(t, nil)
			src := testSource(srv)
			out := runRead(t, src, testConfig, configured(t, src, synclet.SyncModeFullRefresh, tt.stream), "")
			require.NoError(t, out.Err)
			assert.Len(t, h.of(tt.path), 1)
			recs := records(t, out, tt.stream)
			key := "id"
			if tt.stream == streamEventsList {
				key = "name"
			}
			assert.Equal(t, tt.ids, field(recs, key))
			raw, err := json.Marshal(recs[0])
			require.NoError(t, err)
			assert.JSONEq(t, tt.first, string(raw))
		})
	}
}

func TestFullRefreshEmptyResponse(t *testing.T) {
	srv, _ := fixtureServer(t, map[string]fixture{"3/cohorts": {status: http.StatusOK, file: "errors/invalid_credentials.json"}})
	src := testSource(srv)
	out := runRead(t, src, testConfig, configured(t, src, synclet.SyncModeFullRefresh, streamCohorts), "")
	require.NoError(t, out.Err)
	assert.Empty(t, records(t, out, streamCohorts))
}

func TestReadForbiddenStream(t *testing.T) {
	srv, _ := fixtureServer(t, map[string]fixture{"3/cohorts": {status: http.StatusForbidden}})
	src := testSource(srv)
	out := runRead(t, src, testConfig, configured(t, src, synclet.SyncModeFullRefresh, streamCohorts), "")
	require.EqualError(t, out.Err, "cohorts: GET /3/cohorts: the API key may not read this endpoint (HTTP 403)")
	traces := out.ErrorTraces()
	require.Len(t, traces, 1)
	assert.Equal(t, synclet.FailureTypeConfigError, traces[0].FailureType)
}

func TestReadUnknownStream(t *testing.T) {
	srv, _ := fixtureServer(t, nil)
	src := testSource(srv)
	cat := configured(t, src, synclet.SyncModeFullRefresh, streamCohorts)
	cat.Streams[0].Stream.Name = "funnels"
	out := runRead(t, src, testConfig, cat, "")
	require.EqualError(t, out.Err, `unknown stream "funnels"`)
}

func TestReadInvalidState(t *testing.T) {
	srv, _ := fixtureServer(t, nil)
	src := testSource(srv)
	out := runRead(t, src, testConfig, configured(t, src, synclet.SyncModeIncremental, streamEvents), stateOf(streamEvents, `{"server_upload_time":"yesterday"}`))
	require.EqualError(t, out.Err, `events: state server_upload_time "yesterday" is not an RFC 3339 time`)
}

func TestDiscover(t *testing.T) {
	srv, _ := fixtureServer(t, nil)
	streams := discoverStreams(t, testSource(srv))
	require.Len(t, streams, len(streamNames))
	for name, cursor := range map[string]string{streamEvents: "server_upload_time", streamActiveUsers: "date", streamAverageSessionLength: "date"} {
		s := streams[name]
		assert.True(t, s.SourceDefinedCursor, name)
		assert.Equal(t, []string{cursor}, s.DefaultCursorField, name)
		assert.Len(t, s.SupportedSyncModes, 2, name)
	}
	for _, name := range []string{streamCohorts, streamAnnotations, streamEventsList} {
		assert.Equal(t, []synclet.SyncMode{synclet.SyncModeFullRefresh}, streams[name].SupportedSyncModes, name)
	}
	assert.Equal(t, [][]string{{"uuid"}}, streams[streamEvents].SourceDefinedPrimaryKey)
	var schema struct {
		Properties map[string]map[string]any `json:"properties"`
	}
	require.NoError(t, json.Unmarshal(streams[streamEvents].JSONSchema, &schema))
	for _, f := range eventTimestampFields {
		assert.Equal(t, "date-time", schema.Properties[f]["format"], f)
	}
}
