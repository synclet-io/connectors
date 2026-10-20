package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
)

func TestActiveUsersByCountry(t *testing.T) {
	srv, h := fixtureServer(t, nil)
	src := testSource(srv)
	out := runRead(t, src, testConfig, configured(t, src, synclet.SyncModeFullRefresh, streamActiveUsers), "")
	require.NoError(t, out.Err)
	qs := h.of("2/users")
	require.Len(t, qs, 1)
	assert.Equal(t, "end=20261018&g=country&i=1&m=active&start=20261016", qs[0].Encode())
	recs := records(t, out, streamActiveUsers)
	raw, err := json.Marshal(recs)
	require.NoError(t, err)
	assert.JSONEq(t, `[
		{"date":"2026-10-16","statistics":{"United States":70,"Germany":50}},
		{"date":"2026-10-17","statistics":{"United States":75,"Germany":55}},
		{"date":"2026-10-18","statistics":{"United States":80,"Germany":60}}]`, string(raw))
	assert.Empty(t, streamStates(out, streamActiveUsers))
}

func TestActiveUsersUngrouped(t *testing.T) {
	srv, h := fixtureServer(t, nil)
	src := testSource(srv)
	cfg := withConfig(t, `{"active_users_group_by_country":false}`)
	out := runRead(t, src, cfg, configured(t, src, synclet.SyncModeIncremental, streamActiveUsers), "")
	require.NoError(t, out.Err)
	assert.False(t, h.of("2/users")[0].Has("g"))
	raw, err := json.Marshal(records(t, out, streamActiveUsers))
	require.NoError(t, err)
	assert.JSONEq(t, `[
		{"date":"2026-10-16","statistics":{"0":120}},
		{"date":"2026-10-17","statistics":{"0":130}},
		{"date":"2026-10-18","statistics":{"0":140}}]`, string(raw))
	assert.Equal(t, []string{`{"date":"2026-10-18"}`}, streamStates(out, streamActiveUsers))
}

func TestAverageSessionLength(t *testing.T) {
	srv, h := fixtureServer(t, nil)
	src := testSource(srv)
	out := runRead(t, src, testConfig, configured(t, src, synclet.SyncModeIncremental, streamAverageSessionLength), "")
	require.NoError(t, out.Err)
	assert.Equal(t, "end=20261018&start=20261016", h.of("2/sessions/average")[0].Encode())
	raw, err := json.Marshal(records(t, out, streamAverageSessionLength))
	require.NoError(t, err)
	assert.JSONEq(t, `[{"date":"2026-10-16","length":320.25},{"date":"2026-10-17","length":330},{"date":"2026-10-18","length":340}]`, string(raw))
	assert.Equal(t, []string{`{"date":"2026-10-18"}`}, streamStates(out, streamAverageSessionLength))
}

// TestMetricResume checks that an incremental read starts on the saved day,
// whose counts may have changed, and never before start_date.
func TestMetricResume(t *testing.T) {
	tests := []struct {
		name, config, state string
		start               string
		dates               []any
	}{
		{name: "saved day", config: testConfig, state: `{"date":"2026-10-17"}`, start: "20261017", dates: []any{"2026-10-17", "2026-10-18"}},
		{name: "saved day before start_date", config: testConfig, state: `{"date":"2026-10-01"}`, start: "20261016", dates: []any{"2026-10-16", "2026-10-17", "2026-10-18"}},
		{name: "saved today", config: testConfig, state: `{"date":"2026-10-18"}`, start: "20261018", dates: []any{"2026-10-18"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv, h := fixtureServer(t, nil)
			src := testSource(srv)
			out := runRead(t, src, tt.config, configured(t, src, synclet.SyncModeIncremental, streamAverageSessionLength), stateOf(streamAverageSessionLength, tt.state))
			require.NoError(t, out.Err)
			qs := h.of("2/sessions/average")
			require.Len(t, qs, 1)
			assert.Equal(t, tt.start, qs[0].Get("start"))
			assert.Equal(t, tt.dates, field(records(t, out, streamAverageSessionLength), "date"))
		})
	}
}

// TestMetricWindows checks the request windows of a long range and that
// days the API returns before the start are not emitted.
func TestMetricWindows(t *testing.T) {
	var mu sync.Mutex
	var got [][2]string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		got = append(got, [2]string{r.URL.Path + "?" + r.URL.Query().Get("start"), r.URL.Query().Get("end")})
		mu.Unlock()
		_, _ = w.Write([]byte(`{"data":{"series":[[1]],"seriesLabels":[0],"xValues":["2026-08-01"]}}`))
	}))
	t.Cleanup(srv.Close)
	src := testSource(srv)
	cfg := withConfig(t, `{"start_date":"2026-08-20T00:00:00Z"}`)
	out := runRead(t, src, cfg, configured(t, src, synclet.SyncModeIncremental, streamActiveUsers, streamAverageSessionLength), "")
	require.NoError(t, out.Err)
	assert.Equal(t, [][2]string{
		{"/2/users?20260820", "20260918"}, {"/2/users?20260919", "20261018"},
		{"/2/sessions/average?20260820", "20260903"}, {"/2/sessions/average?20260904", "20260918"},
		{"/2/sessions/average?20260919", "20261003"}, {"/2/sessions/average?20261004", "20261018"},
	}, got)
	assert.Empty(t, out.Records(synclet.StreamDescriptor{Name: streamActiveUsers}))
	assert.Equal(t, []string{`{"date":"2026-09-18"}`, `{"date":"2026-10-18"}`}, streamStates(out, streamActiveUsers))
}

func TestSeriesLabel(t *testing.T) {
	assert.Equal(t, "United States", seriesLabel([]any{json.Number("0"), "United States"}))
	assert.Equal(t, "0", seriesLabel(json.Number("0")))
	assert.Equal(t, "(none)", seriesLabel("(none)"))
	assert.Equal(t, "3", seriesLabel([]any{json.Number("3")}))
}

func TestMetricEmptyResponse(t *testing.T) {
	assert.Empty(t, averageSessionLengthRecords(seriesData{XValues: []string{"2026-10-18"}}))
	assert.Equal(t, []map[string]any{{"date": "2026-10-18", "statistics": map[string]any{}}}, activeUsersRecords(seriesData{XValues: []string{"2026-10-18"}}))
}
