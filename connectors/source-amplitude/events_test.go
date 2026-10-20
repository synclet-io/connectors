package main

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
)

// windows returns the start-end pairs of export requests.
func windows(qs []url.Values) []string {
	var ws []string
	for _, q := range qs {
		ws = append(ws, q.Get("start")+"-"+q.Get("end"))
	}
	return ws
}

func uuids(n ...int) []any {
	var vs []any
	for _, i := range n {
		vs = append(vs, "00000000-0000-4000-8000-00000000000"+string(rune('0'+i)))
	}
	return vs
}

func TestEventsFullRefresh(t *testing.T) {
	srv, h := fixtureServer(t, nil)
	src := testSource(srv)
	out := runRead(t, src, testConfig, configured(t, src, synclet.SyncModeFullRefresh, streamEvents), "")
	require.NoError(t, out.Err)
	assert.Equal(t, []string{"20261016T00-20261016T23", "20261017T00-20261017T23", "20261018T00-20261018T09"}, windows(h.of(exportPath)),
		"hours up to the last one that ended two hours ago")
	recs := records(t, out, streamEvents)
	assert.Equal(t, uuids(1, 2, 3, 4, 5), field(recs, "uuid"))
	assert.Empty(t, streamStates(out, streamEvents))

	first := recs[0]
	assert.Equal(t, "2026-10-16T08:15:00.123Z", first["server_upload_time"])
	assert.Equal(t, "2026-10-16T08:15:00.123Z", first["event_time"])
	assert.Equal(t, "2026-01-01T00:00:00Z", first["user_creation_time"])
	assert.Equal(t, "2026-10-16T08:59:59.999999Z", recs[1]["server_upload_time"])
	assert.Equal(t, map[string]any{"screen": "home"}, first["event_properties"])
	assert.InDelta(t, 1001, first["amplitude_id"], 0)
	assert.Nil(t, first["paying"])
}

func TestEventsIncremental(t *testing.T) {
	srv, _ := fixtureServer(t, nil)
	src := testSource(srv)
	out := runRead(t, src, testConfig, configured(t, src, synclet.SyncModeIncremental, streamEvents), "")
	require.NoError(t, out.Err)
	assert.Equal(t, []string{
		`{"server_upload_time":"2026-10-17T00:00:00Z"}`,
		`{"server_upload_time":"2026-10-18T00:00:00Z"}`,
		`{"server_upload_time":"2026-10-18T10:00:00Z"}`,
	}, streamStates(out, streamEvents))
}

func TestEventsResume(t *testing.T) {
	tests := []struct {
		name, config, state string
		windows             []string
		uuids               []any
	}{
		{
			name: "saved hour", config: testConfig,
			state:   `{"server_upload_time":"2026-10-17T00:00:00Z"}`,
			windows: []string{"20261017T00-20261017T23", "20261018T00-20261018T09"}, uuids: uuids(4, 5),
		},
		{
			name: "saved hour before start_date", config: withConfig(t, `{"start_date":"2026-10-18T00:00:00Z"}`),
			state:   `{"server_upload_time":"2026-10-01T00:00:00Z"}`,
			windows: []string{"20261018T00-20261018T09"}, uuids: uuids(5),
		},
		{
			name: "nothing new", config: testConfig,
			state: `{"server_upload_time":"2026-10-18T10:00:00Z"}`,
		},
		{
			name: "start_date inside an hour", config: withConfig(t, `{"start_date":"2026-10-18T08:30:00Z"}`),
			windows: []string{"20261018T08-20261018T09"}, uuids: uuids(5),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv, h := fixtureServer(t, nil)
			src := testSource(srv)
			state := ""
			if tt.state != "" {
				state = stateOf(streamEvents, tt.state)
			}
			out := runRead(t, src, tt.config, configured(t, src, synclet.SyncModeIncremental, streamEvents), state)
			require.NoError(t, out.Err)
			assert.Equal(t, tt.windows, windows(h.of(exportPath)))
			assert.Equal(t, tt.uuids, field(records(t, out, streamEvents), "uuid"))
		})
	}
}

func TestEventsRequestTimeRange(t *testing.T) {
	srv, h := fixtureServer(t, nil)
	src := testSource(srv)
	cfg := withConfig(t, `{"start_date":"2026-10-18T00:00:00Z","request_time_range":4}`)
	out := runRead(t, src, cfg, configured(t, src, synclet.SyncModeIncremental, streamEvents), "")
	require.NoError(t, out.Err)
	assert.Equal(t, []string{"20261018T00-20261018T03", "20261018T04-20261018T07", "20261018T08-20261018T09"}, windows(h.of(exportPath)))
	assert.Equal(t, []string{
		`{"server_upload_time":"2026-10-18T04:00:00Z"}`,
		`{"server_upload_time":"2026-10-18T08:00:00Z"}`,
		`{"server_upload_time":"2026-10-18T10:00:00Z"}`,
	}, streamStates(out, streamEvents), "a window without events (404) still moves the state")
	assert.Equal(t, uuids(5), field(records(t, out, streamEvents), "uuid"))
}

func TestEventsSplitsRejectedWindow(t *testing.T) {
	srv, h := fixtureServer(t, map[string]fixture{
		"2/export?20261016T00-20261016T23": {status: http.StatusBadRequest},
		"2/export?20261016T00-20261016T11": {status: http.StatusBadRequest},
	})
	src := testSource(srv)
	out := runRead(t, src, testConfig, configured(t, src, synclet.SyncModeIncremental, streamEvents), "")
	require.NoError(t, out.Err)
	assert.Equal(t, []string{
		"20261016T00-20261016T23",
		"20261016T00-20261016T11", "20261016T00-20261016T05", "20261016T06-20261016T11",
		"20261016T12-20261016T23",
		"20261017T00-20261017T23", "20261018T00-20261018T09",
	}, windows(h.of(exportPath)))
	assert.Equal(t, uuids(1, 2, 3, 4, 5), field(records(t, out, streamEvents), "uuid"))
	assert.Equal(t, `{"server_upload_time":"2026-10-17T00:00:00Z"}`, streamStates(out, streamEvents)[0])
}

func TestEventsRejectedHourFails(t *testing.T) {
	srv, _ := fixtureServer(t, map[string]fixture{"2/export?20261016T08-20261016T08": {status: http.StatusBadRequest}})
	src := testSource(srv)
	cfg := withConfig(t, `{"start_date":"2026-10-16T07:00:00Z","request_time_range":1}`)
	out := runRead(t, src, cfg, configured(t, src, synclet.SyncModeIncremental, streamEvents), "")
	require.EqualError(t, out.Err, "events: export 20261016T08 to 20261016T08: GET /2/export: HTTP 400")
	assert.Equal(t, []string{`{"server_upload_time":"2026-10-16T08:00:00Z"}`}, streamStates(out, streamEvents))
}

func TestEventsInvalidArchive(t *testing.T) {
	srv, _ := fixtureServer(t, map[string]fixture{"2/export?20261016T00-20261016T23": {status: http.StatusOK, file: "2/annotations.json"}})
	src := testSource(srv)
	out := runRead(t, src, testConfig, configured(t, src, synclet.SyncModeFullRefresh, streamEvents), "")
	require.ErrorContains(t, out.Err, "events: export 20261016T00 to 20261016T23: open archive: ")
}

func TestEventsArchiveFileOrder(t *testing.T) {
	data := exportZip(t, map[string][]byte{
		"1/1_2026-10-16_10#0.json.gz": []byte(`{"uuid":"c"}` + "\n"),
		"1/1_2026-10-16_9#0.json.gz":  []byte(`{"uuid":"b"}` + "\n"),
		"1/1_2026-10-16_10#1.json.gz": []byte(`{"uuid":"d"}` + "\n" + `{"uuid":"e"}`),
		"1/1_2026-10-15_23#0.json.gz": []byte(`{"uuid":"a"}`),
	})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(data)
	}))
	t.Cleanup(srv.Close)
	src := testSource(srv)
	cfg := withConfig(t, `{"start_date":"2026-10-18T00:00:00Z","request_time_range":24}`)
	out := runRead(t, src, cfg, configured(t, src, synclet.SyncModeFullRefresh, streamEvents), "")
	require.NoError(t, out.Err)
	assert.Equal(t, []any{"a", "b", "c", "d", "e"}, field(records(t, out, streamEvents), "uuid"))
}

func TestNormalizeEventTimes(t *testing.T) {
	ev := map[string]any{
		"event_time":         "2026-10-16 08:15:00",
		"server_upload_time": "2026-10-16T08:15:00Z",
		"client_event_time":  nil,
		"event_type":         "2026-10-16 08:15:00.000000",
	}
	normalizeEventTimes(ev)
	assert.Equal(t, map[string]any{
		"event_time":         "2026-10-16T08:15:00Z",
		"server_upload_time": "2026-10-16T08:15:00Z",
		"client_event_time":  nil,
		"event_type":         "2026-10-16 08:15:00.000000",
	}, ev)
}

func TestEventsResumeFromEventTime(t *testing.T) {
	for _, saved := range []string{"2026-10-17T08:15:00.123Z", "2026-10-17 08:15:00.123000"} {
		t.Run(saved, func(t *testing.T) {
			srv, h := fixtureServer(t, nil)
			src := testSource(srv)
			state := stateOf(streamEvents, `{"server_upload_time":"`+saved+`"}`)
			out := runRead(t, src, testConfig, configured(t, src, synclet.SyncModeIncremental, streamEvents), state)
			require.NoError(t, out.Err)
			assert.Equal(t, []string{"20261017T08-20261018T07", "20261018T08-20261018T09"}, windows(h.of(exportPath)))
			assert.Equal(t, []string{
				`{"server_upload_time":"2026-10-18T08:00:00Z"}`,
				`{"server_upload_time":"2026-10-18T10:00:00Z"}`,
			}, streamStates(out, streamEvents), "a saved time inside an hour resumes at the start of that hour")
		})
	}
}
