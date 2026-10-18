package main

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
	"github.com/synclet-io/synclet-go-sdk/testkit"
)

const testConfig = `{"subdomain":"acme","email":"` + testEmail + `","api_token":"` + testToken + `","start_date":"2026-10-01"}`

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

func catalogOf(t *testing.T, src Source, mode synclet.SyncMode, names ...string) *synclet.ConfiguredCatalog {
	t.Helper()
	streams := discoverStreams(t, src)
	cat := &synclet.ConfiguredCatalog{}
	for _, n := range names {
		cat.Streams = append(cat.Streams, synclet.ConfiguredStream{
			Stream: streams[n], SyncMode: mode, DestinationSyncMode: synclet.DestinationSyncModeAppendDedup,
		})
	}
	return cat
}

func streamState(name, state string) json.RawMessage {
	return json.RawMessage(`[{"type":"STREAM","stream":{"stream_descriptor":{"name":"` + name + `"},"stream_state":` + state + `}}]`)
}

func runRead(t *testing.T, src Source, catalog *synclet.ConfiguredCatalog, state json.RawMessage) *testkit.Output {
	t.Helper()
	raw, err := json.Marshal(catalog)
	require.NoError(t, err)
	out, err := testkit.RunSource(t.Context(), src, synclet.Request{
		Command: synclet.CommandRead, Config: json.RawMessage(testConfig), Catalog: raw, State: state,
	})
	require.NoError(t, err)
	require.NoError(t, out.Err)
	return out
}

func recordIDs(t *testing.T, out *testkit.Output, stream string) []int64 {
	t.Helper()
	var ids []int64
	for _, r := range out.Records(synclet.StreamDescriptor{Name: stream}) {
		var rec struct {
			ID int64 `json:"id"`
		}
		require.NoError(t, json.Unmarshal(r.Data, &rec))
		ids = append(ids, rec.ID)
	}
	return ids
}

// states returns the stream_state of every STATE message, in order.
func states(out *testkit.Output) []string {
	var got []string
	for _, m := range out.Messages {
		if m.Type == synclet.MessageTypeState && m.State.Stream != nil {
			got = append(got, string(m.State.Stream.StreamState))
		}
	}
	return got
}

func TestReadTickets(t *testing.T) {
	srv, h := fixtureServer(t, nil)
	src := testSource(srv)
	out := runRead(t, src, catalogOf(t, src, synclet.SyncModeIncremental, "tickets"), nil)

	assert.Equal(t, []string{
		"incremental/tickets/cursor.json?start_time=1790812800",
		"incremental/tickets/cursor.json?cursor=c1",
	}, h.list(), "first by start_time, then by after_cursor until end_of_stream")
	assert.Equal(t, []int64{2001, 2002, 2003}, recordIDs(t, out, "tickets"))
	assert.Equal(t, []string{`{"cursor":"c1"}`, `{"cursor":"c2"}`}, states(out), "each page's after_cursor is saved")

	var first map[string]any
	require.NoError(t, json.Unmarshal(out.Records(synclet.StreamDescriptor{Name: "tickets"})[0].Data, &first))
	assert.NotContains(t, first, "undocumented_field", "fields outside the schema are dropped")
	assert.Equal(t, "Printer is on fire", first["subject"])
}

func TestReadTicketsFullRefresh(t *testing.T) {
	srv, h := fixtureServer(t, nil)
	src := testSource(srv)
	out := runRead(t, src, catalogOf(t, src, synclet.SyncModeFullRefresh, "tickets"), streamState("tickets", `{"cursor":"c2"}`))

	assert.Equal(t, "incremental/tickets/cursor.json?start_time=1790812800", h.list()[0], "a full refresh ignores state")
	assert.Equal(t, []int64{2001, 2002, 2003}, recordIDs(t, out, "tickets"))
	assert.Empty(t, states(out))
}

func TestResume(t *testing.T) {
	t.Run("cursor export", func(t *testing.T) {
		srv, h := fixtureServer(t, nil)
		src := testSource(srv)
		out := runRead(t, src, catalogOf(t, src, synclet.SyncModeIncremental, "tickets"), streamState("tickets", `{"cursor":"c1"}`))

		assert.Equal(t, []string{"incremental/tickets/cursor.json?cursor=c1"}, h.list(), "the saved cursor, no start_time")
		assert.Equal(t, []int64{2003}, recordIDs(t, out, "tickets"))
		assert.Equal(t, []string{`{"cursor":"c2"}`}, states(out))
	})

	t.Run("users", func(t *testing.T) {
		srv, h := fixtureServer(t, nil)
		src := testSource(srv)
		out := runRead(t, src, catalogOf(t, src, synclet.SyncModeIncremental, "users"), streamState("users", `{"cursor":"u1"}`))

		assert.Equal(t, []string{"incremental/users/cursor.json?cursor=u1"}, h.list())
		assert.Empty(t, recordIDs(t, out, "users"))
		assert.Equal(t, []string{`{"cursor":"u1"}`}, states(out))
	})

	t.Run("ticket_events from start_date", func(t *testing.T) {
		srv, h := fixtureServer(t, nil)
		src := testSource(srv)
		out := runRead(t, src, catalogOf(t, src, synclet.SyncModeIncremental, "ticket_events"), nil)

		assert.Equal(t, []string{
			"incremental/ticket_events.json?start_time=1790812800",
			"incremental/ticket_events.json?start_time=1791072000",
		}, h.list(), "end_time is the next start_time; end_of_stream stops")
		assert.Equal(t, []int64{7001, 7002, 7003}, recordIDs(t, out, "ticket_events"))
		assert.Equal(t, []string{`{"start_time":1791072000}`, `{"start_time":1791244800}`}, states(out))
	})

	t.Run("ticket_events from state", func(t *testing.T) {
		srv, h := fixtureServer(t, nil)
		src := testSource(srv)
		out := runRead(t, src, catalogOf(t, src, synclet.SyncModeIncremental, "ticket_events"), streamState("ticket_events", `{"start_time":1791072000}`))

		assert.Equal(t, []string{"incremental/ticket_events.json?start_time=1791072000"}, h.list())
		assert.Equal(t, []int64{7003}, recordIDs(t, out, "ticket_events"))
		assert.Equal(t, []string{`{"start_time":1791244800}`}, states(out), "the saved start_time advances to end_time")
	})

	t.Run("ticket_events empty state starts at start_date", func(t *testing.T) {
		srv, h := fixtureServer(t, nil)
		src := testSource(srv)
		out := runRead(t, src, catalogOf(t, src, synclet.SyncModeIncremental, "ticket_events"), streamState("ticket_events", `{}`))

		assert.Equal(t, "incremental/ticket_events.json?start_time=1790812800", h.list()[0])
		assert.Equal(t, []int64{7001, 7002, 7003}, recordIDs(t, out, "ticket_events"))
	})

	t.Run("ticket_events end_time not advancing moves start_time one second on", func(t *testing.T) {
		srv, h := fixtureServer(t, nil)
		src := testSource(srv)
		out := runRead(t, src, catalogOf(t, src, synclet.SyncModeIncremental, "ticket_events"), streamState("ticket_events", `{"start_time":1791331200}`))

		assert.Equal(t, []string{
			"incremental/ticket_events.json?start_time=1791331200",
			"incremental/ticket_events.json?start_time=1791331201",
		}, h.list(), "a page with end_of_stream false and end_time == start_time is followed by start_time + 1")
		assert.Equal(t, []int64{7004, 7005}, recordIDs(t, out, "ticket_events"))
		assert.Equal(t, []string{`{"start_time":1791331201}`, `{"start_time":1791331300}`}, states(out))
	})

	for _, state := range []string{`{}`, `{"cursor":""}`, `{"date":"x"}`} {
		t.Run("cursor-less state "+state+" starts at start_date", func(t *testing.T) {
			srv, h := fixtureServer(t, nil)
			src := testSource(srv)
			saved := json.RawMessage(`[{"type":"STREAM","stream":{"stream_descriptor":{"name":"tickets"},"stream_state":` + state +
				`}},{"type":"STREAM","stream":{"stream_descriptor":{"name":"users"},"stream_state":` + state + `}}]`)
			out := runRead(t, src, catalogOf(t, src, synclet.SyncModeIncremental, "tickets", "users"), saved)

			assert.Equal(t, []string{
				"incremental/tickets/cursor.json?start_time=1790812800",
				"incremental/tickets/cursor.json?cursor=c1",
				"incremental/users/cursor.json?start_time=1790812800",
			}, h.list())
			assert.Equal(t, []int64{2001, 2002, 2003}, recordIDs(t, out, "tickets"))
			assert.Equal(t, []int64{1001, 1003}, recordIDs(t, out, "users"))
		})
	}

	t.Run("undecodable state", func(t *testing.T) {
		srv, _ := fixtureServer(t, nil)
		src := testSource(srv)
		raw, err := json.Marshal(catalogOf(t, src, synclet.SyncModeIncremental, "tickets"))
		require.NoError(t, err)
		out, err := testkit.RunSource(t.Context(), src, synclet.Request{
			Command: synclet.CommandRead, Config: json.RawMessage(testConfig), Catalog: raw, State: streamState("tickets", `["c1"]`),
		})
		require.NoError(t, err)
		require.ErrorContains(t, out.Err, "state of tickets")
	})
}

func TestReadPaged(t *testing.T) {
	srv, h := fixtureServer(t, nil)
	src := testSource(srv)
	out := runRead(t, src, catalogOf(t, src, synclet.SyncModeFullRefresh, "organizations", "groups", "ticket_fields", "satisfaction_ratings"), nil)

	assert.Equal(t, []string{
		"organizations.json?page%5Bsize%5D=100",
		"organizations.json?page%5Bafter%5D=o1&page%5Bsize%5D=100",
		"groups.json?page%5Bsize%5D=100",
		"ticket_fields.json?page%5Bsize%5D=100",
		"satisfaction_ratings.json?page%5Bsize%5D=100",
	}, h.list())
	assert.Equal(t, []int64{4001, 4002}, recordIDs(t, out, "organizations"))
	assert.Equal(t, []int64{5001, 5002}, recordIDs(t, out, "groups"))
	assert.Equal(t, []int64{6001}, recordIDs(t, out, "ticket_fields"))
	assert.Equal(t, []int64{8001}, recordIDs(t, out, "satisfaction_ratings"))
	assert.Empty(t, states(out))
}
