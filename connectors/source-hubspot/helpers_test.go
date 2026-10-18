package main

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
	"github.com/synclet-io/synclet-go-sdk/testkit"
)

const testConfig = `{"access_token":"` + testToken + `","start_date":"2026-10-01"}`

// startMS and toMS are start_date and now minus the index lag of the
// test clock in Unix milliseconds, the bounds of a first incremental read.
const (
	startMS = "1790812800000"
	toMS    = "1792324740000"
)

// discoverStreams returns the discovered streams by name.
func discoverStreams(t *testing.T, src Source) map[string]synclet.Stream {
	t.Helper()
	out, err := testkit.RunSource(t.Context(), src, synclet.Request{Command: synclet.CommandDiscover, Config: json.RawMessage(testConfig)})
	require.NoError(t, err)
	require.NoError(t, out.Err)
	require.NotNil(t, out.Catalog())
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
		s, ok := streams[n]
		require.True(t, ok, "stream %s not discovered", n)
		cat.Streams = append(cat.Streams, synclet.ConfiguredStream{
			Stream: s, SyncMode: mode, DestinationSyncMode: synclet.DestinationSyncModeAppendDedup,
		})
	}
	return cat
}

func streamState(name, state string) json.RawMessage {
	return json.RawMessage(`[{"type":"STREAM","stream":{"stream_descriptor":{"name":"` + name + `"},"stream_state":` + state + `}}]`)
}

func read(t *testing.T, src Source, catalog *synclet.ConfiguredCatalog, state json.RawMessage) *testkit.Output {
	t.Helper()
	raw, err := json.Marshal(catalog)
	require.NoError(t, err)
	out, err := testkit.RunSource(t.Context(), src, synclet.Request{
		Command: synclet.CommandRead, Config: json.RawMessage(testConfig), Catalog: raw, State: state,
	})
	require.NoError(t, err)
	return out
}

func runRead(t *testing.T, src Source, catalog *synclet.ConfiguredCatalog, state json.RawMessage) *testkit.Output {
	t.Helper()
	out := read(t, src, catalog, state)
	require.NoError(t, out.Err)
	return out
}

// records returns the records of stream decoded as maps.
func records(t *testing.T, out *testkit.Output, stream string) []map[string]any {
	t.Helper()
	var recs []map[string]any
	for _, r := range out.Records(synclet.StreamDescriptor{Name: stream}) {
		var m map[string]any
		require.NoError(t, json.Unmarshal(r.Data, &m))
		recs = append(recs, m)
	}
	return recs
}

func recordIDs(t *testing.T, out *testkit.Output, stream string) []string {
	t.Helper()
	var ids []string
	for _, r := range records(t, out, stream) {
		ids = append(ids, r["id"].(string))
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
