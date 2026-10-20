package main

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
	"github.com/synclet-io/synclet-go-sdk/testkit"
)

func allStreams() []string {
	return []string{streamExport, streamEngage, streamCohorts, streamCohortMembers, streamAnnotations, streamFunnels, streamRevenue}
}

func incrementalStreams() []string {
	return []string{streamExport, streamEngage, streamCohorts, streamCohortMembers, streamFunnels, streamRevenue}
}

// discoverStreams returns the discovered streams by name.
func discoverStreams(t *testing.T, src Source, cfg json.RawMessage) map[string]synclet.Stream {
	t.Helper()
	out, err := testkit.RunSource(t.Context(), src, synclet.Request{Command: synclet.CommandDiscover, Config: cfg})
	require.NoError(t, err)
	require.NoError(t, out.Err)
	require.NotNil(t, out.Catalog())
	streams := map[string]synclet.Stream{}
	for _, s := range out.Catalog().Streams {
		streams[s.Name] = s
	}
	return streams
}

func catalogOf(t *testing.T, src Source, cfg json.RawMessage, mode synclet.SyncMode, names ...string) *synclet.ConfiguredCatalog {
	t.Helper()
	streams := discoverStreams(t, src, cfg)
	cat := &synclet.ConfiguredCatalog{}
	for _, n := range names {
		s, ok := streams[n]
		require.True(t, ok, n)
		cat.Streams = append(cat.Streams, synclet.ConfiguredStream{
			Stream: s, SyncMode: mode, DestinationSyncMode: synclet.DestinationSyncModeAppend,
		})
	}
	return cat
}

func read(t *testing.T, src Source, cfg json.RawMessage, catalog *synclet.ConfiguredCatalog, state json.RawMessage) *testkit.Output {
	t.Helper()
	raw, err := json.Marshal(catalog)
	require.NoError(t, err)
	out, err := testkit.RunSource(t.Context(), src, synclet.Request{Command: synclet.CommandRead, Config: cfg, Catalog: raw, State: state})
	require.NoError(t, err)
	return out
}

func runCheck(t *testing.T, src Source, cfg json.RawMessage) *synclet.ConnectionStatus {
	t.Helper()
	out, err := testkit.RunSource(t.Context(), src, synclet.Request{Command: synclet.CommandCheck, Config: cfg})
	require.NoError(t, err)
	st := out.ConnectionStatus()
	require.NotNil(t, st)
	return st
}

// records returns the records of stream decoded with numbers as
// json.Number.
func records(t *testing.T, out *testkit.Output, stream string) []map[string]any {
	t.Helper()
	var recs []map[string]any
	for _, r := range out.Records(synclet.StreamDescriptor{Name: stream}) {
		var rec map[string]any
		dec := json.NewDecoder(bytes.NewReader(r.Data))
		dec.UseNumber()
		require.NoError(t, dec.Decode(&rec))
		recs = append(recs, rec)
	}
	return recs
}

// field returns the values of name in recs.
func field(recs []map[string]any, name string) []any {
	vals := make([]any, 0, len(recs))
	for _, r := range recs {
		vals = append(vals, r[name])
	}
	return vals
}

// streamStates returns the stream_state of each STATE of stream, in order.
func streamStates(out *testkit.Output, stream string) []string {
	var states []string
	for _, st := range out.States() {
		if st.Stream != nil && st.Stream.StreamDescriptor.Name == stream {
			states = append(states, string(st.Stream.StreamState))
		}
	}
	return states
}

func stateOf(t *testing.T, stream, state string) json.RawMessage {
	t.Helper()
	raw, err := json.Marshal([]synclet.StateMessage{{
		Type:   synclet.StateTypeStream,
		Stream: &synclet.StreamState{StreamDescriptor: synclet.StreamDescriptor{Name: stream}, StreamState: json.RawMessage(state)},
	}})
	require.NoError(t, err)
	return raw
}
