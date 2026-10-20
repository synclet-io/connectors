package main

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
	"github.com/synclet-io/synclet-go-sdk/testkit"
)

const testConfig = `{"api_key":"` + testAPIKey + `","secret_key":"` + testSecretKey + `","start_date":"2026-10-16T00:00:00Z"}`

// withConfig returns testConfig with the fields of extra added or replaced.
func withConfig(t *testing.T, extra string) string {
	t.Helper()
	var base, add map[string]any
	require.NoError(t, json.Unmarshal([]byte(testConfig), &base))
	require.NoError(t, json.Unmarshal([]byte(extra), &add))
	for k, v := range add {
		base[k] = v
	}
	b, err := json.Marshal(base)
	require.NoError(t, err)
	return string(b)
}

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

// configured returns a catalog of names in mode; a stream without
// incremental support is configured for full refresh.
func configured(t *testing.T, src Source, mode synclet.SyncMode, names ...string) *synclet.ConfiguredCatalog {
	t.Helper()
	streams := discoverStreams(t, src)
	cat := &synclet.ConfiguredCatalog{}
	for _, name := range names {
		st, ok := streams[name]
		require.True(t, ok, name)
		m := mode
		if len(st.SupportedSyncModes) == 1 {
			m = synclet.SyncModeFullRefresh
		}
		dst := synclet.DestinationSyncModeAppendDedup
		if m == synclet.SyncModeFullRefresh {
			dst = synclet.DestinationSyncModeOverwrite
		}
		cat.Streams = append(cat.Streams, synclet.ConfiguredStream{Stream: st, SyncMode: m, DestinationSyncMode: dst})
	}
	return cat
}

func runRead(t *testing.T, src Source, config string, cat *synclet.ConfiguredCatalog, state string) *testkit.Output {
	t.Helper()
	rawCat, err := json.Marshal(cat)
	require.NoError(t, err)
	req := synclet.Request{Command: synclet.CommandRead, Config: json.RawMessage(config), Catalog: rawCat}
	if state != "" {
		req.State = json.RawMessage(state)
	}
	out, err := testkit.RunSource(t.Context(), src, req)
	require.NoError(t, err)
	return out
}

// records returns the data of the stream's records.
func records(t *testing.T, out *testkit.Output, name string) []map[string]any {
	t.Helper()
	var recs []map[string]any
	for _, r := range out.Records(synclet.StreamDescriptor{Name: name}) {
		var rec map[string]any
		require.NoError(t, json.Unmarshal(r.Data, &rec))
		recs = append(recs, rec)
	}
	return recs
}

// field returns the values of one field of recs.
func field(recs []map[string]any, name string) []any {
	var vs []any
	for _, r := range recs {
		vs = append(vs, r[name])
	}
	return vs
}

// streamStates returns the stream states of a stream as JSON, in order.
func streamStates(out *testkit.Output, name string) []string {
	var ss []string
	for _, st := range out.States() {
		if st.Stream != nil && st.Stream.StreamDescriptor.Name == name {
			ss = append(ss, string(st.Stream.StreamState))
		}
	}
	return ss
}

func stateOf(name, state string) string {
	return `[{"type":"STREAM","stream":{"stream_descriptor":{"name":"` + name + `"},"stream_state":` + state + `}}]`
}
