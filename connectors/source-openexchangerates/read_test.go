package main

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
	"github.com/synclet-io/synclet-go-sdk/testkit"
)

const testConfig = `{"app_id":"` + testAppID + `","start_date":"2026-10-14"}`

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

func historicalCatalog(t *testing.T, src Source, mode synclet.SyncMode) *synclet.ConfiguredCatalog {
	t.Helper()
	return &synclet.ConfiguredCatalog{Streams: []synclet.ConfiguredStream{{
		Stream:              discoverStreams(t, src)[streamHistoricalRates],
		SyncMode:            mode,
		DestinationSyncMode: synclet.DestinationSyncModeAppendDedup,
	}}}
}

func savedDate(date string) json.RawMessage {
	return json.RawMessage(`[{"type":"STREAM","stream":{"stream_descriptor":{"name":"historical_rates"},"stream_state":{"date":"` + date + `"}}}]`)
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

func recordDates(t *testing.T, out *testkit.Output) []string {
	t.Helper()
	var dates []string
	for _, r := range out.Records(synclet.StreamDescriptor{Name: streamHistoricalRates}) {
		var rec rateRecord
		require.NoError(t, json.Unmarshal(r.Data, &rec))
		if len(dates) == 0 || dates[len(dates)-1] != rec.Date {
			dates = append(dates, rec.Date)
		}
	}
	return dates
}

func TestIncrementalResume(t *testing.T) {
	srv, h := fixtureServer(t, nil)
	src := testSource(srv)
	out := runRead(t, src, historicalCatalog(t, src, synclet.SyncModeIncremental), savedDate("2026-10-15"))

	assert.Equal(t, []string{"historical/2026-10-16.json", "historical/2026-10-17.json"}, h.list())
	assert.Equal(t, []string{"2026-10-16", "2026-10-17"}, recordDates(t, out))
	assert.Len(t, out.Records(synclet.StreamDescriptor{Name: streamHistoricalRates}), 6)
	states := out.States()
	require.Len(t, states, 2)
	assert.JSONEq(t, `{"date":"2026-10-17"}`, string(states[1].Stream.StreamState))
}

func TestIncrementalUpToDate(t *testing.T) {
	srv, h := fixtureServer(t, nil)
	src := testSource(srv)
	out := runRead(t, src, historicalCatalog(t, src, synclet.SyncModeIncremental), savedDate("2026-10-17"))

	assert.Empty(t, h.list())
	assert.Empty(t, out.Records(synclet.StreamDescriptor{Name: streamHistoricalRates}))
}

func TestIncrementalFromStartDate(t *testing.T) {
	srv, h := fixtureServer(t, nil)
	src := testSource(srv)
	out := runRead(t, src, historicalCatalog(t, src, synclet.SyncModeIncremental), nil)

	assert.Len(t, h.list(), 4)
	assert.Equal(t, []string{"2026-10-14", "2026-10-15", "2026-10-16", "2026-10-17"}, recordDates(t, out))
	assert.Len(t, out.States(), 4)
}

func TestFullRefreshIgnoresState(t *testing.T) {
	srv, _ := fixtureServer(t, nil)
	src := testSource(srv)
	out := runRead(t, src, historicalCatalog(t, src, synclet.SyncModeFullRefresh), savedDate("2026-10-16"))

	assert.Equal(t, []string{"2026-10-14", "2026-10-15", "2026-10-16", "2026-10-17"}, recordDates(t, out))
	assert.Empty(t, out.States())
}

func TestReadRecords(t *testing.T) {
	srv, _ := fixtureServer(t, nil)
	src := testSource(srv)
	streams := discoverStreams(t, src)
	catalog := &synclet.ConfiguredCatalog{}
	for _, name := range []string{streamCurrencies, streamLatestRates} {
		catalog.Streams = append(catalog.Streams, synclet.ConfiguredStream{
			Stream: streams[name], SyncMode: synclet.SyncModeFullRefresh, DestinationSyncMode: synclet.DestinationSyncModeOverwrite,
		})
	}
	out := runRead(t, src, catalog, nil)

	cur := out.Records(synclet.StreamDescriptor{Name: streamCurrencies})
	require.Len(t, cur, 4)
	assert.JSONEq(t, `{"code":"EUR","name":"Euro"}`, string(cur[0].Data))
	latest := out.Records(synclet.StreamDescriptor{Name: streamLatestRates})
	require.Len(t, latest, 3)
	assert.JSONEq(t, `{"base":"USD","currency":"JPY","rate":149.123456,"timestamp":1792321200}`, string(latest[2].Data))
}

func TestReadInvalidState(t *testing.T) {
	srv, _ := fixtureServer(t, nil)
	src := testSource(srv)
	raw, err := json.Marshal(historicalCatalog(t, src, synclet.SyncModeIncremental))
	require.NoError(t, err)
	out, err := testkit.RunSource(t.Context(), src, synclet.Request{
		Command: synclet.CommandRead, Config: json.RawMessage(testConfig), Catalog: raw, State: savedDate("yesterday"),
	})
	require.NoError(t, err)
	require.ErrorContains(t, out.Err, `date "yesterday" is not a YYYY-MM-DD date`)
}
