package main

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
	"github.com/synclet-io/synclet-go-sdk/testkit"
)

// typesRows are the rows of ds.types as the emulator returns them. Its
// GEOGRAPHY WKT has a space after the type name; real BigQuery's has none
// (see TestRealWireFormat).
var typesRows = []string{
	`{"c_arr":[1,2],"c_bignum":0.12345678901234567890123456789012345678,"c_bool":true,"c_bytes":"AP8=","c_date":"2024-02-29",` +
		`"c_dt":"2024-02-29T12:34:56.789","c_float":1.5,"c_geo":"POINT (30.5 50.45)","c_int":9223372036854775807,"c_json":{"a":[1,2]},` +
		`"c_num":-99999999999999999999999999999.999999999,"c_rarr":[{"k":"k1"}],"c_rec":{"k":"x","v":1},"c_str":"héllo",` +
		`"c_time":"23:59:59.123456","c_ts":"2024-02-29T10:34:56.789Z"}`,
	`{"c_arr":[],"c_bignum":null,"c_bool":null,"c_bytes":null,"c_date":null,"c_dt":null,"c_float":null,"c_geo":null,"c_int":null,` +
		`"c_json":null,"c_num":null,"c_rarr":[],"c_rec":null,"c_str":null,"c_time":null,"c_ts":null}`,
}

func TestTypesRoundTrip(t *testing.T) {
	src, cfg := sharedEmulator(t)
	stream := discoverStream(t, src, cfg, "ds", "types")
	require.JSONEq(t, typesSchema, string(stream.JSONSchema))

	t.Run("table read", func(t *testing.T) {
		out := runRead(t, src, cfg, &synclet.ConfiguredCatalog{Streams: []synclet.ConfiguredStream{fullRefresh(stream)}}, nil)
		require.ElementsMatch(t, typesRows, rawRecords(t, out, stream.Descriptor()))
	})
	t.Run("query read", func(t *testing.T) {
		cat := incrementalCatalog(stream, "c_int")
		out := runRead(t, src, cfg, cat, nil)
		require.Equal(t, []string{typesRows[0], typesRows[1]}, rawRecords(t, out, stream.Descriptor()))
	})
}

func TestDiscover(t *testing.T) {
	src, cfg := sharedEmulator(t)
	for _, c := range []Config{cfg, {ProjectID: cfg.ProjectID}} {
		cat, err := src.Discover(t.Context(), c)
		require.NoError(t, err)
		var names []string
		for _, s := range cat.Streams {
			names = append(names, s.Namespace+"."+s.Name)
			require.Equal(t, []synclet.SyncMode{synclet.SyncModeFullRefresh, synclet.SyncModeIncremental}, s.SupportedSyncModes)
		}
		require.Subset(t, names, []string{"ds.events", "ds.types", "ds.v_events"})
	}
	view := discoverStream(t, src, cfg, "ds", "v_events")
	require.JSONEq(t, `{"type":"object","properties":{"id":{"type":["integer","null"]},"updated_at":{"type":["string","null"],"format":"date-time","airbyte_type":"timestamp_with_timezone"}}}`, string(view.JSONSchema))
}

func TestReadView(t *testing.T) {
	src, cfg := sharedEmulator(t)
	view := discoverStream(t, src, cfg, "ds", "v_events")
	view.JSONSchema = json.RawMessage(`{"type":"object","properties":{"id":{}}}`)
	out := runRead(t, src, cfg, &synclet.ConfiguredCatalog{Streams: []synclet.ConfiguredStream{fullRefresh(view)}}, nil)
	require.ElementsMatch(t, []string{`{"id":1}`, `{"id":2}`, `{"id":3}`}, rawRecords(t, out, view.Descriptor()))
}

func TestIncrementalResume(t *testing.T) {
	src, cfg := sharedEmulator(t)
	execSQL(t, src, `CREATE TABLE ds.resume (id INT64, updated_at TIMESTAMP)`)
	t.Cleanup(func() { execSQL(t, src, `DROP TABLE ds.resume`) })
	execSQL(t, src, `INSERT INTO ds.resume VALUES (1, TIMESTAMP '2024-01-01 00:00:00+00'), (2, TIMESTAMP '2024-01-02 00:00:00+00'), (3, TIMESTAMP '2024-01-03 00:00:00+00')`)
	stream := discoverStream(t, src, cfg, "ds", "resume")
	desc := stream.Descriptor()
	cat := incrementalCatalog(stream, "updated_at")
	cursor := func(s string) *string { return &s }

	first := runRead(t, src, cfg, cat, nil)
	require.Equal(t, []int{1, 2, 3}, ids(t, first, desc))
	t3 := lastState(t, first)
	require.Equal(t, streamState{CursorField: []string{"updated_at"}, Cursor: cursor("2024-01-03T00:00:00Z")}, t3)

	fromT2 := runRead(t, src, cfg, cat, stateFor(t, desc, streamState{CursorField: []string{"updated_at"}, Cursor: cursor("2024-01-02T00:00:00Z")}))
	require.Equal(t, []int{2, 3}, ids(t, fromT2, desc))

	execSQL(t, src, `INSERT INTO ds.resume VALUES (4, TIMESTAMP '2024-01-04 00:00:00+00')`)
	execSQL(t, src, `UPDATE ds.resume SET updated_at = TIMESTAMP '2024-01-05 00:00:00+00' WHERE id = 1`)
	resumed := runRead(t, src, cfg, cat, stateFor(t, desc, t3))
	require.Equal(t, []int{3, 4, 1}, ids(t, resumed, desc))
	require.Equal(t, streamState{CursorField: []string{"updated_at"}, Cursor: cursor("2024-01-05T00:00:00Z")}, lastState(t, resumed))

	changed := runRead(t, src, cfg, cat, stateFor(t, desc, streamState{CursorField: []string{"id"}, Cursor: cursor("3")}))
	require.ElementsMatch(t, []int{1, 2, 3, 4}, ids(t, changed, desc))
	var warned bool
	for _, l := range changed.Logs() {
		warned = warned || (l.Level == synclet.LogLevelWarn && strings.Contains(l.Message, "cursor field changed"))
	}
	require.True(t, warned, "a WARN log about the changed cursor field")

	t.Run("NULL cursor only in the first read", func(t *testing.T) {
		execSQL(t, src, `INSERT INTO ds.resume VALUES (5, NULL)`)
		require.Equal(t, []int{2, 3, 4, 1, 5}, ids(t, runRead(t, src, cfg, cat, nil), desc))
		require.Equal(t, []int{1}, ids(t, runRead(t, src, cfg, cat, stateFor(t, desc, lastState(t, resumed))), desc))
	})

	t.Run("cursor of every comparable type", func(t *testing.T) {
		types := discoverStream(t, src, cfg, "ds", "types")
		for col, saved := range map[string]string{
			"c_int": "9223372036854775807", "c_num": "-99999999999999999999999999999.999999999",
			"c_bignum": "0.12345678901234567890123456789012345678", "c_float": "1.5", "c_str": "héllo",
			"c_date": "2024-02-29", "c_time": "23:59:59.123456", "c_dt": "2024-02-29T12:34:56.789", "c_ts": "2024-02-29T10:34:56.789Z",
		} {
			first := runRead(t, src, cfg, incrementalCatalog(types, col), nil)
			require.Len(t, first.Records(types.Descriptor()), 2, col)
			require.Equal(t, saved, *lastState(t, first).Cursor, col)
			again := runRead(t, src, cfg, incrementalCatalog(types, col), stateFor(t, types.Descriptor(), lastState(t, first)))
			require.Len(t, again.Records(types.Descriptor()), 1, col)
		}
	})
}

func TestIncrementalCheckpoints(t *testing.T) {
	src, cfg := sharedEmulator(t)
	execSQL(t, src, `CREATE TABLE ds.big AS SELECT id FROM UNNEST(GENERATE_ARRAY(1, 25000)) AS id`)
	t.Cleanup(func() { execSQL(t, src, `DROP TABLE ds.big`) })
	stream := discoverStream(t, src, cfg, "ds", "big")

	out := runRead(t, src, cfg, incrementalCatalog(stream, "id"), nil)
	require.Len(t, out.Records(stream.Descriptor()), 25000)
	var cursors []string
	for _, s := range out.States() {
		var st streamState
		require.NoError(t, json.Unmarshal(s.Stream.StreamState, &st))
		cursors = append(cursors, *st.Cursor)
	}
	require.Equal(t, []string{"10000", "20000", "25000"}, cursors)
}

func TestReadConfigErrors(t *testing.T) {
	src, cfg := sharedEmulator(t)
	events := discoverStream(t, src, cfg, "ds", "events")
	types := discoverStream(t, src, cfg, "ds", "types")
	run := func(cs synclet.ConfiguredStream) *testkit.Output {
		out, err := testkit.RunSource(t.Context(), src, synclet.Request{
			Command: synclet.CommandRead, Config: mustJSON(t, cfg),
			Catalog: mustJSON(t, &synclet.ConfiguredCatalog{Streams: []synclet.ConfiguredStream{cs}}),
		})
		require.NoError(t, err)
		require.Error(t, out.Err)
		traces := out.ErrorTraces()
		require.Len(t, traces, 1)
		require.Equal(t, synclet.FailureTypeConfigError, traces[0].FailureType)
		return out
	}

	missing := events
	missing.Name = "nope"
	require.ErrorContains(t, run(fullRefresh(missing)).Err, "ds.nope: no such table or view")

	extra := events
	extra.JSONSchema = json.RawMessage(`{"type":"object","properties":{"id":{},"ghost":{}}}`)
	require.ErrorContains(t, run(fullRefresh(extra)).Err, `column "ghost" does not exist`)

	require.ErrorContains(t, run(synclet.ConfiguredStream{Stream: events, SyncMode: synclet.SyncModeIncremental}).Err, "needs a cursor_field")
	require.ErrorContains(t, run(incrementalCatalog(events, "nope").Streams[0]).Err, `cursor column "nope" does not exist`)
	for _, col := range []string{"c_bool", "c_bytes", "c_geo", "c_json", "c_rec", "c_arr"} {
		require.ErrorContains(t, run(incrementalCatalog(types, col).Streams[0]).Err, "cannot be a cursor", col)
	}
}

func TestConformance(t *testing.T) {
	src, cfg := sharedEmulator(t)
	broken := cfg
	broken.Datasets = []string{"missing"}
	events := discoverStream(t, src, cfg, "ds", "events")
	types := discoverStream(t, src, cfg, "ds", "types")
	view := discoverStream(t, src, cfg, "ds", "v_events")

	t.Run("full refresh", func(t *testing.T) {
		testkit.RunSourceSuite(t, src, testkit.SourceSuite{
			Config:       mustJSON(t, cfg),
			BrokenConfig: mustJSON(t, broken),
			Catalog:      &synclet.ConfiguredCatalog{Streams: []synclet.ConfiguredStream{fullRefresh(types), fullRefresh(events), fullRefresh(view)}},
		})
	})
	t.Run("incremental", func(t *testing.T) {
		testkit.RunSourceSuite(t, src, testkit.SourceSuite{
			Config:       mustJSON(t, cfg),
			BrokenConfig: mustJSON(t, broken),
			Catalog:      incrementalCatalog(events, "updated_at"),
		})
	})
}
