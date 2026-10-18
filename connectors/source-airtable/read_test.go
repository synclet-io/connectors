package main

import (
	"encoding/json"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
	"github.com/synclet-io/synclet-go-sdk/testkit"
)

const testConfig = `{"personal_access_token":"` + testToken + `"}`

const (
	tasksPath    = base1 + "/tblTasks00000001"
	allTypesPath = base1 + "/tblAllTypes00001"
)

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
		s, ok := streams[n]
		require.True(t, ok, n)
		cs := synclet.ConfiguredStream{Stream: s, SyncMode: mode, DestinationSyncMode: synclet.DestinationSyncModeAppendDedup}
		if mode == synclet.SyncModeIncremental {
			cs.CursorField = s.DefaultCursorField
		}
		cat.Streams = append(cat.Streams, cs)
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
	return out
}

func records(t *testing.T, out *testkit.Output, stream string) []map[string]any {
	t.Helper()
	var recs []map[string]any
	for _, r := range out.Records(synclet.StreamDescriptor{Name: stream}) {
		var rec map[string]any
		require.NoError(t, json.Unmarshal(r.Data, &rec))
		recs = append(recs, rec)
	}
	return recs
}

func ids(recs []map[string]any) []string {
	var out []string
	for _, r := range recs {
		out = append(out, r["_airtable_id"].(string))
	}
	return out
}

// states returns the stream_state of every STATE message, in order.
func states(out *testkit.Output) []string {
	var s []string
	for _, st := range out.States() {
		s = append(s, string(st.Stream.StreamState))
	}
	return s
}

// recordHits returns the record requests among h, with their queries
// decoded.
func recordHits(t *testing.T, h *hits, path string) []url.Values {
	t.Helper()
	var out []url.Values
	for _, r := range h.list() {
		p, q, _ := strings.Cut(r, "?")
		if p != path {
			continue
		}
		v, err := url.ParseQuery(q)
		require.NoError(t, err)
		out = append(out, v)
	}
	return out
}

func TestReadFullRefresh(t *testing.T) {
	srv, h := fixtureServer(t, nil)
	src := testSource(srv)
	out := runRead(t, src, catalogOf(t, src, synclet.SyncModeFullRefresh, tasksStream, contactsStream), nil)
	require.NoError(t, out.Err)

	tasks := records(t, out, tasksStream)
	assert.Equal(t, []string{"recTask000000003", "recTask000000001", "recTask000000002", "recTask000000004"}, ids(tasks), "every page until no offset")
	assert.Equal(t, []url.Values{
		{"pageSize": {"100"}, "returnFieldsByFieldId": {"true"}},
		{"pageSize": {"100"}, "returnFieldsByFieldId": {"true"}, "offset": {"itrTasks2/recTask000000002"}},
	}, recordHits(t, h, tasksPath))
	assert.Equal(t, map[string]any{
		"_airtable_id": "recTask000000001", "_airtable_created_time": "2026-10-01T09:00:00.000Z",
		"Name": "Task 1", "Status": "Todo", "Hours": 1.5, "Last Modified": "2026-10-03T10:00:00.000Z",
		"Score": float64(5), "Owner Names": []any{"Ann"},
	}, tasks[1])
	assert.NotContains(t, tasks[3], "fldTaskUnknown01", "fields missing from the catalog are not emitted")
	assert.Empty(t, out.States(), "a full refresh emits no state")

	contacts := records(t, out, contactsStream)
	assert.Equal(t, []map[string]any{
		{"_airtable_id": "recContact000001", "_airtable_created_time": "2026-10-01T00:00:00.000Z", "Name": "Ann", "Email": "ann@example.com"},
		{"_airtable_id": "recContact000002", "_airtable_created_time": "2026-10-02T00:00:00.000Z", "Name": "Bob"},
	}, contacts)
}

func TestReadErrorValues(t *testing.T) {
	srv, _ := fixtureServer(t, nil)
	src := testSource(srv)
	out := runRead(t, src, catalogOf(t, src, synclet.SyncModeFullRefresh, tasksStream), nil)
	require.NoError(t, out.Err)
	tasks := records(t, out, tasksStream)
	require.Len(t, tasks, 4, "a record with an error value is kept")

	assert.Contains(t, tasks[0], "Score")
	assert.Nil(t, tasks[0]["Score"], "NaN becomes null")
	assert.Contains(t, tasks[2], "Score")
	assert.Nil(t, tasks[2]["Score"], "#ERROR! becomes null")
	assert.Equal(t, []any{"Bob", nil}, tasks[2]["Owner Names"], "an error inside a lookup becomes null")

	var warns []string
	for _, l := range out.Logs() {
		if l.Level == synclet.LogLevelWarn {
			warns = append(warns, l.Message)
		}
	}
	require.Len(t, warns, 2, "one warning per field")
	assert.Contains(t, warns[0], `field=Score`)
	assert.Contains(t, warns[1], `field="Owner Names"`)
}

func TestReadIncremental(t *testing.T) {
	sort := url.Values{"pageSize": {"100"}, "returnFieldsByFieldId": {"true"}, "sort[0][field]": {"fldTaskModified01"}, "sort[0][direction]": {"asc"}}
	with := func(kv ...string) url.Values {
		v := url.Values{}
		for k, vs := range sort {
			v[k] = vs
		}
		for i := 0; i < len(kv); i += 2 {
			v.Set(kv[i], kv[i+1])
		}
		return v
	}

	t.Run("first read has no filter and saves the greatest cursor", func(t *testing.T) {
		srv, h := fixtureServer(t, nil)
		src := testSource(srv)
		out := runRead(t, src, catalogOf(t, src, synclet.SyncModeIncremental, tasksStream), nil)
		require.NoError(t, out.Err)
		assert.Len(t, records(t, out, tasksStream), 4)
		assert.Equal(t, []url.Values{sort, with("offset", "itrTasks2/recTask000000002")}, recordHits(t, h, tasksPath))
		assert.Equal(t, []string{`{"cursor":"2026-10-07T12:30:00Z"}`}, states(out), "state at stream end only")
	})

	t.Run("saved cursor filters at or after it", func(t *testing.T) {
		srv, h := fixtureServer(t, nil)
		src := testSource(srv)
		out := runRead(t, src, catalogOf(t, src, synclet.SyncModeIncremental, tasksStream), streamState(tasksStream, `{"cursor":"2026-10-05T10:00:00Z"}`))
		require.NoError(t, out.Err)
		formula := "OR(DATETIME_DIFF({Last Modified}, DATETIME_PARSE('2026-10-05T10:00:00Z'), 'milliseconds') >= 0, " +
			"AND({Last Modified} = BLANK(), DATETIME_DIFF(CREATED_TIME(), DATETIME_PARSE('2026-10-05T10:00:00Z'), 'milliseconds') >= 0))"
		assert.Equal(t, []url.Values{with("filterByFormula", formula)}, recordHits(t, h, tasksPath))
		recs := records(t, out, tasksStream)
		assert.Equal(t, []string{"recTask000000003", "recTask000000002", "recTask000000004"}, ids(recs), "the record at the saved value and the blank-cursor record are emitted")
		assert.Equal(t, "2026-10-05T10:00:00.000Z", recs[1]["Last Modified"])
		assert.Equal(t, []string{`{"cursor":"2026-10-07T12:30:00Z"}`}, states(out))
	})

	t.Run("blank cursor counts by created time", func(t *testing.T) {
		srv, _ := fixtureServer(t, map[string]fixture{tasksPath: {status: 200, file: "appBase0000000001/tblTasks00000001/blank-latest.json"}})
		src := testSource(srv)
		out := runRead(t, src, catalogOf(t, src, synclet.SyncModeIncremental, tasksStream), nil)
		require.NoError(t, out.Err)
		assert.Equal(t, []string{`{"cursor":"2026-10-09T08:00:00Z"}`}, states(out))
	})

	t.Run("no record keeps the saved cursor", func(t *testing.T) {
		srv, _ := fixtureServer(t, map[string]fixture{tasksPath: {status: 200, file: "appBase0000000001/tblTasks00000001/empty.json"}})
		src := testSource(srv)
		out := runRead(t, src, catalogOf(t, src, synclet.SyncModeIncremental, tasksStream), streamState(tasksStream, `{"cursor":"2026-10-05T10:00:00Z"}`))
		require.NoError(t, out.Err)
		assert.Equal(t, []string{`{"cursor":"2026-10-05T10:00:00Z"}`}, states(out))
	})

	t.Run("cursor that is not a last-modified-time field", func(t *testing.T) {
		srv, _ := fixtureServer(t, nil)
		src := testSource(srv)
		cat := catalogOf(t, src, synclet.SyncModeIncremental, tasksStream)
		cat.Streams[0].CursorField = []string{"Hours"}
		out := runRead(t, src, cat, nil)
		require.Error(t, out.Err)
		assert.Equal(t, `stream Projects/Tasks: cursor field "Hours" is not a last-modified-time field; only a "Last modified time" field can be the cursor`, out.ErrorTraces()[0].Message)
		assert.Equal(t, synclet.FailureType("config_error"), out.ErrorTraces()[0].FailureType)
	})

	t.Run("table without a last-modified-time field", func(t *testing.T) {
		srv, _ := fixtureServer(t, nil)
		src := testSource(srv)
		cat := catalogOf(t, src, synclet.SyncModeIncremental, contactsStream)
		out := runRead(t, src, cat, nil)
		require.Error(t, out.Err)
		assert.Contains(t, out.ErrorTraces()[0].Message, "the table has none, so it syncs as full refresh only")
	})

	t.Run("invalid state", func(t *testing.T) {
		srv, _ := fixtureServer(t, nil)
		src := testSource(srv)
		out := runRead(t, src, catalogOf(t, src, synclet.SyncModeIncremental, tasksStream), streamState(tasksStream, `{"cursor":"yesterday"}`))
		require.Error(t, out.Err)
		assert.Contains(t, out.ErrorTraces()[0].Message, `cursor "yesterday" is not an RFC 3339 time`)
	})
}

func TestSinceFormulaEscapesFieldName(t *testing.T) {
	f := sinceFormula("Edited {at}", testNow)
	assert.Equal(t, "OR(DATETIME_DIFF({Edited {at\\}}, DATETIME_PARSE('2026-10-18T12:00:00Z'), 'milliseconds') >= 0, "+
		"AND({Edited {at\\}} = BLANK(), DATETIME_DIFF(CREATED_TIME(), DATETIME_PARSE('2026-10-18T12:00:00Z'), 'milliseconds') >= 0))", f)
}

func TestReadFieldIDs(t *testing.T) {
	t.Run("a field renamed since discover keeps its column", func(t *testing.T) {
		srv, _ := fixtureServer(t, nil)
		src := testSource(srv)
		cat := catalogOf(t, src, synclet.SyncModeFullRefresh, tasksStream)
		cat.Streams[0].Stream.JSONSchema = renameProperty(t, cat.Streams[0].Stream.JSONSchema, "Name", "Title")
		out := runRead(t, src, cat, nil)
		require.NoError(t, out.Err)
		recs := records(t, out, tasksStream)
		assert.Equal(t, "Task 1", recs[1]["Title"])
		assert.NotContains(t, recs[1], "Name")
	})

	t.Run("a schema without hints matches stream and field names", func(t *testing.T) {
		srv, _ := fixtureServer(t, nil)
		src := testSource(srv)
		cat := catalogOf(t, src, synclet.SyncModeIncremental, tasksStream)
		cat.Streams[0].Stream.JSONSchema = json.RawMessage(`{"type":"object","properties":{"_airtable_id":{"type":"string"},"Name":{"type":["string","null"]},"Last Modified":{"type":["string","null"]}}}`)
		out := runRead(t, src, cat, nil)
		require.NoError(t, out.Err)
		recs := records(t, out, tasksStream)
		require.Len(t, recs, 4)
		assert.Equal(t, map[string]any{
			"_airtable_id": "recTask000000001", "_airtable_created_time": "2026-10-01T09:00:00.000Z",
			"Name": "Task 1", "Last Modified": "2026-10-03T10:00:00.000Z",
		}, recs[1])
	})

	t.Run("a deleted table", func(t *testing.T) {
		srv, _ := fixtureServer(t, nil)
		src := testSource(srv)
		cat := catalogOf(t, src, synclet.SyncModeFullRefresh, tasksStream)
		cat.Streams[0].Stream.JSONSchema = json.RawMessage(strings.Replace(string(cat.Streams[0].Stream.JSONSchema), "tblTasks00000001", "tblGone000000001", 1))
		out := runRead(t, src, cat, nil)
		require.Error(t, out.Err)
		assert.Contains(t, out.ErrorTraces()[0].Message, "table tblGone000000001 no longer exists in base appBase0000000001; refresh the source schema")
	})
}

func renameProperty(t *testing.T, schema json.RawMessage, from, to string) json.RawMessage {
	t.Helper()
	var s map[string]any
	require.NoError(t, json.Unmarshal(schema, &s))
	props := s["properties"].(map[string]any)
	props[to] = props[from]
	delete(props, from)
	b, err := json.Marshal(s)
	require.NoError(t, err)
	return b
}

func TestReadAllTypes(t *testing.T) {
	srv, h := fixtureServer(t, nil)
	src := testSource(srv)
	out := runRead(t, src, catalogOf(t, src, synclet.SyncModeFullRefresh, allTypesStream), nil)
	require.NoError(t, out.Err)
	recs := records(t, out, allTypesStream)
	require.Len(t, recs, 1)
	assert.Len(t, recs[0], 38+2)
	assert.Equal(t, []any{float64(5), nil}, recs[0]["Linked Scores"])
	assert.Equal(t, map[string]any{"anything": []any{float64(1), "x"}}, recs[0]["Broken Formula"], "a formula without a result type passes its value through")
	assert.Len(t, recordHits(t, h, allTypesPath), 1)
}
