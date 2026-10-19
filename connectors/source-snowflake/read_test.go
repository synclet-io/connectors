package main

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
)

func TestReadTypes(t *testing.T) {
	eachFormat(t, func(t *testing.T, format string) {
		src := newFake(t).in(format).on(sessionOK, typesColumnsOK, typesRead).source()
		out := runRead(t, src, passwordConfig(), catalog(fullRefresh(typesStream())), nil)
		require.NoError(t, out.Err)
		got := rawRecords(t, out, typesDesc)
		require.Len(t, got, 2)
		for i := range got {
			require.JSONEq(t, typesRecords[i], got[i])
		}
		require.Contains(t, got[0], `"C_NUM38":12345678901234567890123456789012345678`, "NUMBER(38,0) keeps every digit")
		require.Empty(t, out.States(), "a full refresh stream sends no state")
	})
}

func TestReadSelectsConfiguredColumns(t *testing.T) {
	eachFormat(t, func(t *testing.T, format string) {
		q := fakeQuery{sql: readEventsIDOnlSQL, columns: textColumns("ID", "UPDATED_AT"), rows: [][]*string{
			row(str("1"), str("2024-01-01T00:00:00.000000000+00:00")),
		}}
		src := newFake(t).in(format).on(sessionOK, eventsColumnsOK, q).source()
		stream := eventsStream()
		stream.JSONSchema = json.RawMessage(`{"type":"object","properties":{"ID":{}}}`)
		out := runRead(t, src, passwordConfig(), catalog(incremental(stream, "UPDATED_AT")), nil)
		require.NoError(t, out.Err)
		require.Equal(t, []string{`{"ID":1}`}, rawRecords(t, out, eventsDesc))
		require.Equal(t, []streamState{{CursorField: []string{"UPDATED_AT"}, Cursor: str("2024-01-01T00:00:00.000000000+00:00")}}, streamStates(t, out))
	})
}

func TestIncrementalResume(t *testing.T) {
	eachFormat(t, func(t *testing.T, format string) {
		first := fakeQuery{sql: readEventsSQL, columns: eventsReadColumns, rows: eventRows}
		fromT2 := fakeQuery{sql: readEventsFromSQL, bindings: []string{"2024-01-02T00:00:00.000000000+00:00"}, columns: eventsReadColumns, rows: eventRows[1:3]}
		fake := newFake(t).in(format).on(sessionOK, eventsColumnsOK, first, fromT2)
		src := fake.source()
		cat := catalog(incremental(eventsStream(), "UPDATED_AT"))

		out := runRead(t, src, passwordConfig(), cat, nil)
		require.NoError(t, out.Err)
		require.Equal(t, []string{"1", "2", "3", "4"}, ids(t, out, eventsDesc))
		t3 := "2024-01-03T02:00:00.000000000+02:00"
		require.Equal(t, []streamState{{CursorField: []string{"UPDATED_AT"}, Cursor: &t3}}, streamStates(t, out),
			"the NULL cursor of row 4 leaves the greatest cursor in the state")

		t2 := "2024-01-02T00:00:00.000000000+00:00"
		out = runRead(t, src, passwordConfig(), cat, stateFor(t, eventsDesc, streamState{CursorField: []string{"UPDATED_AT"}, Cursor: &t2}))
		require.NoError(t, out.Err)
		require.Equal(t, []string{"2", "3"}, ids(t, out, eventsDesc))
		require.Equal(t, []streamState{{CursorField: []string{"UPDATED_AT"}, Cursor: &t3}}, streamStates(t, out))
		require.Contains(t, fake.executed(), readEventsFromSQL, "the resumed read asks for cursor >= the saved value")
	})
}

func TestIncrementalNoNewRowsKeepsState(t *testing.T) {
	eachFormat(t, func(t *testing.T, format string) {
		t3 := "2024-01-03T02:00:00.000000000+02:00"
		q := fakeQuery{sql: readEventsFromSQL, bindings: []string{t3}, columns: eventsReadColumns}
		src := newFake(t).in(format).on(sessionOK, eventsColumnsOK, q).source()
		out := runRead(t, src, passwordConfig(), catalog(incremental(eventsStream(), "UPDATED_AT")),
			stateFor(t, eventsDesc, streamState{CursorField: []string{"UPDATED_AT"}, Cursor: &t3}))
		require.NoError(t, out.Err)
		require.Empty(t, out.Records(eventsDesc))
		require.Equal(t, []streamState{{CursorField: []string{"UPDATED_AT"}, Cursor: &t3}}, streamStates(t, out))
	})
}

func TestIncrementalCursorFieldChanged(t *testing.T) {
	eachFormat(t, func(t *testing.T, format string) {
		q := fakeQuery{sql: readEventsByIDSQL, columns: eventsReadColumns, rows: eventRows[:1]}
		src := newFake(t).in(format).on(sessionOK, eventsColumnsOK, q).source()
		t3 := "2024-01-03T02:00:00.000000000+02:00"
		out := runRead(t, src, passwordConfig(), catalog(incremental(eventsStream(), "ID")),
			stateFor(t, eventsDesc, streamState{CursorField: []string{"UPDATED_AT"}, Cursor: &t3}))
		require.NoError(t, out.Err)
		require.Equal(t, []string{"1"}, ids(t, out, eventsDesc))
		require.Equal(t, []streamState{{CursorField: []string{"ID"}, Cursor: str("1")}}, streamStates(t, out))
	})
}

func TestIncrementalCheckpoints(t *testing.T) {
	eachFormat(t, func(t *testing.T, format string) {
		defer func(n int) { checkpointRows = n }(checkpointRows)
		checkpointRows = 2
		q := fakeQuery{sql: readEventsSQL, columns: eventsReadColumns, rows: eventRows[:3]}
		src := newFake(t).in(format).on(sessionOK, eventsColumnsOK, q).source()
		out := runRead(t, src, passwordConfig(), catalog(incremental(eventsStream(), "UPDATED_AT")), nil)
		require.NoError(t, out.Err)
		t2, t3 := "2024-01-02T00:00:00.000000000+00:00", "2024-01-03T02:00:00.000000000+02:00"
		require.Equal(t, []streamState{
			{CursorField: []string{"UPDATED_AT"}, Cursor: &t2},
			{CursorField: []string{"UPDATED_AT"}, Cursor: &t3},
		}, streamStates(t, out))
	})
}

func TestReadFailsLoudly(t *testing.T) {
	eachFormat(t, func(t *testing.T, format string) {
		bad := fakeQuery{sql: readEventsFullSQL, columns: eventsReadColumns, rows: [][]*string{
			row(str("1"), str("one"), str("2024-01-01T00:00:00.000000000+00:00")),
			row(str("2.5"), str("two"), str("2024-01-02T00:00:00.000000000+00:00")),
		}}
		src := newFake(t).in(format).on(sessionOK, eventsColumnsOK, bad).source()
		out := runRead(t, src, passwordConfig(), catalog(fullRefresh(eventsStream())), nil)
		require.ErrorContains(t, out.Err, `read "SYNCLET_DB"."PUBLIC"."EVENTS": column "ID": integer value "2.5"`)
		require.Equal(t, []string{"1"}, ids(t, out, eventsDesc))
	})
}

func TestReadConfigErrors(t *testing.T) {
	missing := fakeQuery{sql: tableColumnsSQL, bindings: []string{"PUBLIC", "GONE"}, columns: discoverColumns[2:]}
	gone := synclet.Stream{Name: "GONE", Namespace: "PUBLIC", JSONSchema: json.RawMessage(`{"type":"object","properties":{"ID":{}}}`)}
	noNamespace := eventsStream()
	noNamespace.Namespace = ""
	twoSchemas := passwordConfig()
	twoSchemas.Schemas = []string{"PUBLIC", "OTHER"}
	extraColumn := eventsStream()
	extraColumn.JSONSchema = json.RawMessage(`{"type":"object","properties":{"ID":{},"NOPE":{}}}`)
	variantInfo := fakeQuery{sql: tableColumnsSQL, bindings: []string{"PUBLIC", "EVENTS"}, columns: discoverColumns[2:], rows: [][]*string{
		row(str("ID"), str("NUMBER"), str("38"), str("0")), row(str("DOC"), str("VARIANT"), null, null),
	}}
	idOnly := eventsStream()
	idOnly.JSONSchema = json.RawMessage(`{"type":"object","properties":{"ID":{}}}`)
	noCursor := incremental(eventsStream(), "")
	noCursor.CursorField = nil

	cases := []struct {
		name  string
		fake  []fakeQuery
		cfg   Config
		cs    synclet.ConfiguredStream
		state json.RawMessage
		want  string
	}{
		{"missing table", []fakeQuery{missing}, passwordConfig(), fullRefresh(gone), nil, `stream PUBLIC.GONE: no table or view "SYNCLET_DB"."PUBLIC"."GONE" is visible to role "SYNCLET_ROLE"`},
		{"no namespace", nil, twoSchemas, fullRefresh(noNamespace), nil, "stream EVENTS: set the stream's namespace to its schema"},
		{"unknown column", []fakeQuery{eventsColumnsOK}, passwordConfig(), fullRefresh(extraColumn), nil, `stream PUBLIC.EVENTS: column "NOPE" does not exist`},
		{"no cursor", []fakeQuery{eventsColumnsOK}, passwordConfig(), noCursor, nil, "stream PUBLIC.EVENTS: incremental sync needs a cursor_field naming one column"},
		{"unknown cursor", []fakeQuery{eventsColumnsOK}, passwordConfig(), incremental(eventsStream(), "NOPE"), nil, `stream PUBLIC.EVENTS: cursor column "NOPE" does not exist`},
		{"variant cursor", []fakeQuery{variantInfo}, passwordConfig(), incremental(idOnly, "DOC"), nil, `stream PUBLIC.EVENTS: column "DOC" of type VARIANT cannot be a cursor`},
		{"bad state", []fakeQuery{eventsColumnsOK}, passwordConfig(), incremental(eventsStream(), "UPDATED_AT"),
			json.RawMessage(`[{"type":"STREAM","stream":{"stream_descriptor":{"name":"EVENTS","namespace":"PUBLIC"},"stream_state":{"cursor":5}}}]`), "stream PUBLIC.EVENTS: invalid state"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			src := newFake(t).on(append([]fakeQuery{sessionOK}, tc.fake...)...).source()
			out := runRead(t, src, tc.cfg, catalog(tc.cs), tc.state)
			require.ErrorContains(t, out.Err, tc.want)
			traces := out.ErrorTraces()
			require.Len(t, traces, 1)
			require.Equal(t, synclet.FailureTypeConfigError, traces[0].FailureType, fmt.Sprint(out.Err))
		})
	}
}

func TestIncrementalRejectsNonFiniteFloatCursor(t *testing.T) {
	info := fakeQuery{sql: tableColumnsSQL, bindings: []string{"PUBLIC", "MEASURES"}, columns: discoverColumns[2:], rows: [][]*string{
		row(str("ID"), str("NUMBER"), str("38"), str("0")), row(str("V"), str("FLOAT"), null, null),
	}}
	stream := synclet.Stream{Name: "MEASURES", Namespace: "PUBLIC", JSONSchema: json.RawMessage(`{"type":"object","properties":{"ID":{},"V":{}}}`)}
	desc := synclet.StreamDescriptor{Name: "MEASURES", Namespace: "PUBLIC"}
	readSQL := `SELECT TO_VARCHAR("ID"), "V" FROM "SYNCLET_DB"."PUBLIC"."MEASURES" ORDER BY "V" ASC NULLS LAST`
	for _, bad := range []string{"NaN", "inf", "-inf"} {
		t.Run(bad, func(t *testing.T) {
			eachFormat(t, func(t *testing.T, format string) {
				q := fakeQuery{sql: readSQL, columns: []fakeColumn{{"ID", "text"}, {"V", "real"}}, rows: [][]*string{
					row(str("1"), str("1.5")), row(str("2"), str(bad)),
				}}
				src := newFake(t).in(format).on(sessionOK, info, q).source()
				out := runRead(t, src, passwordConfig(), catalog(incremental(stream, "V")), nil)
				require.ErrorContains(t, out.Err, `stream PUBLIC.MEASURES: cursor column "V" is`)
				require.ErrorContains(t, out.Err, "at row 2")
				require.Equal(t, []string{"1"}, ids(t, out, desc))
				require.Empty(t, out.States(), "a non-finite cursor never becomes state")
			})
		})
		t.Run("saved "+bad, func(t *testing.T) {
			src := newFake(t).on(sessionOK, info).source()
			out := runRead(t, src, passwordConfig(), catalog(incremental(stream, "V")),
				stateFor(t, desc, streamState{CursorField: []string{"V"}, Cursor: str(bad)}))
			require.ErrorContains(t, out.Err, "stream PUBLIC.MEASURES: invalid state cursor")
			require.Empty(t, out.States())
		})
	}
}

// eachFormat runs test once in each result format.
func eachFormat(t *testing.T, test func(t *testing.T, format string)) {
	t.Helper()
	for _, format := range resultFormats {
		t.Run(format, func(t *testing.T) { test(t, format) })
	}
}

// chunkedEvents is a read of events whose rows come inline and in two
// downloaded chunks.
func chunkedEvents(failChunk int) fakeQuery {
	var rows [][]*string
	for i := 1; i <= 9; i++ {
		rows = append(rows, row(str(fmt.Sprint(i)), str(fmt.Sprintf("e%d", i)), str(fmt.Sprintf("2024-01-%02dT00:00:00.000000000+00:00", i))))
	}
	return fakeQuery{sql: readEventsSQL, columns: eventsReadColumns, rows: rows[:3], chunks: [][][]*string{rows[3:6], rows[6:]}, failChunk: failChunk}
}

func TestReadChunks(t *testing.T) {
	eachFormat(t, func(t *testing.T, format string) {
		src := newFake(t).in(format).on(sessionOK, eventsColumnsOK, chunkedEvents(0)).source()
		out := runRead(t, src, passwordConfig(), catalog(incremental(eventsStream(), "UPDATED_AT")), nil)
		require.NoError(t, out.Err)
		require.Equal(t, []string{"1", "2", "3", "4", "5", "6", "7", "8", "9"}, ids(t, out, eventsDesc))
		require.Equal(t, []streamState{{CursorField: []string{"UPDATED_AT"}, Cursor: str("2024-01-09T00:00:00.000000000+00:00")}}, streamStates(t, out))
	})
}

func TestReadFailedChunk(t *testing.T) {
	eachFormat(t, func(t *testing.T, format string) {
		src := newFake(t).in(format).on(sessionOK, eventsColumnsOK, chunkedEvents(2)).source()
		out := runRead(t, src, passwordConfig(), catalog(incremental(eventsStream(), "UPDATED_AT")), nil)
		require.ErrorContains(t, out.Err, `read "SYNCLET_DB"."PUBLIC"."EVENTS"`)
		got := ids(t, out, eventsDesc)
		require.NotEmpty(t, got, "the rows before the failed chunk are emitted")
		require.Less(t, len(got), 9)
		require.Equal(t, []string{"1", "2", "3", "4", "5", "6"}[:len(got)], got)
		require.Empty(t, out.States(), "no STATE follows the rows of a failed read")
	})
}
