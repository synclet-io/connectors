package main

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
	"github.com/synclet-io/synclet-go-sdk/testkit"
)

const (
	discoverSQL     = `SELECT c.TABLE_SCHEMA, c.TABLE_NAME, c.COLUMN_NAME, c.DATA_TYPE, c.NUMERIC_PRECISION, c.NUMERIC_SCALE FROM "SYNCLET_DB".INFORMATION_SCHEMA.COLUMNS c JOIN "SYNCLET_DB".INFORMATION_SCHEMA.TABLES t ON t.TABLE_SCHEMA = c.TABLE_SCHEMA AND t.TABLE_NAME = c.TABLE_NAME WHERE c.TABLE_SCHEMA <> 'INFORMATION_SCHEMA' AND t.TABLE_TYPE IN ('BASE TABLE', 'VIEW', 'MATERIALIZED VIEW') AND c.TABLE_SCHEMA IN (?) ORDER BY c.TABLE_SCHEMA, c.TABLE_NAME, c.ORDINAL_POSITION`
	discoverAllSQL  = `SELECT c.TABLE_SCHEMA, c.TABLE_NAME, c.COLUMN_NAME, c.DATA_TYPE, c.NUMERIC_PRECISION, c.NUMERIC_SCALE FROM "SYNCLET_DB".INFORMATION_SCHEMA.COLUMNS c JOIN "SYNCLET_DB".INFORMATION_SCHEMA.TABLES t ON t.TABLE_SCHEMA = c.TABLE_SCHEMA AND t.TABLE_NAME = c.TABLE_NAME WHERE c.TABLE_SCHEMA <> 'INFORMATION_SCHEMA' AND t.TABLE_TYPE IN ('BASE TABLE', 'VIEW', 'MATERIALIZED VIEW') ORDER BY c.TABLE_SCHEMA, c.TABLE_NAME, c.ORDINAL_POSITION`
	primaryKeysSQL  = `SHOW PRIMARY KEYS IN DATABASE "SYNCLET_DB"`
	tableColumnsSQL = `SELECT COLUMN_NAME, DATA_TYPE, NUMERIC_PRECISION, NUMERIC_SCALE FROM "SYNCLET_DB".INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION`

	tzFormat = `'YYYY-MM-DD"T"HH24:MI:SS.FF9TZH:TZM'`

	readTypesSQL = `SELECT TO_VARCHAR("C_NUM38"), TO_VARCHAR("C_DEC"), "C_FLOAT", "C_TEXT", TO_VARCHAR("C_BOOL"), TO_VARCHAR("C_DATE", 'YYYY-MM-DD'), ` +
		`TO_VARCHAR("C_TIME", 'HH24:MI:SS.FF9'), TO_VARCHAR("C_NTZ", 'YYYY-MM-DD"T"HH24:MI:SS.FF9'), TO_VARCHAR("C_LTZ", ` + tzFormat + `), ` +
		`TO_VARCHAR("C_TZ", ` + tzFormat + `), TO_JSON("C_VARIANT"), TO_JSON("C_OBJECT"), TO_JSON("C_ARRAY"), TO_VARCHAR("C_BINARY", 'BASE64'), ` +
		`ST_ASWKT("C_GEO") FROM "SYNCLET_DB"."PUBLIC"."TYPES"`
	readEventsSQL      = `SELECT TO_VARCHAR("ID"), "NAME", TO_VARCHAR("UPDATED_AT", ` + tzFormat + `) FROM "SYNCLET_DB"."PUBLIC"."EVENTS" ORDER BY "UPDATED_AT" ASC NULLS LAST`
	readEventsFromSQL  = `SELECT TO_VARCHAR("ID"), "NAME", TO_VARCHAR("UPDATED_AT", ` + tzFormat + `) FROM "SYNCLET_DB"."PUBLIC"."EVENTS" WHERE "UPDATED_AT" >= TO_TIMESTAMP_TZ(?, ` + tzFormat + `) ORDER BY "UPDATED_AT" ASC NULLS LAST`
	readEventsFullSQL  = `SELECT TO_VARCHAR("ID"), "NAME", TO_VARCHAR("UPDATED_AT", ` + tzFormat + `) FROM "SYNCLET_DB"."PUBLIC"."EVENTS"`
	readEventsByIDSQL  = `SELECT TO_VARCHAR("ID"), "NAME", TO_VARCHAR("UPDATED_AT", ` + tzFormat + `) FROM "SYNCLET_DB"."PUBLIC"."EVENTS" ORDER BY "ID" ASC NULLS LAST`
	readEventsIDOnlSQL = `SELECT TO_VARCHAR("ID"), TO_VARCHAR("UPDATED_AT", ` + tzFormat + `) FROM "SYNCLET_DB"."PUBLIC"."EVENTS" ORDER BY "UPDATED_AT" ASC NULLS LAST`
)

// infoColumn is a row of the discover query.
func infoColumn(schema, table, name, dataType, precision, scale string) []*string {
	cell := func(s string) *string {
		if s == "" {
			return nil
		}
		return str(s)
	}
	return row(str(schema), str(table), str(name), str(dataType), cell(precision), cell(scale))
}

var (
	typesInfo = [][]*string{
		infoColumn("PUBLIC", "TYPES", "C_NUM38", "NUMBER", "38", "0"),
		infoColumn("PUBLIC", "TYPES", "C_DEC", "NUMBER", "10", "2"),
		infoColumn("PUBLIC", "TYPES", "C_FLOAT", "FLOAT", "", ""),
		infoColumn("PUBLIC", "TYPES", "C_TEXT", "TEXT", "", ""),
		infoColumn("PUBLIC", "TYPES", "C_BOOL", "BOOLEAN", "", ""),
		infoColumn("PUBLIC", "TYPES", "C_DATE", "DATE", "", ""),
		infoColumn("PUBLIC", "TYPES", "C_TIME", "TIME", "", ""),
		infoColumn("PUBLIC", "TYPES", "C_NTZ", "TIMESTAMP_NTZ", "", ""),
		infoColumn("PUBLIC", "TYPES", "C_LTZ", "TIMESTAMP_LTZ", "", ""),
		infoColumn("PUBLIC", "TYPES", "C_TZ", "TIMESTAMP_TZ", "", ""),
		infoColumn("PUBLIC", "TYPES", "C_VARIANT", "VARIANT", "", ""),
		infoColumn("PUBLIC", "TYPES", "C_OBJECT", "OBJECT", "", ""),
		infoColumn("PUBLIC", "TYPES", "C_ARRAY", "ARRAY", "", ""),
		infoColumn("PUBLIC", "TYPES", "C_BINARY", "BINARY", "", ""),
		infoColumn("PUBLIC", "TYPES", "C_GEO", "GEOGRAPHY", "", ""),
	}
	eventsInfo = [][]*string{
		infoColumn("PUBLIC", "EVENTS", "ID", "NUMBER", "38", "0"),
		infoColumn("PUBLIC", "EVENTS", "NAME", "TEXT", "", ""),
		infoColumn("PUBLIC", "EVENTS", "UPDATED_AT", "TIMESTAMP_TZ", "", ""),
	}
	discoverColumns = textColumns("TABLE_SCHEMA", "TABLE_NAME", "COLUMN_NAME", "DATA_TYPE", "NUMERIC_PRECISION", "NUMERIC_SCALE")

	discoverOK = fakeQuery{sql: discoverSQL, bindings: []string{"PUBLIC"}, columns: discoverColumns, rows: append(append([][]*string{}, eventsInfo...), typesInfo...)}
	pkColumns  = textColumns("created_on", "database_name", "schema_name", "table_name", "column_name", "key_sequence", "constraint_name", "rely", "comment")
	pkOK       = fakeQuery{sql: primaryKeysSQL, columns: pkColumns, rows: [][]*string{
		row(str("2024-01-01 00:00:00.000 -0800"), str("SYNCLET_DB"), str("PUBLIC"), str("EVENTS"), str("ID"), str("1"), str("SYS_CONSTRAINT_1"), str("false"), null),
	}}

	typesSchema = `{"type":"object","properties":{` +
		`"C_NUM38":{"type":["integer","null"]},"C_DEC":{"type":["number","null"]},"C_FLOAT":{"type":["number","null"]},` +
		`"C_TEXT":{"type":["string","null"]},"C_BOOL":{"type":["boolean","null"]},"C_DATE":{"type":["string","null"],"format":"date"},` +
		`"C_TIME":{"type":["string","null"],"airbyte_type":"time_without_timezone"},` +
		`"C_NTZ":{"type":["string","null"],"airbyte_type":"timestamp_without_timezone"},` +
		`"C_LTZ":{"type":["string","null"],"airbyte_type":"timestamp_with_timezone","format":"date-time"},` +
		`"C_TZ":{"type":["string","null"],"airbyte_type":"timestamp_with_timezone","format":"date-time"},` +
		`"C_VARIANT":{},"C_OBJECT":{"type":["object","null"]},"C_ARRAY":{"type":["array","null"]},` +
		`"C_BINARY":{"type":["string","null"]},"C_GEO":{"type":["string","null"]}}}`
	eventsSchema = `{"type":"object","properties":{"ID":{"type":["integer","null"]},"NAME":{"type":["string","null"]},` +
		`"UPDATED_AT":{"type":["string","null"],"airbyte_type":"timestamp_with_timezone","format":"date-time"}}}`

	typesColumnsOK  = fakeQuery{sql: tableColumnsSQL, bindings: []string{"PUBLIC", "TYPES"}, columns: discoverColumns[2:], rows: tail(typesInfo)}
	eventsColumnsOK = fakeQuery{sql: tableColumnsSQL, bindings: []string{"PUBLIC", "EVENTS"}, columns: discoverColumns[2:], rows: tail(eventsInfo)}

	typesRead = fakeQuery{sql: readTypesSQL, columns: append(textColumns("C_NUM38", "C_DEC"), fakeColumn{"C_FLOAT", "real"}, fakeColumn{"C_TEXT", "text"},
		fakeColumn{"C_BOOL", "text"}, fakeColumn{"C_DATE", "text"}, fakeColumn{"C_TIME", "text"}, fakeColumn{"C_NTZ", "text"}, fakeColumn{"C_LTZ", "text"},
		fakeColumn{"C_TZ", "text"}, fakeColumn{"C_VARIANT", "text"}, fakeColumn{"C_OBJECT", "text"}, fakeColumn{"C_ARRAY", "text"},
		fakeColumn{"C_BINARY", "text"}, fakeColumn{"C_GEO", "text"}),
		rows: [][]*string{
			row(str("12345678901234567890123456789012345678"), str("-123.45"), str("1.5"), str("héllo"), str("true"), str("2024-02-29"),
				str("23:59:59.123456000"), str("2024-02-29T12:34:56.789000000"), str("2024-02-29T10:34:56.789000000+00:00"),
				str("2024-02-29T12:34:56.789000000+02:00"), str(`{"a":[1,2],"n":123456789012345678901234567890}`), str(`{"k":"x"}`),
				str(`[1,"two",null]`), str("AP8="), str("POINT(30.5 50.45)")),
			row(null, null, null, null, null, null, null, null, null, null, null, null, null, null, null),
		}}
	typesRecords = []string{
		`{"C_NUM38":12345678901234567890123456789012345678,"C_DEC":-123.45,"C_FLOAT":1.5,"C_TEXT":"héllo","C_BOOL":true,"C_DATE":"2024-02-29",` +
			`"C_TIME":"23:59:59.123456","C_NTZ":"2024-02-29T12:34:56.789","C_LTZ":"2024-02-29T10:34:56.789Z","C_TZ":"2024-02-29T10:34:56.789Z",` +
			`"C_VARIANT":{"a":[1,2],"n":123456789012345678901234567890},"C_OBJECT":{"k":"x"},"C_ARRAY":[1,"two",null],"C_BINARY":"AP8=","C_GEO":"POINT(30.5 50.45)"}`,
		`{"C_NUM38":null,"C_DEC":null,"C_FLOAT":null,"C_TEXT":null,"C_BOOL":null,"C_DATE":null,"C_TIME":null,"C_NTZ":null,"C_LTZ":null,"C_TZ":null,` +
			`"C_VARIANT":null,"C_OBJECT":null,"C_ARRAY":null,"C_BINARY":null,"C_GEO":null}`,
	}

	eventsReadColumns = textColumns("ID", "NAME", "UPDATED_AT")
	eventRows         = [][]*string{
		row(str("1"), str("one"), str("2024-01-01T00:00:00.000000000+00:00")),
		row(str("2"), str("two"), str("2024-01-02T00:00:00.000000000+00:00")),
		row(str("3"), str("three"), str("2024-01-03T02:00:00.000000000+02:00")),
		row(str("4"), str("four"), null),
	}
)

// tail drops the schema and table cells of discover rows.
func tail(rows [][]*string) [][]*string {
	out := make([][]*string, len(rows))
	for i, r := range rows {
		out[i] = r[2:]
	}
	return out
}

var (
	eventsDesc = synclet.StreamDescriptor{Name: "EVENTS", Namespace: "PUBLIC"}
	typesDesc  = synclet.StreamDescriptor{Name: "TYPES", Namespace: "PUBLIC"}
)

func eventsStream() synclet.Stream {
	return synclet.Stream{Name: "EVENTS", Namespace: "PUBLIC", JSONSchema: json.RawMessage(eventsSchema),
		SupportedSyncModes: []synclet.SyncMode{synclet.SyncModeFullRefresh, synclet.SyncModeIncremental}, SourceDefinedPrimaryKey: [][]string{{"ID"}}}
}

func typesStream() synclet.Stream {
	return synclet.Stream{Name: "TYPES", Namespace: "PUBLIC", JSONSchema: json.RawMessage(typesSchema),
		SupportedSyncModes: []synclet.SyncMode{synclet.SyncModeFullRefresh, synclet.SyncModeIncremental}}
}

func fullRefresh(s synclet.Stream) synclet.ConfiguredStream {
	return synclet.ConfiguredStream{Stream: s, SyncMode: synclet.SyncModeFullRefresh, DestinationSyncMode: synclet.DestinationSyncModeOverwrite}
}

func incremental(s synclet.Stream, cursor string) synclet.ConfiguredStream {
	return synclet.ConfiguredStream{Stream: s, SyncMode: synclet.SyncModeIncremental, CursorField: []string{cursor},
		DestinationSyncMode: synclet.DestinationSyncModeAppendDedup, PrimaryKey: [][]string{{"ID"}}}
}

func catalog(streams ...synclet.ConfiguredStream) *synclet.ConfiguredCatalog {
	return &synclet.ConfiguredCatalog{Streams: streams}
}

func runRead(t *testing.T, src Source, cfg Config, cat *synclet.ConfiguredCatalog, state json.RawMessage) *testkit.Output {
	t.Helper()
	out, err := testkit.RunSource(t.Context(), src, synclet.Request{
		Command: synclet.CommandRead, Config: mustJSON(t, cfg), Catalog: mustJSON(t, cat), State: state,
	})
	require.NoError(t, err)
	return out
}

func rawRecords(t *testing.T, out *testkit.Output, desc synclet.StreamDescriptor) []string {
	t.Helper()
	var got []string
	for _, r := range out.Records(desc) {
		got = append(got, string(r.Data))
	}
	return got
}

func ids(t *testing.T, out *testkit.Output, desc synclet.StreamDescriptor) []string {
	t.Helper()
	var got []string
	for _, r := range out.Records(desc) {
		var row struct {
			ID json.Number `json:"ID"`
		}
		require.NoError(t, json.Unmarshal(r.Data, &row))
		got = append(got, row.ID.String())
	}
	return got
}

func streamStates(t *testing.T, out *testkit.Output) []streamState {
	t.Helper()
	var got []streamState
	for _, s := range out.States() {
		var st streamState
		require.NoError(t, json.Unmarshal(s.Stream.StreamState, &st))
		got = append(got, st)
	}
	return got
}

func stateFor(t *testing.T, desc synclet.StreamDescriptor, st streamState) json.RawMessage {
	t.Helper()
	return mustJSON(t, []synclet.StateMessage{{
		Type:   synclet.StateTypeStream,
		Stream: &synclet.StreamState{StreamDescriptor: desc, StreamState: mustJSON(t, st)},
	}})
}
