package main

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSelectQuery(t *testing.T) {
	tbl := &table{schema: "my schema", name: `we"ird`, columns: []column{
		{name: "id", typ: colType{kind: kindInt}, sqlType: "integer"},
		{name: "blob", typ: colType{kind: kindBytea}, sqlType: "bytea"},
		{name: "at", typ: colType{kind: kindTimestampTZ}, sqlType: "timestamp with time zone"},
	}}

	full, args := selectQuery{table: tbl, columns: tbl.columns}.sql()
	require.Empty(t, args)
	require.Equal(t, `SELECT row_to_json(_synclet_r)::text, NULL::text FROM "my schema"."we""ird" AS _synclet_t, `+
		`LATERAL (SELECT _synclet_t."id" AS "id", translate(encode(_synclet_t."blob", 'base64'), E'\n', '') AS "blob", _synclet_t."at" AS "at") AS _synclet_r`, full)

	cursor := tbl.columns[2]
	first, args := selectQuery{table: tbl, columns: tbl.columns[:1], cursor: &cursor}.sql()
	require.Empty(t, args)
	require.Equal(t, `SELECT row_to_json(_synclet_r)::text, _synclet_t."at"::text FROM "my schema"."we""ird" AS _synclet_t, `+
		`LATERAL (SELECT _synclet_t."id" AS "id") AS _synclet_r ORDER BY _synclet_t."at"`, first)

	from := "2024-01-02 00:00:00+00"
	resumed, args := selectQuery{table: tbl, columns: tbl.columns[:1], cursor: &cursor, from: &from}.sql()
	require.Equal(t, []any{from}, args)
	require.Equal(t, `SELECT row_to_json(_synclet_r)::text, _synclet_t."at"::text FROM "my schema"."we""ird" AS _synclet_t, `+
		`LATERAL (SELECT _synclet_t."id" AS "id") AS _synclet_r WHERE _synclet_t."at" >= $1::timestamp with time zone ORDER BY _synclet_t."at"`, resumed)
}

func TestColumnExpr(t *testing.T) {
	cases := []struct {
		typ  colType
		want string
	}{
		{colType{kind: kindInt}, `c`},
		{colType{kind: kindFloat}, `CASE WHEN c::text IN ('NaN', 'Infinity', '-Infinity') THEN NULL ELSE c END`},
		{colType{kind: kindOther}, `c::text`},
		{colType{kind: kindTimeTZ}, `regexp_replace(c::text, '([+-][0-9]{2})$', '\1:00')`},
		{colType{kind: kindJSON}, `c`},
		{colType{kind: kindInt, array: true}, `c`},
		{colType{kind: kindBytea, array: true}, `c::text[]`},
		{colType{kind: kindOther, array: true}, `c::text[]`},
		{colType{kind: kindFloat, array: true}, `CASE WHEN c IS NULL THEN NULL ELSE ARRAY(SELECT CASE WHEN e::text IN ('NaN', 'Infinity', '-Infinity') THEN NULL ELSE e END FROM unnest(c) WITH ORDINALITY AS u(e, i) ORDER BY i) END`},
	}
	for _, tc := range cases {
		require.Equal(t, tc.want, tc.typ.expr("c"))
	}
}

func TestColumnSchema(t *testing.T) {
	cases := []struct {
		typ  colType
		want string
	}{
		{colType{kind: kindBool}, `{"type":["boolean","null"]}`},
		{colType{kind: kindInt}, `{"type":["integer","null"]}`},
		{colType{kind: kindFloat}, `{"type":["number","null"]}`},
		{colType{kind: kindString}, `{"type":["string","null"]}`},
		{colType{kind: kindBytea}, `{"type":["string","null"]}`},
		{colType{kind: kindOther}, `{"type":["string","null"]}`},
		{colType{kind: kindInterval}, `{"type":["string","null"]}`},
		{colType{kind: kindJSON}, `{}`},
		{colType{kind: kindDate}, `{"type":["string","null"],"format":"date"}`},
		{colType{kind: kindTime}, `{"type":["string","null"],"airbyte_type":"time_without_timezone"}`},
		{colType{kind: kindTimeTZ}, `{"type":["string","null"],"airbyte_type":"time_with_timezone","format":"time"}`},
		{colType{kind: kindTimestamp}, `{"type":["string","null"],"airbyte_type":"timestamp_without_timezone"}`},
		{colType{kind: kindTimestampTZ}, `{"type":["string","null"],"airbyte_type":"timestamp_with_timezone","format":"date-time"}`},
		{colType{kind: kindInt, array: true}, `{"type":["array","null"],"items":{"type":["integer","null"]}}`},
		{colType{kind: kindBytea, array: true}, `{"type":["array","null"],"items":{"type":["string","null"]}}`},
		{colType{kind: kindJSON, array: true}, `{"type":["array","null"]}`},
		{colType{kind: kindDate, array: true}, `{"type":["array","null"],"items":{"format":"date","type":["string","null"]}}`},
	}
	for _, tc := range cases {
		got, err := json.Marshal(tc.typ.schema())
		require.NoError(t, err)
		require.JSONEq(t, tc.want, string(got))
	}
}

func TestKindOf(t *testing.T) {
	r := &typeResolver{types: map[uint32]pgType{
		1: {name: "int4", typtype: "b", category: "N", builtin: true},
		2: {name: "_int4", typtype: "b", category: "A", elem: 1, builtin: true},
		3: {name: "mood", typtype: "e", category: "E"},
		4: {name: "positive_int", typtype: "d", category: "N", base: 1},
		5: {name: "_positive_int", typtype: "b", category: "A", elem: 4},
		6: {name: "point", typtype: "b", category: "G", builtin: true},
		7: {name: "int4", typtype: "b", category: "N"},
		8: {name: "name", typtype: "b", category: "S", elem: 9, builtin: true},
		9: {name: "char", typtype: "b", category: "Z", builtin: true},
	}}
	require.Equal(t, colType{kind: kindInt}, r.resolve(1))
	require.Equal(t, colType{kind: kindInt, array: true}, r.resolve(2))
	require.Equal(t, colType{kind: kindString}, r.resolve(3))
	require.Equal(t, colType{kind: kindInt}, r.resolve(4))
	require.Equal(t, colType{kind: kindInt, array: true}, r.resolve(5))
	require.Equal(t, colType{kind: kindOther}, r.resolve(6))
	require.Equal(t, colType{kind: kindOther}, r.resolve(7), "a user type named like a builtin")
	require.Equal(t, colType{kind: kindString}, r.resolve(8))
}
