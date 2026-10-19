package main

import (
	"database/sql"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func num(precision, scale int64) column {
	return column{name: "C", dataType: "NUMBER", precision: sql.NullInt64{Int64: precision, Valid: true}, scale: sql.NullInt64{Int64: scale, Valid: true}}
}

func col(dataType string) column { return column{name: "C", dataType: dataType} }

func TestColumnSchema(t *testing.T) {
	cases := []struct {
		col    column
		schema string
		expr   string
	}{
		{num(38, 0), `{"type":["integer","null"]}`, `TO_VARCHAR("C")`},
		{num(10, 2), `{"type":["number","null"]}`, `TO_VARCHAR("C")`},
		{col("DECFLOAT"), `{"type":["number","null"]}`, `TO_VARCHAR("C")`},
		{col("FLOAT"), `{"type":["number","null"]}`, `"C"`},
		{col("TEXT"), `{"type":["string","null"]}`, `"C"`},
		{col("BOOLEAN"), `{"type":["boolean","null"]}`, `TO_VARCHAR("C")`},
		{col("DATE"), `{"type":["string","null"],"format":"date"}`, `TO_VARCHAR("C", 'YYYY-MM-DD')`},
		{col("TIME"), `{"type":["string","null"],"airbyte_type":"time_without_timezone"}`, `TO_VARCHAR("C", 'HH24:MI:SS.FF9')`},
		{col("TIMESTAMP_NTZ"), `{"type":["string","null"],"airbyte_type":"timestamp_without_timezone"}`, `TO_VARCHAR("C", 'YYYY-MM-DD"T"HH24:MI:SS.FF9')`},
		{col("TIMESTAMP_LTZ"), `{"type":["string","null"],"airbyte_type":"timestamp_with_timezone","format":"date-time"}`, `TO_VARCHAR("C", 'YYYY-MM-DD"T"HH24:MI:SS.FF9TZH:TZM')`},
		{col("TIMESTAMP_TZ"), `{"type":["string","null"],"airbyte_type":"timestamp_with_timezone","format":"date-time"}`, `TO_VARCHAR("C", 'YYYY-MM-DD"T"HH24:MI:SS.FF9TZH:TZM')`},
		{col("VARIANT"), `{}`, `TO_JSON("C")`},
		{col("OBJECT"), `{"type":["object","null"]}`, `TO_JSON("C")`},
		{col("MAP"), `{"type":["object","null"]}`, `TO_JSON("C")`},
		{col("ARRAY"), `{"type":["array","null"]}`, `TO_JSON("C")`},
		{col("VECTOR"), `{"type":["array","null"],"items":{"type":"number"}}`, `TO_JSON("C"::ARRAY)`},
		{col("BINARY"), `{"type":["string","null"]}`, `TO_VARCHAR("C", 'BASE64')`},
		{col("GEOGRAPHY"), `{"type":["string","null"]}`, `ST_ASWKT("C")`},
		{col("GEOMETRY"), `{"type":["string","null"]}`, `ST_ASWKT("C")`},
	}
	for _, tc := range cases {
		t.Run(tc.col.dataType+tc.expr, func(t *testing.T) {
			k, err := tc.col.kind()
			require.NoError(t, err)
			require.JSONEq(t, tc.schema, string(mustJSON(t, k.schema())))
			require.Equal(t, tc.expr, k.expr(ident(tc.col.name)))
		})
	}
	_, err := column{name: "F", dataType: "FILE"}.kind()
	require.EqualError(t, err, `column "F" has type FILE, which the connector cannot read`)
}

func TestColumnValue(t *testing.T) {
	cases := []struct {
		col  column
		in   string
		want string
	}{
		{num(38, 0), "99999999999999999999999999999999999999", `99999999999999999999999999999999999999`},
		{num(38, 0), "-12345678901234567890123456789012345678", `-12345678901234567890123456789012345678`},
		{num(38, 37), "-9.9999999999999999999999999999999999999", `-9.9999999999999999999999999999999999999`},
		{num(10, 2), "1.50", `1.50`},
		{col("DECFLOAT"), "1.2345678901234567890123456789012345678E+16000", `1.2345678901234567890123456789012345678E+16000`},
		{col("FLOAT"), "1.5", `1.5`},
		{col("FLOAT"), "-2.2250738585072014e-308", `-2.2250738585072014e-308`},
		{col("FLOAT"), "NaN", `null`},
		{col("FLOAT"), "inf", `null`},
		{col("FLOAT"), "-Inf", `null`},
		{col("TEXT"), "héllo \"q\"", `"héllo \"q\""`},
		{col("BOOLEAN"), "true", `true`},
		{col("BOOLEAN"), "false", `false`},
		{col("DATE"), "2024-02-29", `"2024-02-29"`},
		{col("TIME"), "23:59:59.123456000", `"23:59:59.123456"`},
		{col("TIME"), "00:00:00.000000000", `"00:00:00"`},
		{col("TIMESTAMP_NTZ"), "2024-02-29T12:34:56.789000000", `"2024-02-29T12:34:56.789"`},
		{col("TIMESTAMP_NTZ"), "2024-02-29T12:34:56.000000001", `"2024-02-29T12:34:56.000000001"`},
		{col("TIMESTAMP_LTZ"), "2024-02-29T10:34:56.789000000+00:00", `"2024-02-29T10:34:56.789Z"`},
		{col("TIMESTAMP_TZ"), "2024-02-29T12:34:56.789000000+02:00", `"2024-02-29T10:34:56.789Z"`},
		{col("TIMESTAMP_TZ"), "2024-02-29T00:00:00.000000000-05:30", `"2024-02-29T05:30:00Z"`},
		{col("VARIANT"), `{"a":[1,2],"big":123456789012345678901234567890}`, `{"a":[1,2],"big":123456789012345678901234567890}`},
		{col("VARIANT"), `"just a string"`, `"just a string"`},
		{col("VARIANT"), `null`, `null`},
		{col("OBJECT"), `{"k":"v","n":{"x":1.5}}`, `{"k":"v","n":{"x":1.5}}`},
		{col("ARRAY"), `[1,"two",null,{"three":3}]`, `[1,"two",null,{"three":3}]`},
		{col("VECTOR"), `[0.5,-1.25,3]`, `[0.5,-1.25,3]`},
		{col("BINARY"), "AP8Q", `"AP8Q"`},
		{col("GEOGRAPHY"), "POINT(30.5 50.45)", `"POINT(30.5 50.45)"`},
	}
	for _, tc := range cases {
		t.Run(tc.col.dataType+" "+tc.in, func(t *testing.T) {
			k, err := tc.col.kind()
			require.NoError(t, err)
			v, err := k.value(sql.NullString{String: tc.in, Valid: true})
			require.NoError(t, err)
			b, err := json.Marshal(v)
			require.NoError(t, err)
			require.Equal(t, tc.want, string(b))
		})
	}
	t.Run("NULL", func(t *testing.T) {
		for _, c := range []column{num(38, 0), col("VARIANT"), col("TIMESTAMP_TZ"), col("BINARY")} {
			k, err := c.kind()
			require.NoError(t, err)
			v, err := k.value(sql.NullString{})
			require.NoError(t, err)
			require.Nil(t, v)
		}
	})
}

func TestColumnValueRejectsMalformed(t *testing.T) {
	cases := []struct {
		col column
		in  string
	}{
		{num(38, 0), "1.5"},
		{num(38, 0), "1e5"},
		{num(10, 2), "abc"},
		{col("FLOAT"), "1,5"},
		{col("BOOLEAN"), "TRUE"},
		{col("DATE"), "2024-02-30"},
		{col("TIME"), "25:00:00.000000000"},
		{col("TIMESTAMP_NTZ"), "2024-02-29 12:34:56"},
		{col("TIMESTAMP_TZ"), "2024-02-29T12:34:56.789000000"},
		{col("VARIANT"), `{"a":`},
		{col("ARRAY"), `{"a":1}`},
		{col("OBJECT"), `[1]`},
		{col("BINARY"), "not base64!"},
	}
	for _, tc := range cases {
		t.Run(tc.col.dataType+" "+tc.in, func(t *testing.T) {
			k, err := tc.col.kind()
			require.NoError(t, err)
			_, err = k.value(sql.NullString{String: tc.in, Valid: true})
			require.Error(t, err)
		})
	}
}
