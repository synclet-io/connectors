package main

import (
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
)

func loadCatalog(t *testing.T, name string) *synclet.ConfiguredCatalog {
	t.Helper()
	b, err := os.ReadFile("testdata/" + name)
	require.NoError(t, err)
	var c synclet.ConfiguredCatalog
	require.NoError(t, json.Unmarshal(b, &c))
	return &c
}

func TestColumnTypesOfFixtureCatalog(t *testing.T) {
	plans, err := planStreams(loadCatalog(t, "types_catalog.json"))
	require.NoError(t, err)
	require.Len(t, plans, 1)
	p := plans[0]
	assert.Equal(t, "ALL_TYPES", p.table)
	got := map[string]string{}
	for _, c := range p.columns {
		got[c.ident] = string(c.typ)
	}
	assert.Equal(t, map[string]string{
		"ID":        "NUMBER(38,0)",
		"FLAG":      "BOOLEAN",
		"COUNT":     "NUMBER(38,0)",
		"BIG":       "NUMBER(38,0)",
		"AMOUNT":    "FLOAT",
		"NAME":      "VARCHAR",
		"DAY":       "DATE",
		"AT_TZ":     "TIMESTAMP_TZ",
		"AT_TZ2":    "TIMESTAMP_TZ",
		"AT_NTZ":    "TIMESTAMP_NTZ",
		"CLOCK":     "TIME",
		"CLOCK_NTZ": "TIME",
		"CLOCK_TZ":  "VARCHAR",
		"REF_TS":    "TIMESTAMP_TZ",
		"REF_NUM":   "FLOAT",
		"PAYLOAD":   "VARIANT",
		"TAGS":      "VARIANT",
		"MIXED":     "VARIANT",
		"UNION":     "VARIANT",
		"UNTYPED":   "VARIANT",
	}, got)
	var names []string
	for _, c := range p.allColumns() {
		names = append(names, c.ident)
	}
	assert.Equal(t, []string{"_AIRBYTE_RAW_ID", "_AIRBYTE_EXTRACTED_AT"}, names[:2], "meta columns lead")
	assert.Equal(t, "AMOUNT", names[2], "data columns in name order")
}

func TestEncodeValue(t *testing.T) {
	cases := []struct {
		typ  colType
		in   string
		want string
	}{
		{colBoolean, `true`, "true"},
		{colBoolean, `"false"`, "false"},
		{colInteger, `42`, "42"},
		{colInteger, `-7`, "-7"},
		{colInteger, `"12345678901234567890123456789012345678"`, "12345678901234567890123456789012345678"},
		{colInteger, `1.0`, "1"},
		{colInteger, `1e3`, "1000"},
		{colInteger, `-0`, "0"},
		{colNumber, `12.5`, "12.5"},
		{colNumber, `"0.912345678"`, "0.912345678"},
		{colNumber, `1.50000000000000`, "1.5"},
		{colNumber, `2.5e-7`, "2.5e-07"},
		{colNumber, `-1.25E2`, "-125"},
		{colNumber, `00012.0100`, "12.01"},
		{colNumber, `0.30000000000000004`, "0.30000000000000004"},
		{colNumber, `37.77492912345`, "37.77492912345"},
		{colNumber, `1e-10`, "1e-10"},
		{colNumber, `1e29`, "1e+29"},
		{colNumber, `9007199254740992`, "9.007199254740992e+15"},
		{colNumber, `-0`, "-0"},
		{colVarchar, `"héllo \"q\"\n"`, "héllo \"q\"\n"},
		{colVarchar, `15`, "15"},
		{colVarchar, `{"a": 1}`, `{"a": 1}`},
		{colDate, `"2024-02-29"`, "2024-02-29"},
		{colTimestampTZ, `"2024-01-02T03:04:05Z"`, "2024-01-02 03:04:05.000000000 +00:00"},
		{colTimestampTZ, `"2024-01-02 03:04:05.123+05:30"`, "2024-01-02 03:04:05.123000000 +05:30"},
		{colTimestampTZ, `"2024-01-02T03:04:05.123456789-0800"`, "2024-01-02 03:04:05.123456789 -08:00"},
		{colTimestampTZ, `"2024-01-02T03:04"`, "2024-01-02 03:04:00.000000000 +00:00"},
		{colTimestampNTZ, `"2024-01-02T03:04:05.5"`, "2024-01-02 03:04:05.500000000"},
		{colTime, `"23:59:59.999999"`, "23:59:59.999999000"},
		{colTime, `"08:30"`, "08:30:00.000000000"},
		{colVariant, `{"a":[1,2.5,"x",null],"b":{"c":true}}`, `{"a":[1,2.5,"x",null],"b":{"c":true}}`},
		{colVariant, `"just text"`, `"just text"`},
		{colVariant, `12345678901234567890123456789012345678`, `12345678901234567890123456789012345678`},
		{colVariant, `[1.5e10,123456789012345e3,-1E-300]`, `[1.5e10,123456789012345e3,-1E-300]`},
		{colVariant, `0.` + strings.Repeat("1", 37), `0.` + strings.Repeat("1", 37)},
	}
	for _, c := range cases {
		got, null, err := encodeValue(c.typ, json.RawMessage(c.in))
		require.NoError(t, err, "%s %s", c.typ, c.in)
		assert.False(t, null)
		assert.Equal(t, c.want, got, "%s %s", c.typ, c.in)
	}

	for _, typ := range []colType{colBoolean, colInteger, colNumber, colVarchar, colDate, colTime, colTimestampTZ, colTimestampNTZ, colVariant} {
		for _, in := range []string{``, `null`, ` null `} {
			_, null, err := encodeValue(typ, json.RawMessage(in))
			require.NoError(t, err)
			assert.True(t, null, "%s %q", typ, in)
		}
	}
}

func TestEncodeValueFailsRatherThanChangeTheValue(t *testing.T) {
	cases := []struct {
		typ  colType
		in   string
		want string
	}{
		{colBoolean, `1`, "not a boolean"},
		{colBoolean, `"yes"`, "not a boolean"},
		{colInteger, `1.5`, "not an integer"},
		{colInteger, `"abc"`, "not a number"},
		{colInteger, `"NaN"`, "not a number"},
		{colInteger, `123456789012345678901234567890123456789`, "more than 38 digits"},
		{colInteger, `1e400`, "more than 38 digits"},
		{colInteger, `true`, "not a number"},
		{colNumber, `9007199254740993`, "has more precision than FLOAT, which cannot hold it exactly (nearest is 9.007199254740992e+15)"},
		{colNumber, `0.1234567890123456789`, "has more precision than FLOAT"},
		{colNumber, `1e400`, "outside the FLOAT range"},
		{colNumber, `1e99999999999`, "outside the FLOAT range"},
		{colNumber, `1e-99999999999`, "outside the FLOAT range"},
		{colNumber, `"NaN"`, "not a number"},
		{colNumber, `"Infinity"`, "not a number"},
		{colNumber, `true`, "not a number"},
		{colDate, `"2024-02-30"`, "not a date"},
		{colDate, `"2024-01-01T00:00:00Z"`, "not a date"},
		{colTimestampTZ, `"yesterday"`, "not a timestamp"},
		{colTimestampTZ, `"2024-13-01T00:00:00Z"`, "not a timestamp"},
		{colTimestampTZ, `"2024-01-01T00:00:00.1234567891Z"`, "not a timestamp"},
		{colTimestampTZ, `1700000000`, "not a timestamp"},
		{colTimestampNTZ, `"2024-01-01T00:00:00Z"`, "has a time zone"},
		{colTimestampNTZ, `"2024-01-01T00:00:00+02:00"`, "has a time zone"},
		{colTime, `"10:00:00Z"`, "has a time zone"},
		{colTime, `"25:00:00"`, "not a time"},
		{colVarchar, "\"bad \xff byte\"", "not valid UTF-8"},
		{colVarchar, `"lone \ud800 surrogate"`, "not valid UTF-8"},
		{colVariant, `{"x":"\udc00"}`, "not valid UTF-8"},
		{colVariant, `{"x":123456789012345678901234567890123456789}`, "more than 38 significant digits"},
		{colVariant, `[0.12345678901234567890123456789012345678901]`, "more than 38 significant digits"},
		{colVariant, `{"x":1.2345678901234567e5}`, "number 1.2345678901234567e5 has an exponent and more than 15 significant digits, which a VARIANT stores as a double"},
		{colVariant, `[0.` + strings.Repeat("0", 37) + `1]`, "has more than 37 digits after the decimal point, which a VARIANT stores as a double"},
	}
	for _, c := range cases {
		_, _, err := encodeValue(c.typ, json.RawMessage(c.in))
		require.Error(t, err, "%s %s", c.typ, c.in)
		assert.Contains(t, err.Error(), c.want, "%s %s", c.typ, c.in)
	}

	big := `"` + strings.Repeat("x", maxValueBytes+1) + `"`
	_, _, err := encodeValue(colVarchar, json.RawMessage(big))
	assert.ErrorContains(t, err, "longer than 16 MiB")
}

func TestCastSQL(t *testing.T) {
	assert.Equal(t, `TO_NUMBER("C", 38, 0)`, castSQL(colInteger, `"C"`))
	assert.Equal(t, `TO_DOUBLE("C")`, castSQL(colNumber, `"C"`))
	assert.Equal(t, colType("FLOAT"), colNumber)
	assert.Equal(t, `TO_BOOLEAN("C")`, castSQL(colBoolean, `"C"`))
	assert.Equal(t, `TO_DATE("C", 'YYYY-MM-DD')`, castSQL(colDate, `"C"`))
	assert.Equal(t, `TO_TIME("C", 'HH24:MI:SS.FF9')`, castSQL(colTime, `"C"`))
	assert.Equal(t, `TO_TIMESTAMP_NTZ("C", 'YYYY-MM-DD HH24:MI:SS.FF9')`, castSQL(colTimestampNTZ, `"C"`))
	assert.Equal(t, `TO_TIMESTAMP_TZ("C", 'YYYY-MM-DD HH24:MI:SS.FF9 TZH:TZM')`, castSQL(colTimestampTZ, `"C"`))
	assert.Equal(t, `PARSE_JSON("C")`, castSQL(colVariant, `"C"`))
	assert.Equal(t, `"C"`, castSQL(colVarchar, `"C"`))
}

func TestInformationSchemaType(t *testing.T) {
	assert.Equal(t, colInteger, infoSchemaType("NUMBER", 38, 0))
	assert.Equal(t, colNumber, infoSchemaType("FLOAT", 0, 0))
	assert.Equal(t, colType("NUMBER(38,9)"), infoSchemaType("NUMBER", 38, 9))
	assert.Equal(t, colType("NUMBER(10,2)"), infoSchemaType("NUMBER", 10, 2))
	assert.Equal(t, colVarchar, infoSchemaType("TEXT", 0, 0))
	for _, ty := range []colType{colBoolean, colDate, colTime, colTimestampTZ, colTimestampNTZ, colVariant} {
		assert.Equal(t, ty, infoSchemaType(string(ty), 0, 0))
	}
}
