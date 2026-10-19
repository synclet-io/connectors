package main

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
)

// TestColumnTypesOfFixtureCatalog maps every JSON-schema type of the fixture
// catalog to its documented Delta type.
func TestColumnTypesOfFixtureCatalog(t *testing.T) {
	raw, err := os.ReadFile("testdata/types_catalog.json")
	require.NoError(t, err)
	var catalog synclet.ConfiguredCatalog
	require.NoError(t, json.Unmarshal(raw, &catalog))
	props, err := schemaProperties(catalog.Streams[0].Stream.JSONSchema)
	require.NoError(t, err)

	want := map[string]colType{
		"bool":          colBoolean,
		"bool_nullable": colBoolean,
		"int":           colBigint,
		"int_number":    colBigint,
		"int_string":    colBigint,
		"num":           colDouble,
		"big_int":       colBigDecimal,
		"str":           colString,
		"date":          colDate,
		"ts":            colTimestamp,
		"ts_tz":         colTimestamp,
		"ts_ntz":        colTimestampNTZ,
		"time":          colString,
		"time_tz":       colString,
		"time_ntz":      colString,
		"obj":           colVariant,
		"arr":           colVariant,
		"union":         colVariant,
		"one_of":        colVariant,
		"untyped":       colVariant,
		"ref_string":    colString,
		"ref_binary":    colString,
		"ref_bool":      colBoolean,
		"ref_int":       colBigint,
		"ref_num":       colDouble,
		"ref_date":      colDate,
		"ref_ts_tz":     colTimestamp,
		"ref_ts_ntz":    colTimestampNTZ,
		"ref_time_tz":   colString,
		"ref_time_ntz":  colString,
		"ref_unknown":   colVariant,
	}
	require.Len(t, props, len(want))
	for name, prop := range props {
		m, _ := prop.(map[string]any)
		assert.Equal(t, want[name], columnType(m), name)
	}
}

func TestEncodeValue(t *testing.T) {
	tests := []struct {
		typ  colType
		in   string
		want string
	}{
		{colBoolean, `true`, "true"},
		{colBoolean, `false`, "false"},
		{colBoolean, `"TRUE"`, "true"},
		{colBigint, `9223372036854775807`, "9223372036854775807"},
		{colBigint, `"-42"`, "-42"},
		{colDouble, `0.30000000000000004`, "0.30000000000000004"},
		{colDouble, `37.77492912345`, "37.77492912345"},
		{colDouble, `1e-10`, "1e-10"},
		{colDouble, `0.000000000000000001`, "1e-18"},
		{colDouble, `1e29`, "1e+29"},
		{colDouble, `"-1.5"`, "-1.5"},
		{colDouble, `1.10000000000000`, "1.1"},
		{colDouble, `9007199254740992`, "9.007199254740992e+15"},
		{colBigDecimal, `"123456789012345678901234567890"`, "123456789012345678901234567890"},
		{colString, `"héllo\n\"x\""`, "héllo\n\"x\""},
		{colString, `12.50`, "12.50"},
		{colDate, `"2024-02-29"`, "2024-02-29"},
		{colTimestamp, `"2024-01-02T03:04:05Z"`, "2024-01-02T03:04:05.000000Z"},
		{colTimestamp, `"2024-01-02T03:04:05.123456+02:00"`, "2024-01-02T01:04:05.123456Z"},
		{colTimestamp, `"2024-01-02 03:04:05.120000000-0130"`, "2024-01-02T04:34:05.120000Z"},
		{colTimestamp, `"2024-01-02T03:04:05"`, "2024-01-02T03:04:05.000000Z"},
		{colTimestamp, `"2024-01-02"`, "2024-01-02T00:00:00.000000Z"},
		{colTimestampNTZ, `"2024-01-02T03:04:05.5"`, "2024-01-02T03:04:05.500000"},
		{colTimestampNTZ, `"2024-01-02 03:04"`, "2024-01-02T03:04:00.000000"},
		{colVariant, `{"a": [1, 2.50, "x"], "b": null}`, `{"a": [1, 2.50, "x"], "b": null}`},
		{colVariant, `"plain"`, `"plain"`},
	}
	for _, tt := range tests {
		got, null, err := encodeValue(tt.typ, json.RawMessage(tt.in))
		if assert.NoError(t, err, "%s %s", tt.typ, tt.in) {
			assert.False(t, null)
			assert.Equal(t, tt.want, got, "%s %s", tt.typ, tt.in)
		}
	}

	for _, typ := range []colType{colBoolean, colBigint, colDouble, colString, colDate, colTimestamp, colVariant} {
		_, null, err := encodeValue(typ, json.RawMessage(`null`))
		require.NoError(t, err)
		assert.True(t, null, typ)
	}
}

// TestEncodeValueRejects keeps every value a column cannot hold exactly from
// being written: rounded, truncated or reinterpreted.
func TestEncodeValueRejects(t *testing.T) {
	tests := []struct {
		typ colType
		in  string
		msg string
	}{
		{colBoolean, `1`, "not a boolean"},
		{colBoolean, `"yes"`, "not a boolean"},
		{colBigint, `9223372036854775808`, "outside the BIGINT range"},
		{colBigint, `1.5`, "not an integer"},
		{colBigint, `"abc"`, "not an integer"},
		{colDouble, `9007199254740993`, "more precision than DOUBLE"},
		{colDouble, `0.1000000000000000000001`, "more precision than DOUBLE"},
		{colDouble, `1e400`, "outside the DOUBLE range"},
		{colDouble, `"1/3"`, "not a number"},
		{colDouble, `"NaN"`, "not a number"},
		{colBigDecimal, `1.5`, "not an integer"},
		{colBigDecimal, `123456789012345678901234567890123456789`, "more than 38 digits"},
		{colString, `"\ud800"`, "not valid UTF-8"},
		{colDate, `"2024-02-30"`, "not a date"},
		{colDate, `"0000-01-01"`, "year 0000"},
		{colTimestamp, `"0000-12-31T00:00:00Z"`, "year 0000"},
		{colTimestampNTZ, `"0000-01-01T00:00:00"`, "year 0000"},
		{colTimestamp, `"0001-01-01T00:30:00+01:00"`, "year 0000"},
		{colDate, `"2024-01-02T00:00:00Z"`, "not a date"},
		{colTimestamp, `"2024-01-02T03:04:05.1234567Z"`, "finer than microseconds"},
		{colTimestamp, `"yesterday"`, "not a timestamp"},
		{colTimestamp, `"2024-13-01T00:00:00Z"`, "not a timestamp"},
		{colTimestamp, `1704164645`, "not a string"},
		{colTimestampNTZ, `"2024-01-02T03:04:05Z"`, "carries a time zone"},
		{colTimestampNTZ, `"2024-01-02T03:04:05+01:00"`, "carries a time zone"},
		{colVariant, `"\udc00"`, "not valid UTF-8"},
	}
	for _, tt := range tests {
		_, _, err := encodeValue(tt.typ, json.RawMessage(tt.in))
		if assert.Error(t, err, "%s %s", tt.typ, tt.in) {
			assert.Contains(t, err.Error(), tt.msg, "%s %s", tt.typ, tt.in)
		}
	}
}
