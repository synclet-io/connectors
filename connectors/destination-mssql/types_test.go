package main

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTypesColumn(t *testing.T) {
	cases := []struct {
		schema string
		want   kind
		sql    string
	}{
		{`{"type":"boolean"}`, kindBoolean, "BIT"},
		{`{"type":["null","boolean"]}`, kindBoolean, "BIT"},
		{`{"type":"integer"}`, kindInteger, "BIGINT"},
		{`{"type":"number","airbyte_type":"integer"}`, kindInteger, "BIGINT"},
		{`{"type":"string","airbyte_type":"integer"}`, kindInteger, "BIGINT"},
		{`{"type":"number"}`, kindNumber, "FLOAT(53)"},
		{`{"type":"string","airbyte_type":"big_integer"}`, kindBigInteger, "DECIMAL(38,0)"},
		{`{"type":"number","airbyte_type":"big_integer"}`, kindBigInteger, "DECIMAL(38,0)"},
		{`{"type":"string"}`, kindText, "NVARCHAR(MAX)"},
		{`{"type":"string","format":"date"}`, kindDate, "DATE"},
		{`{"type":"string","format":"date-time"}`, kindTimestampTZ, "DATETIMEOFFSET(7)"},
		{`{"type":"string","format":"date-time","airbyte_type":"timestamp_with_timezone"}`, kindTimestampTZ, "DATETIMEOFFSET(7)"},
		{`{"type":"string","format":"date-time","airbyte_type":"timestamp_without_timezone"}`, kindTimestamp, "DATETIME2(7)"},
		{`{"type":"string","format":"time"}`, kindTime, "TIME(7)"},
		{`{"type":"string","format":"time","airbyte_type":"time_without_timezone"}`, kindTime, "TIME(7)"},
		{`{"type":"string","format":"time","airbyte_type":"time_with_timezone"}`, kindTimeTZ, "NVARCHAR(MAX)"},
		{`{"type":"object","properties":{"a":{"type":"string"}}}`, kindJSON, "NVARCHAR(MAX)"},
		{`{"type":"array","items":{"type":"integer"}}`, kindJSON, "NVARCHAR(MAX)"},
		{`{"type":["string","integer"]}`, kindJSON, "NVARCHAR(MAX)"},
		{`{"anyOf":[{"type":"string"},{"type":"integer"}]}`, kindJSON, "NVARCHAR(MAX)"},
		{`{}`, kindJSON, "NVARCHAR(MAX)"},
		{`{"$ref":"WellKnownTypes.json#/definitions/Integer"}`, kindInteger, "BIGINT"},
		{`{"$ref":"WellKnownTypes.json#/definitions/Number"}`, kindNumber, "FLOAT(53)"},
		{`{"$ref":"WellKnownTypes.json#/definitions/TimestampWithoutTimezone"}`, kindTimestamp, "DATETIME2(7)"},
		{`{"$ref":"WellKnownTypes.json#/definitions/Unknown"}`, kindJSON, "NVARCHAR(MAX)"},
	}
	for _, c := range cases {
		var prop map[string]any
		require.NoError(t, json.Unmarshal([]byte(c.schema), &prop))
		got := columnKind(prop)
		assert.Equal(t, c.want, got, c.schema)
		assert.Equal(t, c.sql, got.sqlType(), c.schema)
	}
}

func TestTypesName(t *testing.T) {
	assert.Equal(t, "nvarchar(max)", typeName("nvarchar", -1, 0, 0))
	assert.Equal(t, "nvarchar(255)", typeName("nvarchar", 510, 0, 0))
	assert.Equal(t, "decimal(38,0)", typeName("decimal", 17, 38, 0))
	assert.Equal(t, "float(53)", typeName("float", 8, 53, 0))
	assert.Equal(t, "datetimeoffset(7)", typeName("datetimeoffset", 10, 34, 7))
	assert.Equal(t, "bigint", typeName("bigint", 8, 19, 0))
}

func TestTypesEncode(t *testing.T) {
	utc := func(s string) time.Time {
		v, err := time.Parse(time.RFC3339Nano, s)
		require.NoError(t, err)
		return v.UTC()
	}
	ok := []struct {
		kind kind
		raw  string
		want any
	}{
		{kindNumber, `12345.678901`, 12345.678901},
		{kindNumber, `1e3`, 1000.0},
		{kindNumber, `1E-30`, 1e-30},
		{kindNumber, `-1.5e-2`, -0.015},
		{kindNumber, `"12.50"`, 12.5},
		{kindNumber, `0.1`, 0.1},
		{kindNumber, `-0.0`, 0.0},
		{kindBigInteger, `"123456789012345678901234567890"`, `123456789012345678901234567890`},
		{kindBigInteger, `1.0e2`, `100`},
		{kindBigInteger, `-00012`, `-12`},
		{kindBigInteger, `"99999999999999999999999999999999999999"`, `99999999999999999999999999999999999999`},
		{kindInteger, `9223372036854775807`, int64(9223372036854775807)},
		{kindInteger, `-9223372036854775808`, int64(-9223372036854775808)},
		{kindInteger, `"-42"`, int64(-42)},
		{kindInteger, `1.0`, int64(1)},
		{kindInteger, `1e3`, int64(1000)},
		{kindInteger, `0e-5`, int64(0)},
		{kindBoolean, `true`, true},
		{kindBoolean, `false`, false},
		{kindBoolean, `"TRUE"`, true},
		{kindBoolean, `0`, false},
		{kindText, `"tab\there"`, "tab\there"},
		{kindText, `42.10`, `42.10`},
		{kindText, `"a\u0000b"`, "a\x00b"},
		{kindText, `"pair 😀 ok"`, "pair \U0001F600 ok"},
		{kindText, `"\\ud800 as text"`, `\ud800 as text`},
		{kindDate, `"2024-02-29"`, utc("2024-02-29T00:00:00Z")},
		{kindDate, `"0001-01-01"`, utc("0001-01-01T00:00:00Z")},
		{kindTimestampTZ, `"2024-02-29T13:14:15.123456+02:00"`, utc("2024-02-29T11:14:15.123456Z")},
		{kindTimestampTZ, `"2024-01-01T00:30:00-0130"`, utc("2024-01-01T02:00:00Z")},
		{kindTimestampTZ, `"2024-01-01T10:00:00Z"`, utc("2024-01-01T10:00:00Z")},
		{kindTimestampTZ, `"2024-01-01 10:00:00"`, utc("2024-01-01T10:00:00Z")},
		{kindTimestampTZ, `"2024-01-01"`, utc("2024-01-01T00:00:00Z")},
		{kindTimestampTZ, `"2024-01-01T10:00:00.12345670"`, utc("2024-01-01T10:00:00.1234567Z")},
		{kindTimestamp, `"2024-01-01T10:00:00.5"`, utc("2024-01-01T10:00:00.5Z")},
		{kindTimestamp, `"9999-12-31T23:59:59.9999999"`, utc("9999-12-31T23:59:59.9999999Z")},
		{kindTime, `"10:00:00.123"`, time.Date(1, 1, 1, 10, 0, 0, 123_000_000, time.UTC)},
		{kindTime, `"23:59"`, time.Date(1, 1, 1, 23, 59, 0, 0, time.UTC)},
		{kindTimeTZ, `"10:00:00+05:00"`, `10:00:00+05:00`},
		{kindJSON, `{"a": [1, 2.50]}`, `{"a": [1, 2.50]}`},
		{kindJSON, `[18446744073709551615, 0.912345678901234567890, 1e300]`, `[18446744073709551615, 0.912345678901234567890, 1e300]`},
		{kindJSON, `"plain"`, `"plain"`},
	}
	for _, c := range ok {
		got, err := encodeValue(c.kind, json.RawMessage(c.raw))
		require.NoError(t, err, c.raw)
		assert.Equal(t, c.want, got, c.raw)
	}

	got, err := encodeValue(kindNumber, json.RawMessage(`null`))
	require.NoError(t, err)
	assert.Nil(t, got)
	got, err = encodeValue(kindText, nil)
	require.NoError(t, err)
	assert.Nil(t, got)

	bad := []struct {
		kind kind
		raw  string
		want string
	}{
		{kindInteger, `9223372036854775808`, "outside the BIGINT range"},
		{kindInteger, `1e19`, "outside the BIGINT range"},
		{kindInteger, `1.5`, "not an integer"},
		{kindInteger, `"x"`, "not an integer"},
		{kindNumber, `"abc"`, "not a number"},
		{kindNumber, `true`, "not a number"},
		{kindNumber, `"NaN"`, "not a number"},
		{kindNumber, `"Infinity"`, "not a number"},
		{kindNumber, `1e309`, "outside the FLOAT range"},
		{kindNumber, `0.912345678901234567890`, "has more precision than FLOAT, which cannot hold it exactly (nearest is 0.9123456789012345)"},
		{kindNumber, `9007199254740993`, "has more precision than FLOAT"},
		{kindBigInteger, `1.5`, "not an integer"},
		{kindBigInteger, `"123456789012345678901234567890123456789"`, "more than 38 digits, which DECIMAL(38,0) cannot hold"},
		{kindBigInteger, `1e38`, "more than 38 digits"},
		{kindBoolean, `"yes"`, "not a boolean"},
		{kindDate, `"2024-02-30"`, "not a valid date"},
		{kindDate, `"0000-01-01"`, "not a valid date"},
		{kindDate, `"2024/01/01"`, "not a date"},
		{kindDate, `20240101`, "not a string"},
		{kindTimestampTZ, `"2024-01-01T25:00:00Z"`, "not a valid date-time"},
		{kindTimestampTZ, `"2024-01-01T10:00:00+24:00"`, "invalid time zone offset"},
		{kindTimestampTZ, `"0001-01-01T00:30:00+01:00"`, "outside 0001-01-01 to 9999-12-31 in UTC"},
		{kindTimestampTZ, `"2024-01-01T10:00:00.12345678Z"`, "more than 7 fractional-second digits"},
		{kindTimestampTZ, `"yesterday"`, "not a date-time"},
		{kindTimestamp, `"2024-01-01T10:00:00Z"`, "carries a time zone"},
		{kindTimestamp, `"2024-01-01T10:00:00 UTC"`, "carries a time zone"},
		{kindTime, `"10:00:00Z"`, "carries a time zone"},
		{kindTime, `"24:00:00"`, "not a valid time of day"},
		{kindTime, `"10"`, "not a time"},
		{kindText, `"bad \ud800"`, "not valid UTF-8"},
		{kindJSON, `{"a": "\udc00"}`, "not valid UTF-8"},
	}
	for _, c := range bad {
		_, err := encodeValue(c.kind, json.RawMessage(c.raw))
		require.Error(t, err, c.raw)
		assert.Contains(t, err.Error(), c.want, c.raw)
	}
	_, err = encodeValue(kindText, json.RawMessage("\"\xff\""))
	require.Error(t, err)
}
