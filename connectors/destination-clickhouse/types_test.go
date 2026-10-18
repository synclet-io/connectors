package main

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestColumnType(t *testing.T) {
	cases := []struct {
		schema string
		want   string
	}{
		{`{"type":"boolean"}`, "Bool"},
		{`{"type":["null","boolean"]}`, "Bool"},
		{`{"type":"integer"}`, "Int64"},
		{`{"type":"number","airbyte_type":"integer"}`, "Int64"},
		{`{"type":"string","airbyte_type":"integer"}`, "Int64"},
		{`{"type":"number"}`, "Decimal(76, 38)"},
		{`{"type":"string","airbyte_type":"big_integer"}`, "Decimal(76, 0)"},
		{`{"type":"number","airbyte_type":"big_integer"}`, "Decimal(76, 0)"},
		{`{"type":"string"}`, "String"},
		{`{"type":"string","format":"date"}`, "Date32"},
		{`{"type":"string","format":"date-time"}`, "DateTime64(6, 'UTC')"},
		{`{"type":"string","format":"date-time","airbyte_type":"timestamp_with_timezone"}`, "DateTime64(6, 'UTC')"},
		{`{"type":"string","format":"date-time","airbyte_type":"timestamp_without_timezone"}`, "DateTime64(6, 'UTC')"},
		{`{"type":"string","format":"time"}`, "String"},
		{`{"type":"string","format":"time","airbyte_type":"time_with_timezone"}`, "String"},
		{`{"type":"object","properties":{"a":{"type":"string"}}}`, "String"},
		{`{"type":"array","items":{"type":"integer"}}`, "String"},
		{`{"type":["string","integer"]}`, "String"},
		{`{"anyOf":[{"type":"string"},{"type":"integer"}]}`, "String"},
		{`{}`, "String"},
		{`{"$ref":"WellKnownTypes.json#/definitions/Integer"}`, "Int64"},
		{`{"$ref":"WellKnownTypes.json#/definitions/Number"}`, "Decimal(76, 38)"},
		{`{"$ref":"WellKnownTypes.json#/definitions/TimestampWithoutTimezone"}`, "DateTime64(6, 'UTC')"},
		{`{"$ref":"WellKnownTypes.json#/definitions/Date"}`, "Date32"},
	}
	for _, c := range cases {
		var prop map[string]any
		require.NoError(t, json.Unmarshal([]byte(c.schema), &prop))
		assert.Equal(t, c.want, columnType(prop).chType(), c.schema)
	}

	t.Run("JSON kinds", func(t *testing.T) {
		for _, s := range []string{`{"type":"object"}`, `{"type":"array"}`, `{"type":["string","integer"]}`, `{}`} {
			var prop map[string]any
			require.NoError(t, json.Unmarshal([]byte(s), &prop))
			assert.Equal(t, kindJSON, columnType(prop), s)
		}
	})
	t.Run("nullable wraps the type", func(t *testing.T) {
		assert.Equal(t, "Nullable(Decimal(76, 38))", column{kind: kindNumber, nullable: true}.sqlType())
		assert.Equal(t, "Int64", column{kind: kindInt}.sqlType())
	})
}

func utc(s string) time.Time {
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		panic(err)
	}
	return t.UTC()
}

func TestColumnTypeEncode(t *testing.T) {
	ok := []struct {
		kind kind
		raw  string
		want any
	}{
		{kindNumber, `12345678901234567890.123456789012345678`, "12345678901234567890.123456789012345678"},
		{kindNumber, `1e-30`, "0.000000000000000000000000000001"},
		{kindNumber, `"12.50"`, "12.5"},
		{kindNumber, `99999999999999999999999999999999999999.99999999999999999999999999999999999999`, "99999999999999999999999999999999999999.99999999999999999999999999999999999999"},
		{kindNumber, `1.50000000000000000000000000000000000000000000`, "1.5"},
		{kindNumber, `-0`, "0"},
		{kindBigInt, `"123456789012345678901234567890"`, "123456789012345678901234567890"},
		{kindBigInt, `1e20`, "100000000000000000000"},
		{kindBigInt, `"` + strings.Repeat("9", 76) + `"`, strings.Repeat("9", 76)},
		{kindInt, `9223372036854775807`, int64(9223372036854775807)},
		{kindInt, `"-42"`, int64(-42)},
		{kindBool, `true`, true},
		{kindBool, `false`, false},
		{kindBool, `"true"`, true},
		{kindString, `"tab\there"`, "tab\there"},
		{kindString, `"nul \u0000 kept"`, "nul \x00 kept"},
		{kindString, `42.10`, "42.10"},
		{kindString, `"pair 😀 ok"`, "pair \U0001F600 ok"},
		{kindString, `"\\ud800 as text"`, `\ud800 as text`},
		{kindJSON, `{"a": [1, 2.50], "n": 99999999999999999999.5}`, `{"a": [1, 2.50], "n": 99999999999999999999.5}`},
		{kindJSON, `"plain"`, `"plain"`},
		{kindTime, `"10:00:00+05:00"`, "10:00:00+05:00"},
		{kindDate, `"2024-02-29"`, utc("2024-02-29T00:00:00Z")},
		{kindDate, `"1900-01-01"`, utc("1900-01-01T00:00:00Z")},
		{kindDate, `"2299-12-31"`, utc("2299-12-31T00:00:00Z")},
		{kindTimestampTZ, `"2024-02-29T13:14:15.123456+02:00"`, utc("2024-02-29T11:14:15.123456Z")},
		{kindTimestampTZ, `"2024-01-01 10:00:00Z"`, utc("2024-01-01T10:00:00Z")},
		{kindTimestampTZ, `"2024-01-01T10:00:00-0330"`, utc("2024-01-01T13:30:00Z")},
		{kindTimestampTZ, `"2024-01-01T10:00:00"`, utc("2024-01-01T10:00:00Z")},
		{kindTimestampTZ, `"2024-01-01T10:00"`, utc("2024-01-01T10:00:00Z")},
		{kindTimestampTZ, `"2024-01-01"`, utc("2024-01-01T00:00:00Z")},
		{kindTimestampTZ, `"2024-01-01T10:00:00.123456000Z"`, utc("2024-01-01T10:00:00.123456Z")},
		{kindTimestampNTZ, `"2024-02-29T13:14:15.5"`, utc("2024-02-29T13:14:15.5Z")},
		{kindTimestampTZ, `"1900-01-01T00:00:00Z"`, utc("1900-01-01T00:00:00Z")},
		{kindTimestampTZ, `"2262-04-11T23:47:16.854775Z"`, utc("2262-04-11T23:47:16.854775Z")},
	}
	for _, c := range ok {
		got, err := encodeValue(c.kind, json.RawMessage(c.raw))
		require.NoError(t, err, c.raw)
		if d, isDec := got.(decimal.Decimal); isDec {
			assert.Equal(t, c.want, d.String(), c.raw)
			continue
		}
		assert.Equal(t, c.want, got, c.raw)
	}

	for _, k := range []kind{kindNumber, kindString, kindJSON, kindDate} {
		got, err := encodeValue(k, json.RawMessage(`null`))
		require.NoError(t, err)
		assert.Nil(t, got)
		got, err = encodeValue(k, nil)
		require.NoError(t, err)
		assert.Nil(t, got)
	}

	bad := []struct {
		kind kind
		raw  string
		want string
	}{
		{kindInt, `9223372036854775808`, "outside the Int64 range"},
		{kindInt, `1.5`, "not an integer"},
		{kindNumber, `"abc"`, "not a number"},
		{kindNumber, `true`, "not a number"},
		{kindNumber, `1e-39`, "more than 38 digits after the decimal point"},
		{kindNumber, `0.123456789012345678901234567890123456789`, "more than 38 digits after the decimal point"},
		{kindNumber, `1` + strings.Repeat("0", 38), "more than 38 digits before the decimal point"},
		{kindNumber, `1e400`, "more than 38 digits before the decimal point"},
		{kindBigInt, `"1.5"`, "digits after the decimal point"},
		{kindBigInt, `"1` + strings.Repeat("0", 76) + `"`, "more than 76 digits before the decimal point"},
		{kindBool, `1`, "not a boolean"},
		{kindBool, `"yes"`, "not a boolean"},
		{kindDate, `20240131`, "not a string"},
		{kindDate, `"2024-02-30"`, "not a valid date"},
		{kindDate, `"1899-12-31"`, "outside the Date32 range"},
		{kindDate, `"2300-01-01"`, "outside the Date32 range"},
		{kindDate, `"2024-01-01T00:00:00Z"`, "not a date"},
		{kindTimestampTZ, `"1899-12-31T23:59:59Z"`, "outside the range 1900-01-01 00:00:00 to 2262-04-11 23:47:16.854775 UTC"},
		{kindTimestampTZ, `"2262-04-11T23:47:16.854776Z"`, "outside the range"},
		{kindTimestampTZ, `"2262-04-11T23:00:00-01:00"`, "outside the range"},
		{kindTimestampNTZ, `"2299-12-31T23:59:59"`, "outside the range"},
		{kindTimestampTZ, `"2024-01-01T10:00:00.1234567Z"`, "digits past microseconds"},
		{kindTimestampTZ, `"2024-01-01T25:00:00Z"`, "not a valid timestamp"},
		{kindTimestampTZ, `"yesterday"`, "not an ISO 8601 timestamp"},
		{kindTimestampTZ, `1700000000`, "not a string"},
		{kindTimestampNTZ, `"2024-01-01T10:00:00+05:00"`, "time zone"},
		{kindTimestampNTZ, `"2024-01-01T10:00:00Z"`, "time zone"},
		{kindString, `"high \ud800A then letter"`, "not valid UTF-8"},
		{kindString, `"lone \ud800 surrogate"`, "not valid UTF-8"},
		{kindString, "\"bad \xff byte\"", "not valid UTF-8"},
		{kindJSON, `{"k":"\udc00"}`, "not valid UTF-8"},
		{kindTime, `10`, "not a string"},
	}
	for _, c := range bad {
		_, err := encodeValue(c.kind, json.RawMessage(c.raw))
		require.Error(t, err, c.raw)
		assert.Contains(t, err.Error(), c.want, c.raw)
	}
}
