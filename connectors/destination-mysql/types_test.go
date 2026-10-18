package main

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
)

func TestTypesColumn(t *testing.T) {
	cases := []struct {
		schema string
		want   kind
		sql    string
	}{
		{`{"type":"boolean"}`, kindBoolean, "BOOLEAN"},
		{`{"type":["null","boolean"]}`, kindBoolean, "BOOLEAN"},
		{`{"type":"integer"}`, kindInteger, "BIGINT"},
		{`{"type":"number","airbyte_type":"integer"}`, kindInteger, "BIGINT"},
		{`{"type":"number"}`, kindNumber, "DECIMAL(65,30)"},
		{`{"type":"string","airbyte_type":"big_integer"}`, kindBigInteger, "DECIMAL(65,0)"},
		{`{"type":"number","airbyte_type":"big_integer"}`, kindBigInteger, "DECIMAL(65,0)"},
		{`{"type":"string"}`, kindText, "LONGTEXT"},
		{`{"type":"string","format":"date"}`, kindDate, "DATE"},
		{`{"type":"string","format":"date-time"}`, kindTimestampTZ, "DATETIME(6)"},
		{`{"type":"string","format":"date-time","airbyte_type":"timestamp_with_timezone"}`, kindTimestampTZ, "DATETIME(6)"},
		{`{"type":"string","format":"date-time","airbyte_type":"timestamp_without_timezone"}`, kindTimestamp, "DATETIME(6)"},
		{`{"type":"string","format":"time"}`, kindTime, "TIME(6)"},
		{`{"type":"string","format":"time","airbyte_type":"time_without_timezone"}`, kindTime, "TIME(6)"},
		{`{"type":"string","format":"time","airbyte_type":"time_with_timezone"}`, kindTimeTZ, "LONGTEXT"},
		{`{"type":"object","properties":{"a":{"type":"string"}}}`, kindJSON, "JSON"},
		{`{"type":"array","items":{"type":"integer"}}`, kindJSON, "JSON"},
		{`{"type":["string","integer"]}`, kindJSON, "JSON"},
		{`{"anyOf":[{"type":"string"},{"type":"integer"}]}`, kindJSON, "JSON"},
		{`{}`, kindJSON, "JSON"},
		{`{"$ref":"WellKnownTypes.json#/definitions/Integer"}`, kindInteger, "BIGINT"},
		{`{"$ref":"WellKnownTypes.json#/definitions/Number"}`, kindNumber, "DECIMAL(65,30)"},
		{`{"$ref":"WellKnownTypes.json#/definitions/TimestampWithoutTimezone"}`, kindTimestamp, "DATETIME(6)"},
	}
	for _, c := range cases {
		var prop map[string]any
		require.NoError(t, json.Unmarshal([]byte(c.schema), &prop))
		got := columnKind(prop)
		assert.Equal(t, c.want, got, c.schema)
		assert.Equal(t, c.sql, got.sqlType(), c.schema)
	}
	assert.Equal(t, "tinyint(1)", infoType("BOOLEAN"))
	assert.Equal(t, "decimal(65,30)", infoType("DECIMAL(65,30)"))
}

func TestTypesEncode(t *testing.T) {
	ok := []struct {
		kind kind
		raw  string
		want any
	}{
		{kindNumber, `12345678901234567890.123456789012345678`, `12345678901234567890.123456789012345678`},
		{kindNumber, `1e3`, `1000`},
		{kindNumber, `1E-30`, `0.000000000000000000000000000001`},
		{kindNumber, `-1.5e-2`, `-0.015`},
		{kindNumber, `"12.50"`, `12.5`},
		{kindNumber, `0.` + "123456789012345678901234567890", `0.12345678901234567890123456789`},
		{kindNumber, `0.` + "1234567890123456789012345678900000", `0.12345678901234567890123456789`},
		{kindNumber, "12345678901234567890123456789012345", "12345678901234567890123456789012345"},
		{kindNumber, `0e999999`, `0`},
		{kindNumber, `-0.0`, `0`},
		{kindBigInteger, `"123456789012345678901234567890"`, `123456789012345678901234567890`},
		{kindBigInteger, `1.0e2`, `100`},
		{kindInteger, `9223372036854775807`, int64(9223372036854775807)},
		{kindInteger, `"-42"`, int64(-42)},
		{kindBoolean, `true`, int64(1)},
		{kindBoolean, `false`, int64(0)},
		{kindBoolean, `"TRUE"`, int64(1)},
		{kindText, `"tab\there"`, "tab\there"},
		{kindText, `42.10`, `42.10`},
		{kindText, `"a\u0000b"`, "a\x00b"},
		{kindText, `"pair 😀 ok"`, "pair \U0001F600 ok"},
		{kindText, `"\\ud800 as text"`, `\ud800 as text`},
		{kindDate, `"2024-01-31"`, `2024-01-31`},
		{kindTimestampTZ, `"2024-02-29T13:14:15.123456+02:00"`, `2024-02-29 11:14:15.123456`},
		{kindTimestampTZ, `"2024-01-01T00:30:00-0130"`, `2024-01-01 02:00:00.000000`},
		{kindTimestampTZ, `"2024-01-01T10:00:00Z"`, `2024-01-01 10:00:00.000000`},
		{kindTimestampTZ, `"2024-01-01 10:00:00"`, `2024-01-01 10:00:00.000000`},
		{kindTimestampTZ, `"2024-01-01"`, `2024-01-01 00:00:00.000000`},
		{kindTimestampTZ, `"2024-01-01T10:00:00.1234560"`, `2024-01-01 10:00:00.123456`},
		{kindTimestamp, `"2024-01-01T10:00:00.5"`, `2024-01-01 10:00:00.500000`},
		{kindTime, `"10:00:00.123"`, `10:00:00.123`},
		{kindTime, `"12:00:00.123456"`, `12:00:00.123456`},
		{kindTimeTZ, `"10:00:00+05:00"`, `10:00:00+05:00`},
		{kindJSON, `{"a": [1, 2.50]}`, `{"a": [1, 2.50]}`},
		{kindJSON, `{"a":0.1}`, `{"a":0.1}`},
		{kindJSON, `[18446744073709551615, -9223372036854775808, 1e300]`, `[18446744073709551615, -9223372036854775808, 1e300]`},
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

	bad := []struct {
		kind kind
		raw  string
		want string
	}{
		{kindInteger, `9223372036854775808`, "outside the BIGINT range"},
		{kindInteger, `1.5`, "not an integer"},
		{kindInteger, `1e3`, "not an integer"},
		{kindNumber, `"abc"`, "not a number"},
		{kindNumber, `true`, "not a number"},
		{kindNumber, `0.` + "1234567890123456789012345678901", "more than 30 fraction digits"},
		{kindNumber, "123456789012345678901234567890123456", "more than 35 integer digits"},
		{kindNumber, `1e35`, "more than 35 integer digits"},
		{kindBigInteger, `1.5`, "not an integer"},
		{kindBigInteger, `1e65`, "more than 65 integer digits"},
		{kindBoolean, `2`, "not a boolean"},
		{kindBoolean, `"yes"`, "not a boolean"},
		{kindDate, `20240131`, "not a string"},
		{kindDate, `"24-01-31"`, "not a date"},
		{kindTimestamp, `"2024-01-01T10:00:00+05:00"`, "time zone"},
		{kindTimestamp, `"2024-01-01T10:00:00Z"`, "time zone"},
		{kindTimestamp, `"2024-01-01 10:00:00 PST"`, "time zone"},
		{kindTimestampTZ, `"2024-02-30T10:00:00Z"`, "not a valid date-time"},
		{kindTimestampTZ, `"2024-01-01T10:00:00.1234567Z"`, "more than 6 fractional-second digits"},
		{kindTimestampTZ, `"yesterday"`, "not a date-time"},
		{kindTime, `"10:00:00+05:00"`, "time zone"},
		{kindTime, `"10:00:00Z"`, "time zone"},
		{kindTime, `"12:00:00.1234567"`, "more than 6 fractional-second digits"},
		{kindTime, `"10 o'clock"`, "time zone"},
		{kindJSON, `{"a":0.912345678901234567890}`, "would be stored as the double 0.9123456789012345"},
		{kindJSON, `[99999999999999999999]`, "would be stored as the double"},
		{kindJSON, `[1e400]`, "outside the range of a double"},
		{kindJSON, `{"a":"\ud800"}`, "not valid UTF-8"},
		{kindText, `"\ud800"`, "not valid UTF-8"},
		{kindText, `"high \ud800A then letter"`, "not valid UTF-8"},
		{kindText, "\"bad \xff byte\"", "not valid UTF-8"},
		{kindDate, `"\udc00"`, "not valid UTF-8"},
	}
	for _, c := range bad {
		_, err := encodeValue(c.kind, json.RawMessage(c.raw))
		require.Error(t, err, c.raw)
		assert.Contains(t, err.Error(), c.want, c.raw)
	}
}

func TestTypesKeyText(t *testing.T) {
	key := column{name: "k", kind: kindText, sqlType: keyTextType, key: true}
	require.NoError(t, checkKeyText(key, string(make([]rune, 255))))
	long := make([]rune, 256)
	for i := range long {
		long[i] = 'é'
	}
	require.ErrorContains(t, checkKeyText(key, string(long)), "longer than 255 characters")
	require.NoError(t, checkKeyText(key, "a "))
}

func TestPlanKeyWidth(t *testing.T) {
	assert.Equal(t, 16, decimalBytes(numberIntDigits))
	assert.Equal(t, 14, decimalBytes(numberFracDigits))
	assert.Equal(t, 29, decimalBytes(bigIntDigits))

	props := `{"type":"object","properties":{"a":{"type":"string"},"b":{"type":"string"},"c":{"type":"string"},"d":{"type":"string"},` +
		`"x":{"type":"integer"},"y":{"type":"integer"},"n":{"type":"number"},"ts":{"type":"string","format":"date-time"}}}`
	plan := func(keys ...string) error {
		pk := make([][]string, len(keys))
		for i, k := range keys {
			pk[i] = []string{k}
		}
		_, err := planStream(0, stream("s", props, synclet.DestinationSyncModeAppendDedup, pk, nil))
		return err
	}
	require.NoError(t, plan("a", "b", "c", "x"), "3,060 + 8 bytes fit")
	require.NoError(t, plan("a", "b", "c", "ts"), "3,060 + 8 bytes fit")
	for _, keys := range [][]string{{"a", "b", "c", "x", "y"}, {"a", "b", "c", "n"}, {"a", "b", "c", "d"}} {
		err := plan(keys...)
		require.Error(t, err, "%v", keys)
		assert.Contains(t, err.Error(), "more than the 3072 bytes of a MySQL key", "%v", keys)
	}
}
