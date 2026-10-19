package main

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestColumnType(t *testing.T) {
	cases := map[string]colType{
		`{"type":"boolean"}`:                                         colBoolean,
		`{"type":["null","integer"]}`:                                colBigint,
		`{"type":"number","airbyte_type":"integer"}`:                 colBigint,
		`{"type":"number"}`:                                          colDouble,
		`{"type":"string","airbyte_type":"big_integer"}`:             colNumeric,
		`{"type":"string"}`:                                          colVarchar,
		`{"type":"string","format":"date"}`:                          colDate,
		`{"type":"string","format":"date-time"}`:                     colTimestamptz,
		`{"type":"string","airbyte_type":"timestamp_with_timezone"}`: colTimestamptz,
		`{"type":"string","format":"date-time","airbyte_type":"timestamp_without_timezone"}`: colTimestamp,
		`{"type":"string","format":"time"}`:                                                  colTime,
		`{"type":"string","airbyte_type":"time_with_timezone"}`:                              colTimetz,
		`{"type":"object","properties":{"a":{"type":"string"}}}`:                             colSuper,
		`{"type":"array","items":{"type":"integer"}}`:                                        colSuper,
		`{"type":["string","integer"]}`:                                                      colSuper,
		`{"oneOf":[{"type":"string"},{"type":"integer"}]}`:                                   colSuper,
		`{}`: colSuper,
		`{"$ref":"WellKnownTypes.json#/definitions/Integer"}`:                  colBigint,
		`{"$ref":"WellKnownTypes.json#/definitions/Number"}`:                   colDouble,
		`{"$ref":"WellKnownTypes.json#/definitions/TimestampWithoutTimezone"}`: colTimestamp,
	}
	for schema, want := range cases {
		var prop map[string]any
		require.NoError(t, json.Unmarshal([]byte(schema), &prop))
		assert.Equal(t, want, columnType(prop), schema)
	}
}

func TestEncodeValue(t *testing.T) {
	long := strings.Repeat("é", 40000) // 80,000 bytes
	cases := []struct {
		typ       colType
		raw       string
		want      string
		null      bool
		truncated bool
		err       string
	}{
		{typ: colVarchar, raw: `null`, null: true},
		{typ: colSuper, raw: `null`, null: true},
		{typ: colBoolean, raw: `true`, want: "true"},
		{typ: colBoolean, raw: `"FALSE"`, want: "false"},
		{typ: colBoolean, raw: `1`, err: "is not a boolean"},
		{typ: colBigint, raw: `-9223372036854775808`, want: "-9223372036854775808"},
		{typ: colBigint, raw: `"42"`, want: "42"},
		{typ: colBigint, raw: `9223372036854775808`, err: "outside the BIGINT range"},
		{typ: colBigint, raw: `1.5`, err: "is not an integer"},
		{typ: colBigint, raw: `1.0`, want: "1"},
		{typ: colBigint, raw: `1e3`, want: "1000"},
		{typ: colBigint, raw: `-2.50E1`, want: "-25"},
		{typ: colBigint, raw: `"12.000"`, want: "12"},
		{typ: colBigint, raw: `1.25e1`, err: "is not an integer"},
		{typ: colBigint, raw: `1e-3`, err: "is not an integer"},
		{typ: colBigint, raw: `1e19`, err: "outside the BIGINT range"},
		{typ: colBigint, raw: `1e999999999999`, err: "outside the BIGINT range"},
		{typ: colBigint, raw: `0e999999999999`, want: "0"},
		{typ: colBigint, raw: `"1e"`, err: "is not an integer"},
		{typ: colNumeric, raw: `"12345678901234567890123456789012345678"`, want: "12345678901234567890123456789012345678"},
		{typ: colNumeric, raw: `123456789012345678901234567890123456789`, err: "more than 38 digits"},
		{typ: colNumeric, raw: `"1.5"`, err: "is not an integer"},
		{typ: colDouble, raw: `41.25`, want: "41.25"},
		{typ: colDouble, raw: `0.30000000000000004`, want: "0.30000000000000004"},
		{typ: colDouble, raw: `1e-30`, want: "1e-30"},
		{typ: colDouble, raw: `"2.5"`, want: "2.5"},
		{typ: colDouble, raw: `0.912345678901234567890`, err: "cannot hold it exactly"},
		{typ: colDouble, raw: `12345678901234567891`, err: "cannot hold it exactly"},
		{typ: colDouble, raw: `1e400`, err: "outside the DOUBLE PRECISION range"},
		{typ: colDouble, raw: `"NaN"`, err: "is not a number"},
		{typ: colVarchar, raw: `"tab\there \"q\""`, want: "tab\there \"q\""},
		{typ: colVarchar, raw: `12.50`, want: "12.50"},
		{typ: colVarchar, raw: `"a\u0000b"`, err: `\u0000`},
		{typ: colVarchar, raw: `"\ud800"`, err: "not valid UTF-8"},
		{typ: colVarchar, raw: `"` + long + `"`, want: strings.Repeat("é", 32767), truncated: true},
		{typ: colDate, raw: `"2024-02-29"`, want: "2024-02-29"},
		{typ: colDate, raw: `"2023-02-29"`, err: "is not a date"},
		{typ: colTimestamptz, raw: `"2024-02-29T13:14:15.123456+02:00"`, want: "2024-02-29 11:14:15.123456+00"},
		{typ: colTimestamptz, raw: `"2024-02-29 13:14:15Z"`, want: "2024-02-29 13:14:15+00"},
		{typ: colTimestamptz, raw: `"2024-02-29T13:14:15"`, want: "2024-02-29 13:14:15+00"},
		{typ: colTimestamptz, raw: `"2024-02-29T13:14:15.1234567Z"`, err: "more than 6 fractional digits"},
		{typ: colTimestamptz, raw: `"2024-02-29T13:14:15.1234560Z"`, want: "2024-02-29 13:14:15.123456+00"},
		{typ: colTimestamptz, raw: `"yesterday"`, err: "is not a timestamp"},
		{typ: colTimestamp, raw: `"2024-02-29T13:14:15"`, want: "2024-02-29 13:14:15"},
		{typ: colTimestamp, raw: `"2024-02-29T13:14:15+02:00"`, err: "carries a time zone"},
		{typ: colTime, raw: `"13:14:15.5"`, want: "13:14:15.5"},
		{typ: colTime, raw: `"13:14:15Z"`, err: "carries a time zone"},
		{typ: colTimetz, raw: `"13:14:15+02"`, want: "13:14:15+02:00"},
		{typ: colTimetz, raw: `"25:00:00+02"`, err: "is not a time"},
		{typ: colSuper, raw: `{"a": [1, 2.50], "b": {"c": null}}`, want: `{"a":[1,2.50],"b":{"c":null}}`},
		{typ: colSuper, raw: `"x"`, want: `"x"`},
		{typ: colSuper, raw: `7`, want: `7`},
	}
	for _, c := range cases {
		val, null, truncated, err := encodeValue(c.typ, json.RawMessage(c.raw))
		name := string(c.typ) + " " + c.raw
		if len(name) > 80 {
			name = name[:80]
		}
		if c.err != "" {
			require.Error(t, err, name)
			assert.Contains(t, err.Error(), c.err, name)
			continue
		}
		require.NoError(t, err, name)
		assert.Equal(t, c.null, null, name)
		assert.Equal(t, c.truncated, len(truncated) > 0, name)
		assert.Equal(t, c.want, val, name)
	}
}

func TestEncodeTruncatesAtRuneBoundary(t *testing.T) {
	s := strings.Repeat("a", maxVarcharBytes-1) + "€tail"
	raw, err := json.Marshal(s)
	require.NoError(t, err)
	val, _, truncated, err := encodeValue(colVarchar, raw)
	require.NoError(t, err)
	assert.Equal(t, []string{""}, truncated)
	assert.Equal(t, maxVarcharBytes-1, len(val), "the 3-byte rune that crosses the limit is dropped whole")
}

func TestEncodeSuperTruncatesLongStrings(t *testing.T) {
	long := strings.Repeat("a", maxVarcharBytes-1) + "€tail"
	cut := strings.Repeat("a", maxVarcharBytes-1)
	nested := `{"keep":"<&>","n":1.50,"list":[1,{"s":"` + long + `"}],"d":{"x":"` + long + `"}}`
	val, null, truncated, err := encodeValue(colSuper, json.RawMessage(nested))
	require.NoError(t, err)
	assert.False(t, null)
	assert.Equal(t, []string{".list[1].s", ".d.x"}, truncated)
	assert.Equal(t, `{"keep":"<&>","n":1.50,"list":[1,{"s":"`+cut+`"}],"d":{"x":"`+cut+`"}}`, val)

	val, _, truncated, err = encodeValue(colSuper, json.RawMessage(`"`+long+`"`))
	require.NoError(t, err)
	assert.Equal(t, []string{""}, truncated)
	assert.Equal(t, `"`+cut+`"`, val)

	exact := `{"s":"` + strings.Repeat("a", maxVarcharBytes) + `"}`
	val, _, truncated, err = encodeValue(colSuper, json.RawMessage(exact))
	require.NoError(t, err)
	assert.Empty(t, truncated)
	assert.Equal(t, exact, val)
}

func TestEncodeSuperTooLarge(t *testing.T) {
	parts := make([]string, maxSuperBytes/maxVarcharBytes+1)
	for i := range parts {
		parts[i] = `"` + strings.Repeat("a", maxVarcharBytes) + `"`
	}
	_, _, _, err := encodeValue(colSuper, json.RawMessage("["+strings.Join(parts, ",")+"]"))
	require.ErrorIs(t, err, errSuperTooLarge)
}
