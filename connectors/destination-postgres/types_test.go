package main

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTypesColumn(t *testing.T) {
	cases := []struct {
		schema string
		want   colType
	}{
		{`{"type":"boolean"}`, colBoolean},
		{`{"type":["null","boolean"]}`, colBoolean},
		{`{"type":"integer"}`, colBigint},
		{`{"type":"number","airbyte_type":"integer"}`, colBigint},
		{`{"type":"number"}`, colNumeric},
		{`{"type":"string","airbyte_type":"big_integer"}`, colNumeric},
		{`{"type":"number","airbyte_type":"big_integer"}`, colNumeric},
		{`{"type":"string"}`, colText},
		{`{"type":"string","format":"date"}`, colDate},
		{`{"type":"string","format":"date-time"}`, colTimestamptz},
		{`{"type":"string","format":"date-time","airbyte_type":"timestamp_with_timezone"}`, colTimestamptz},
		{`{"type":"string","format":"date-time","airbyte_type":"timestamp_without_timezone"}`, colTimestamp},
		{`{"type":"string","format":"time"}`, colTime},
		{`{"type":"string","format":"time","airbyte_type":"time_without_timezone"}`, colTime},
		{`{"type":"string","format":"time","airbyte_type":"time_with_timezone"}`, colTimetz},
		{`{"type":"object","properties":{"a":{"type":"string"}}}`, colJSONB},
		{`{"type":"array","items":{"type":"integer"}}`, colJSONB},
		{`{"type":["string","integer"]}`, colJSONB},
		{`{"anyOf":[{"type":"string"},{"type":"integer"}]}`, colJSONB},
		{`{}`, colJSONB},
		{`{"$ref":"WellKnownTypes.json#/definitions/Integer"}`, colBigint},
		{`{"$ref":"WellKnownTypes.json#/definitions/Number"}`, colNumeric},
		{`{"$ref":"WellKnownTypes.json#/definitions/TimestampWithoutTimezone"}`, colTimestamp},
	}
	for _, c := range cases {
		var prop map[string]any
		require.NoError(t, json.Unmarshal([]byte(c.schema), &prop))
		assert.Equal(t, c.want, columnType(prop), c.schema)
	}
}

func TestTypesEncode(t *testing.T) {
	ok := []struct {
		typ  colType
		raw  string
		want string
	}{
		{colNumeric, `12345678901234567890.123456789012345678`, `12345678901234567890.123456789012345678`},
		{colNumeric, `1e-30`, `1e-30`},
		{colNumeric, `"12.50"`, `12.50`},
		{colNumeric, `-0.000000000000000000000000000001`, `-0.000000000000000000000000000001`},
		{colBigint, `9223372036854775807`, `9223372036854775807`},
		{colBigint, `"-42"`, `-42`},
		{colBoolean, `true`, `t`},
		{colBoolean, `false`, `f`},
		{colText, `"tab\there"`, "tab\there"},
		{colText, `42.10`, `42.10`},
		{colDate, `"2024-01-31"`, `2024-01-31`},
		{colJSONB, `{"a": [1, 2.50]}`, `{"a": [1, 2.50]}`},
		{colJSONB, `"plain"`, `"plain"`},
	}
	for _, c := range ok {
		got, null, err := encodeValue(c.typ, json.RawMessage(c.raw))
		require.NoError(t, err, c.raw)
		assert.False(t, null, c.raw)
		assert.Equal(t, c.want, got, c.raw)
	}

	_, null, err := encodeValue(colNumeric, json.RawMessage(`null`))
	require.NoError(t, err)
	assert.True(t, null)

	bad := []struct {
		typ  colType
		raw  string
		want string
	}{
		{colBigint, `9223372036854775808`, "outside the bigint range"},
		{colBigint, `1.5`, "not an integer"},
		{colNumeric, `"abc"`, "not a number"},
		{colNumeric, `true`, "not a number"},
		{colBoolean, `1`, "not a boolean"},
		{colText, `"a\u0000b"`, `\u0000`},
		{colJSONB, `{"a":"x\u0000"}`, `\u0000`},
		{colDate, `20240131`, "not a string"},
	}
	for _, c := range bad {
		_, _, err := encodeValue(c.typ, json.RawMessage(c.raw))
		require.Error(t, err, c.raw)
		assert.Contains(t, err.Error(), c.want, c.raw)
	}
}

func TestTypesCopyEscape(t *testing.T) {
	var b bytes.Buffer
	copyEscape(&b, "a\\b\tc\nd\re")
	assert.Equal(t, `a\\b\tc\nd\re`, b.String())
}
