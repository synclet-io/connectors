package main

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConvert(t *testing.T) {
	tests := []struct {
		kind propKind
		in   string
		want any
		err  string
	}{
		{kindString, `"a;b"`, "a;b", ""},
		{kindString, `""`, "", ""},
		{kindString, `null`, nil, ""},
		{kindNumber, `"12.50"`, json.RawMessage("12.50"), ""},
		{kindNumber, `"-3e2"`, json.RawMessage("-3e2"), ""},
		{kindNumber, `""`, nil, ""},
		{kindNumber, `"1,000"`, nil, "not a number"},
		{kindNumber, `"NaN"`, nil, "not a number"},
		{kindBool, `"true"`, true, ""},
		{kindBool, `"false"`, false, ""},
		{kindBool, `"1"`, nil, "not a boolean"},
		{kindDateTime, `"2026-10-01T10:00:00.123Z"`, "2026-10-01T10:00:00.123Z", ""},
		{kindDateTime, `"2026-10-01T12:00:00+02:00"`, "2026-10-01T10:00:00Z", ""},
		{kindDateTime, `"1790812800000"`, "2026-10-01T00:00:00Z", ""},
		{kindDateTime, `"yesterday"`, nil, "not a date-time"},
		{kindDate, `"2026-10-01"`, "2026-10-01", ""},
		{kindDate, `"2026-10-01T00:00:00Z"`, "2026-10-01", ""},
		{kindDate, `"1790812800000"`, "2026-10-01", ""},
		{kindDate, `"2026-10-01T10:00:00Z"`, nil, "not a date"},
		{kindDate, `""`, nil, ""},
		{kindString, `12`, nil, "not a string"},
	}
	for _, tt := range tests {
		got, err := convert(tt.kind, json.RawMessage(tt.in))
		if tt.err != "" {
			require.Error(t, err, tt.in)
			assert.Contains(t, err.Error(), tt.err)
			continue
		}
		require.NoError(t, err, tt.in)
		assert.Equal(t, tt.want, got, tt.in)
	}
}

func TestKindOf(t *testing.T) {
	for in, want := range map[string]propKind{
		"string": kindString, "phone_number": kindString, "enumeration": kindString, "json": kindString,
		"object_coordinates": kindString, "unknown": kindString, "number": kindNumber, "bool": kindBool,
		"datetime": kindDateTime, "date": kindDate,
	} {
		assert.Equal(t, want, kindOf(in), in)
		assert.Equal(t, want, kindOfField(schemaFieldOf(t, want)), in)
	}
}

func schemaFieldOf(t *testing.T, k propKind) schemaField {
	t.Helper()
	raw, err := json.Marshal(k.spec())
	require.NoError(t, err)
	var f schemaField
	require.NoError(t, json.Unmarshal(raw, &f))
	return f
}

func TestConfigValidate(t *testing.T) {
	assert.NoError(t, Config{AccessToken: "x"}.validate())
	assert.NoError(t, Config{AccessToken: "x", StartDate: "2026-01-01T00:00:00Z"}.validate())
	err := Config{StartDate: "01/02/2026"}.validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "access_token is required")
	assert.Contains(t, err.Error(), "start_date")
	start, err := Config{}.start()
	require.NoError(t, err)
	assert.Equal(t, int64(0), start.Unix())
}
