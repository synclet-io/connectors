package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
)

func testConfig(numberType string) Config {
	cfg := Config{NumberType: numberType}
	cfg.applyDefaults()
	return cfg
}

func fixtureColumns(t *testing.T, cfg Config) []column {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("testdata", "catalog.json"))
	require.NoError(t, err)
	var catalog synclet.ConfiguredCatalog
	require.NoError(t, json.Unmarshal(b, &catalog))
	cols, err := streamColumns(catalog.Streams[0].Stream.JSONSchema, cfg)
	require.NoError(t, err)
	return cols
}

// fixtureTypes is the documented Iceberg type of every column of
// testdata/catalog.json.
var fixtureTypes = map[string]string{
	"_airbyte_raw_id":       "string",
	"_airbyte_extracted_at": "timestamptz",
	"id":                    "long",
	"bool_col":              "boolean",
	"int_col":               "long",
	"airbyte_int_col":       "long",
	"num_col":               "decimal(38, 9)",
	"big_int_col":           "decimal(38, 0)",
	"big_int_num_col":       "decimal(38, 0)",
	"str_col":               "string",
	"date_col":              "date",
	"ts_col":                "timestamptz",
	"ts_ntz_col":            "timestamp",
	"time_col":              "time",
	"time_plain_col":        "time",
	"timetz_col":            "string",
	"obj_col":               "string",
	"arr_col":               "string",
	"union_col":             "string",
	"any_of_col":            "string",
	"untyped_col":           "string",
}

func TestTypesFixtureMapping(t *testing.T) {
	cols := fixtureColumns(t, testConfig(""))
	got := map[string]string{}
	var order []string
	for _, c := range cols {
		got[c.name] = c.typ.String()
		order = append(order, c.name)
	}
	assert.Equal(t, fixtureTypes, got)
	assert.Equal(t, []string{"_airbyte_raw_id", "_airbyte_extracted_at", "id", "bool_col"}, order[:4], "metadata columns first, then schema order")
}

func TestTypesNumberDouble(t *testing.T) {
	for _, c := range fixtureColumns(t, testConfig(numberTypeDouble)) {
		switch c.name {
		case "num_col":
			assert.Equal(t, "double", c.typ.String())
		case "big_int_col", "big_int_num_col":
			assert.Equal(t, "decimal(38, 0)", c.typ.String())
		}
	}
	scale := 2
	cfg := testConfig("")
	cfg.DecimalScale = &scale
	for _, c := range fixtureColumns(t, cfg) {
		if c.name == "num_col" {
			assert.Equal(t, "decimal(38, 2)", c.typ.String())
		}
	}
}

func TestTypesMetadataCollision(t *testing.T) {
	_, err := streamColumns(json.RawMessage(`{"properties":{"_airbyte_raw_id":{"type":"string"}}}`), testConfig(""))
	assert.EqualError(t, err, `property "_airbyte_raw_id" collides with a metadata column`)
}

func col(kind columnKind, typ iceberg.Type) column {
	return column{name: "c", kind: kind, typ: typ}
}

func TestTypesValues(t *testing.T) {
	dec9 := iceberg.DecimalTypeOf(38, 9)
	dec0 := iceberg.DecimalTypeOf(38, 0)
	for name, tc := range map[string]struct {
		c    column
		raw  string
		want any
	}{
		"decimal":        {col(kindNumber, dec9), `1.5`, decimal128.FromI64(1_500_000_000)},
		"decimal exp":    {col(kindNumber, dec9), `1e-9`, decimal128.FromI64(1)},
		"double":         {col(kindNumber, iceberg.PrimitiveTypes.Float64), `0.1`, 0.1},
		"big int string": {col(kindBigInteger, dec0), `"-42"`, decimal128.FromI64(-42)},
		"big int number": {col(kindBigInteger, dec0), `42`, decimal128.FromI64(42)},
		"long":           {col(kindInteger, iceberg.PrimitiveTypes.Int64), `9223372036854775807`, int64(9223372036854775807)},
		"long as float":  {col(kindInteger, iceberg.PrimitiveTypes.Int64), `3.0`, int64(3)},
		"bool":           {col(kindBoolean, iceberg.PrimitiveTypes.Bool), `false`, false},
		"string":         {col(kindString, iceberg.PrimitiveTypes.String), `"a\"b"`, `a"b`},
		"string number":  {col(kindString, iceberg.PrimitiveTypes.String), `12.50`, `12.50`},
		"json":           {col(kindJSON, iceberg.PrimitiveTypes.String), `{ "a" : [1, 2.50] }`, `{"a":[1,2.50]}`},
		"json string":    {col(kindJSON, iceberg.PrimitiveTypes.String), `"x"`, `"x"`},
		"timetz":         {col(kindTimeTZ, iceberg.PrimitiveTypes.String), `"10:00:00+02:00"`, `10:00:00+02:00`},
		"date":           {col(kindDate, iceberg.PrimitiveTypes.Date), `"1970-01-02"`, arrow.Date32(1)},
		"tstz offset":    {col(kindTimestampTZ, iceberg.PrimitiveTypes.TimestampTz), `"1970-01-01T02:00:00.000001+02:00"`, arrow.Timestamp(1)},
		"tstz no offset": {col(kindTimestampTZ, iceberg.PrimitiveTypes.TimestampTz), `"1970-01-01 00:00:01"`, arrow.Timestamp(1_000_000)},
		"ts":             {col(kindTimestampNTZ, iceberg.PrimitiveTypes.Timestamp), `"1970-01-01T00:00:02"`, arrow.Timestamp(2_000_000)},
		"time":           {col(kindTime, iceberg.PrimitiveTypes.Time), `"00:00:01.25"`, arrow.Time64(1_250_000)},
	} {
		t.Run(name, func(t *testing.T) {
			got, err := convert(tc.c, json.RawMessage(tc.raw))
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestTypesValueErrors(t *testing.T) {
	dec2 := iceberg.DecimalTypeOf(38, 2)
	for name, tc := range map[string]struct {
		c    column
		raw  string
		want string
	}{
		"decimal places":  {col(kindNumber, dec2), `1.234`, "has more than 2 decimal places"},
		"decimal digits":  {col(kindNumber, dec2), `1e36`, "does not fit decimal(38, 2)"},
		"decimal string":  {col(kindNumber, dec2), `"1"`, "is not a number"},
		"big int places":  {col(kindBigInteger, iceberg.DecimalTypeOf(38, 0)), `"1.5"`, "has more than 0 decimal places"},
		"big int digits":  {col(kindBigInteger, iceberg.DecimalTypeOf(38, 0)), `"123456789012345678901234567890123456789"`, "does not fit decimal(38, 0)"},
		"long fraction":   {col(kindInteger, iceberg.PrimitiveTypes.Int64), `1.5`, "is not an integer"},
		"long overflow":   {col(kindInteger, iceberg.PrimitiveTypes.Int64), `9223372036854775808`, "does not fit a long"},
		"double overflow": {col(kindNumber, iceberg.PrimitiveTypes.Float64), `1e400`, "does not fit a double"},
		"bool":            {col(kindBoolean, iceberg.PrimitiveTypes.Bool), `"true"`, "is not a boolean"},
		"date":            {col(kindDate, iceberg.PrimitiveTypes.Date), `"2024-02-30"`, "is not a date"},
		"ts offset":       {col(kindTimestampNTZ, iceberg.PrimitiveTypes.Timestamp), `"2024-01-01T00:00:00Z"`, "is not a date-time without time zone"},
		"ts nanos":        {col(kindTimestampTZ, iceberg.PrimitiveTypes.TimestampTz), `"2024-01-01T00:00:00.0000001Z"`, "has digits below microseconds"},
		"time offset":     {col(kindTime, iceberg.PrimitiveTypes.Time), `"10:00:00+02:00"`, "is not a time without time zone"},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := convert(tc.c, json.RawMessage(tc.raw))
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.want)
		})
	}
}

func TestSchemaEvolve(t *testing.T) {
	cfg := testConfig("")
	old, err := streamColumns(json.RawMessage(`{"properties":{"id":{"type":"integer"},"gone":{"type":"string"}}}`), cfg)
	require.NoError(t, err)
	existing := newSchema(old)

	cols, err := streamColumns(json.RawMessage(`{"properties":{"id":{"type":"integer"},"name":{"type":"string"},"at":{"type":"string","format":"date"}}}`), cfg)
	require.NoError(t, err)
	added, err := evolve(existing, cols)
	require.NoError(t, err)
	var names []string
	for _, c := range added {
		names = append(names, c.name)
	}
	assert.Equal(t, []string{"name", "at"}, names, "new fields are added, dropped fields stay")

	changed, err := streamColumns(json.RawMessage(`{"properties":{"id":{"type":"string"}}}`), cfg)
	require.NoError(t, err)
	_, err = evolve(existing, changed)
	assert.EqualError(t, err, `column "id" is long in the table but string in the catalog; type changes are not supported and the table is unchanged`)
}

func TestSchemaPlanErrors(t *testing.T) {
	cfg := testConfig(numberTypeDouble)
	for name, tc := range map[string]struct {
		streams []synclet.ConfiguredStream
		want    string
	}{
		"dedup without pk": {
			[]synclet.ConfiguredStream{stream("s", `{"properties":{"id":{"type":"integer"}}}`, synclet.DestinationSyncModeAppendDedup, nil, nil)},
			`stream "s": append_dedup needs a primary key`,
		},
		"double pk": {
			[]synclet.ConfiguredStream{stream("s", `{"properties":{"n":{"type":"number"}}}`, synclet.DestinationSyncModeAppendDedup, [][]string{{"n"}}, nil)},
			`stream "s": primary key field "n" is a double column`,
		},
		"nested pk": {
			[]synclet.ConfiguredStream{stream("s", `{"properties":{"id":{"type":"integer"}}}`, synclet.DestinationSyncModeAppendDedup, [][]string{{"a", "b"}}, nil)},
			`primary key field [a b] is nested`,
		},
		"same name": {
			[]synclet.ConfiguredStream{
				stream("s", `{}`, synclet.DestinationSyncModeAppend, nil, nil),
				{Stream: synclet.Stream{Name: "s", Namespace: "other", JSONSchema: json.RawMessage(`{}`)}, DestinationSyncMode: synclet.DestinationSyncModeAppend},
			},
			`streams "s" and "other.s" both map to table "s"`,
		},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := planStreams(&synclet.ConfiguredCatalog{Streams: tc.streams}, cfg)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.want)
		})
	}
}
