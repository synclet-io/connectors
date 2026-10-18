package main

import (
	"bytes"
	"encoding/json"
	"math/big"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/deprecated"
	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
)

func TestFormatParquetTypes(t *testing.T) {
	schema := parquet.NewSchema("types", parquet.Group{
		"day":      parquet.Date(),
		"ts_utc":   parquet.TimestampAdjusted(parquet.Microsecond, true),
		"ts_local": parquet.TimestampAdjusted(parquet.Millisecond, false),
		"ts_nanos": parquet.TimestampAdjusted(parquet.Nanosecond, true),
		"clock":    parquet.Time(parquet.Millisecond),
		"dec32":    parquet.Decimal(2, 9, parquet.Int32Type),
		"dec64":    parquet.Decimal(4, 18, parquet.Int64Type),
		"decbin":   parquet.Decimal(3, 30, parquet.ByteArrayType),
		"dbl":      parquet.Leaf(parquet.DoubleType),
		"u64":      parquet.Uint(64),
		"blob":     parquet.Leaf(parquet.ByteArrayType),
		"legacy":   parquet.Leaf(parquet.Int96Type),
		"tags":     parquet.Optional(parquet.List(parquet.String())),
		"obj":      parquet.Optional(parquet.Group{"a": parquet.Optional(parquet.Int(32)), "b": parquet.String()}),
		"attrs":    parquet.Optional(parquet.Map(parquet.String(), parquet.Int(64))),
	})
	col := func(path ...string) int { return colIndex(t, schema, path...) }
	day := time.Date(2026, 10, 18, 0, 0, 0, 0, time.UTC)
	at := time.Date(2026, 10, 18, 13, 14, 15, 123456000, time.UTC)
	var row parquet.Row
	add := func(v parquet.Value, rep, def int, path ...string) {
		row = append(row, v.Level(rep, def, col(path...)))
	}
	add(parquet.Int32Value(int32(day.Unix()/86400)), 0, 0, "day")
	add(parquet.Int64Value(at.UnixMicro()), 0, 0, "ts_utc")
	add(parquet.Int64Value(at.UnixMilli()), 0, 0, "ts_local")
	add(parquet.Int64Value(at.UnixNano()), 0, 0, "ts_nanos")
	add(parquet.Int32Value(int32((13*3600+14*60+15)*1000+500)), 0, 0, "clock")
	add(parquet.Int32Value(-12345), 0, 0, "dec32")
	add(parquet.Int64Value(1), 0, 0, "dec64")
	unscaled, _ := new(big.Int).SetString("-123456789012345678901234567", 10)
	add(parquet.ByteArrayValue(toTwos(unscaled, 12)), 0, 0, "decbin")
	add(parquet.DoubleValue(0.1), 0, 0, "dbl")
	add(parquet.Int64Value(-1), 0, 0, "u64")
	add(parquet.ByteArrayValue([]byte{0xff, 0x00}), 0, 0, "blob")
	add(parquet.Int96Value(deprecated.Int96{uint32(3_600_000_000_000 & 0xffffffff), uint32(3_600_000_000_000 >> 32), 2440588 + 1}), 0, 0, "legacy")
	add(parquet.ByteArrayValue([]byte("x")), 0, 2, "tags", "list", "element")
	add(parquet.ByteArrayValue([]byte("y")), 1, 2, "tags", "list", "element")
	add(parquet.Int32Value(3), 0, 2, "obj", "a")
	add(parquet.ByteArrayValue([]byte("s")), 0, 1, "obj", "b")
	add(parquet.ByteArrayValue([]byte("k1")), 0, 2, "attrs", "key_value", "key")
	add(parquet.ByteArrayValue([]byte("k2")), 1, 2, "attrs", "key_value", "key")
	add(parquet.Int64Value(5), 0, 2, "attrs", "key_value", "value")
	add(parquet.Int64Value(6), 1, 2, "attrs", "key_value", "value")
	row = sortRow(row)

	var nulls parquet.Row
	for _, c := range []struct {
		path []string
		v    parquet.Value
		def  int
	}{
		{[]string{"day"}, parquet.Int32Value(0), 0},
		{[]string{"ts_utc"}, parquet.Int64Value(0), 0},
		{[]string{"ts_local"}, parquet.Int64Value(0), 0},
		{[]string{"ts_nanos"}, parquet.Int64Value(0), 0},
		{[]string{"clock"}, parquet.Int32Value(0), 0},
		{[]string{"dec32"}, parquet.Int32Value(0), 0},
		{[]string{"dec64"}, parquet.Int64Value(0), 0},
		{[]string{"decbin"}, parquet.ByteArrayValue([]byte{0}), 0},
		{[]string{"dbl"}, parquet.DoubleValue(0), 0},
		{[]string{"u64"}, parquet.Int64Value(0), 0},
		{[]string{"blob"}, parquet.ByteArrayValue(nil), 0},
		{[]string{"legacy"}, parquet.Int96Value(deprecated.Int96{0, 0, 2440588}), 0},
		{[]string{"tags", "list", "element"}, parquet.NullValue(), 1},
		{[]string{"obj", "a"}, parquet.NullValue(), 0},
		{[]string{"obj", "b"}, parquet.NullValue(), 0},
		{[]string{"attrs", "key_value", "key"}, parquet.NullValue(), 0},
		{[]string{"attrs", "key_value", "value"}, parquet.NullValue(), 0},
	} {
		nulls = append(nulls, c.v.Level(0, c.def, col(c.path...)))
	}
	nulls = sortRow(nulls)

	store := newMemStore()
	store.put("p/types.parquet", writeParquet(t, schema, []parquet.Row{row, nulls}), t0)
	src := Source{store: store}
	cfg := testConfig(StreamConfig{Name: "p", Glob: "p/*.parquet", Format: Format{Filetype: formatParquet}})
	s := discoverStreams(t, src, cfg)["p"]
	for name, want := range map[string]string{
		"day":      `{"type":["null","string"],"format":"date"}`,
		"ts_utc":   `{"type":["null","string"],"format":"date-time","airbyte_type":"timestamp_with_timezone"}`,
		"ts_local": `{"type":["null","string"],"format":"date-time","airbyte_type":"timestamp_without_timezone"}`,
		"clock":    `{"type":["null","string"],"format":"time","airbyte_type":"time_with_timezone"}`,
		"dec32":    `{"type":["null","number"]}`,
		"dec64":    `{"type":["null","number"]}`,
		"decbin":   `{"type":["null","number"]}`,
		"dbl":      `{"type":["null","number"]}`,
		"u64":      `{"type":["null","integer"]}`,
		"blob":     `{"type":["null","string"]}`,
		"legacy":   `{"type":["null","string"],"format":"date-time","airbyte_type":"timestamp_with_timezone"}`,
		"tags":     `{"type":["null","array"]}`,
		"obj":      `{"type":["null","object"]}`,
		"attrs":    `{"type":["null","object"]}`,
	} {
		require.JSONEq(t, want, string(propertyOf(t, s, name)), name)
	}

	out := runRead(t, src, cfg, catalogOf(synclet.SyncModeFullRefresh, s), nil)
	require.NoError(t, out.Err)
	recs := records(t, out, "p")
	require.Len(t, recs, 2)
	data, _ := stripMetadata(recs)
	require.Equal(t, map[string]any{
		"day":      "2026-10-18",
		"ts_utc":   "2026-10-18T13:14:15.123456Z",
		"ts_local": "2026-10-18T13:14:15.123",
		"ts_nanos": "2026-10-18T13:14:15.123456Z",
		"clock":    "13:14:15.5Z",
		"dec32":    json.Number("-123.45"),
		"dec64":    json.Number("0.0001"),
		"decbin":   json.Number("-123456789012345678901234.567"),
		"dbl":      json.Number("0.1"),
		"u64":      json.Number("18446744073709551615"),
		"blob":     "/wA=",
		"legacy":   "1970-01-02T01:00:00Z",
		"tags":     []any{"x", "y"},
		"obj":      map[string]any{"a": json.Number("3"), "b": "s"},
		"attrs":    map[string]any{"k1": json.Number("5"), "k2": json.Number("6")},
	}, data[0])
	require.Equal(t, []any{}, data[1]["tags"])
	require.Nil(t, data[1]["obj"])
	require.Nil(t, data[1]["attrs"])
}

// sortRow orders a row's values by column, as a writer expects them.
func sortRow(row parquet.Row) parquet.Row {
	out := make(parquet.Row, 0, len(row))
	for c := 0; len(out) < len(row); c++ {
		for _, v := range row {
			if v.Column() == c {
				out = append(out, v)
			}
		}
	}
	return out
}

func TestFormatDecimal(t *testing.T) {
	for _, c := range []struct {
		unscaled string
		scale    int
		want     string
	}{
		{"0", 2, "0.00"},
		{"5", 3, "0.005"},
		{"-5", 3, "-0.005"},
		{"12345", 2, "123.45"},
		{"7", 0, "7"},
		{"7", -2, "700"},
	} {
		n, _ := new(big.Int).SetString(c.unscaled, 10)
		require.Equal(t, c.want, formatDecimal(n, c.scale), c)
	}
	require.Equal(t, "-1", twosComplement([]byte{0xff}).String())
	require.Equal(t, "255", twosComplement([]byte{0x00, 0xff}).String())
}

func TestFormatParquetRowGroups(t *testing.T) {
	store := newMemStore()
	b := datasetParquet(t, parquet.MaxRowsPerRowGroup(1))
	f, err := parquet.OpenFile(bytes.NewReader(b), int64(len(b)))
	require.NoError(t, err)
	require.Len(t, f.RowGroups(), len(wantDataset))
	store.put("p/groups.parquet", b, t0)
	src := Source{store: store}
	cfg := testConfig(StreamConfig{Name: "p", Glob: "p/*", Format: Format{Filetype: formatParquet}})
	out := runRead(t, src, cfg, catalogOf(synclet.SyncModeFullRefresh, discoverStreams(t, src, cfg)["p"]), nil)
	require.NoError(t, out.Err)
	data, rows := stripMetadata(records(t, out, "p"))
	requireSameRecords(t, wantDataset, data)
	require.Equal(t, []string{"1", "2", "3", "4"}, rows)
}
