package main

import (
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"net/http"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"
	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
)

const typesSchema = `{"type":"object","properties":{
"c_int":{"type":["integer","null"]},
"c_num":{"type":["number","null"]},
"c_bignum":{"type":["number","null"]},
"c_float":{"type":["number","null"]},
"c_bool":{"type":["boolean","null"]},
"c_str":{"type":["string","null"]},
"c_bytes":{"type":["string","null"]},
"c_date":{"type":["string","null"],"format":"date"},
"c_time":{"type":["string","null"],"airbyte_type":"time_without_timezone"},
"c_dt":{"type":["string","null"],"airbyte_type":"timestamp_without_timezone"},
"c_ts":{"type":["string","null"],"format":"date-time","airbyte_type":"timestamp_with_timezone"},
"c_geo":{"type":["string","null"]},
"c_json":{},
"c_rec":{"type":["object","null"],"properties":{"k":{"type":["string","null"]},"v":{"type":["integer","null"]}}},
"c_arr":{"type":["array","null"],"items":{"type":["integer","null"]}},
"c_rarr":{"type":["array","null"],"items":{"type":["object","null"],"properties":{"k":{"type":["string","null"]}}}}
}}`

func TestScalarValues(t *testing.T) {
	rat := func(s string) *big.Rat {
		r, ok := new(big.Rat).SetString(s)
		require.True(t, ok)
		return r
	}
	cases := []struct {
		name  string
		field *bigquery.FieldSchema
		value bigquery.Value
		want  string
	}{
		{"int64", &bigquery.FieldSchema{Type: bigquery.IntegerFieldType}, int64(math.MaxInt64), `9223372036854775807`},
		{"numeric", &bigquery.FieldSchema{Type: bigquery.NumericFieldType}, rat("-99999999999999999999999999999.999999999"), `-99999999999999999999999999999.999999999`},
		{"numeric integer", &bigquery.FieldSchema{Type: bigquery.NumericFieldType}, rat("42"), `42`},
		{"numeric trailing zeros", &bigquery.FieldSchema{Type: bigquery.NumericFieldType}, rat("1.50"), `1.5`},
		{"bignumeric", &bigquery.FieldSchema{Type: bigquery.BigNumericFieldType}, rat("0.12345678901234567890123456789012345678"), `0.12345678901234567890123456789012345678`},
		{"float", &bigquery.FieldSchema{Type: bigquery.FloatFieldType}, 1.5, `1.5`},
		{"NaN", &bigquery.FieldSchema{Type: bigquery.FloatFieldType}, math.NaN(), `null`},
		{"+Inf", &bigquery.FieldSchema{Type: bigquery.FloatFieldType}, math.Inf(1), `null`},
		{"-Inf", &bigquery.FieldSchema{Type: bigquery.FloatFieldType}, math.Inf(-1), `null`},
		{"bool", &bigquery.FieldSchema{Type: bigquery.BooleanFieldType}, true, `true`},
		{"bytes", &bigquery.FieldSchema{Type: bigquery.BytesFieldType}, []byte{0, 0xff}, `"AP8="`},
		{"date", &bigquery.FieldSchema{Type: bigquery.DateFieldType}, civil.Date{Year: 2024, Month: 2, Day: 29}, `"2024-02-29"`},
		{"time", &bigquery.FieldSchema{Type: bigquery.TimeFieldType}, civil.Time{Hour: 23, Minute: 59, Second: 59, Nanosecond: 123456000}, `"23:59:59.123456"`},
		{"whole time", &bigquery.FieldSchema{Type: bigquery.TimeFieldType}, civil.Time{Hour: 1, Minute: 2, Second: 3}, `"01:02:03"`},
		{"datetime", &bigquery.FieldSchema{Type: bigquery.DateTimeFieldType}, civil.DateTime{Date: civil.Date{Year: 2024, Month: 2, Day: 29}, Time: civil.Time{Hour: 12, Minute: 34, Second: 56, Nanosecond: 789000000}}, `"2024-02-29T12:34:56.789"`},
		{"timestamp", &bigquery.FieldSchema{Type: bigquery.TimestampFieldType}, time.Date(2024, 2, 29, 12, 34, 56, 789000000, time.FixedZone("", 7200)), `"2024-02-29T10:34:56.789Z"`},
		{"geography", &bigquery.FieldSchema{Type: bigquery.GeographyFieldType}, "POINT(30.5 50.45)", `"POINT(30.5 50.45)"`},
		{"json", &bigquery.FieldSchema{Type: bigquery.JSONFieldType}, `{"a":[1,2]}`, `{"a":[1,2]}`},
		{"interval", &bigquery.FieldSchema{Type: bigquery.IntervalFieldType}, &bigquery.IntervalValue{Years: 1, Months: 2, Days: 3}, `"1-2 3 0:0:0"`},
		{"range", &bigquery.FieldSchema{Type: bigquery.RangeFieldType, RangeElementType: &bigquery.RangeElementType{Type: bigquery.DateFieldType}}, &bigquery.RangeValue{Start: civil.Date{Year: 2024, Month: 1, Day: 1}}, `"[2024-01-01, UNBOUNDED)"`},
		{"record", &bigquery.FieldSchema{Type: bigquery.RecordFieldType, Schema: bigquery.Schema{{Name: "k", Type: bigquery.StringFieldType}}}, []bigquery.Value{"x"}, `{"k":"x"}`},
		{"repeated", &bigquery.FieldSchema{Type: bigquery.IntegerFieldType, Repeated: true}, []bigquery.Value{int64(1), int64(2)}, `[1,2]`},
		{"NULL repeated", &bigquery.FieldSchema{Type: bigquery.IntegerFieldType, Repeated: true}, nil, `[]`},
		{"NULL", &bigquery.FieldSchema{Type: bigquery.StringFieldType}, nil, `null`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			v, err := fieldValue(tc.field, tc.value)
			require.NoError(t, err)
			got, err := json.Marshal(v)
			require.NoError(t, err)
			require.Equal(t, tc.want, string(got))
		})
	}
}

// TestRealWireFormat reads canned tables.list, tables.get and tabledata.list
// responses in BigQuery's wire format: the emulator returns GEOGRAPHY WKT with
// a space after the type name and drops table constraints, so the WKT and the
// primary key are tested here.
func TestRealWireFormat(t *testing.T) {
	src := fakeBigQuery(t,
		fakeRoute{http.MethodGet, "p/datasets/d/tables", http.StatusOK, testdata(t, "real_tables_list.json")},
		fakeRoute{http.MethodGet, "p/datasets/d/tables/types", http.StatusOK, testdata(t, "real_types_table.json")},
		fakeRoute{http.MethodGet, "p/datasets/d/tables/types/data", http.StatusOK, testdata(t, "real_types_data.json")},
	)
	cfg := Config{ProjectID: "p", Datasets: []string{"d"}}
	stream := discoverStream(t, src, cfg, "d", "types")
	require.JSONEq(t, typesSchema, string(stream.JSONSchema))
	require.Equal(t, [][]string{{"c_int"}}, stream.SourceDefinedPrimaryKey)

	out := runRead(t, src, cfg, &synclet.ConfiguredCatalog{Streams: []synclet.ConfiguredStream{fullRefresh(stream)}}, nil)
	nulls := `"c_arr":[],"c_bignum":null,"c_bool":null,"c_bytes":null,"c_date":null,"c_dt":null,"c_float":null,"c_geo":null,"c_int":%s,` +
		`"c_json":null,"c_num":null,"c_rarr":[],"c_rec":null,"c_str":null,"c_time":null,"c_ts":null`
	require.Equal(t, []string{
		`{"c_arr":[1,2],"c_bignum":0.12345678901234567890123456789012345678,"c_bool":true,"c_bytes":"AP8=","c_date":"2024-02-29",` +
			`"c_dt":"2024-02-29T12:34:56.789","c_float":1.5,"c_geo":"POINT(30.5 50.45)","c_int":9223372036854775807,"c_json":{"a":[1,2]},` +
			`"c_num":-99999999999999999999999999999.999999999,"c_rarr":[{"k":"k1"}],"c_rec":{"k":"x","v":1},"c_str":"héllo",` +
			`"c_time":"23:59:59.123456","c_ts":"2024-02-29T10:34:56.789Z"}`,
		"{" + fmt.Sprintf(nulls, "2") + "}",
		"{" + fmt.Sprintf(nulls, "3") + "}",
	}, rawRecords(t, out, stream.Descriptor()))
}
