package main

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"io"
	"testing"

	"github.com/parquet-go/parquet-go/format"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func formatOf(t *testing.T, raw string) Format {
	t.Helper()
	var cfg Config
	require.NoError(t, json.Unmarshal([]byte(`{"bucket":"b","format":`+raw+`}`), &cfg))
	require.NoError(t, cfg.Format.validate())
	return cfg.Format
}

// encodeFixture writes every fixture record with the given format.
func encodeFixture(t *testing.T, f Format, records []string) []byte {
	t.Helper()
	layout := newStreamLayout("orders", fixtureColumns(t))
	var buf bytes.Buffer
	fw, err := newFileWriter(f, layout, &buf)
	require.NoError(t, err)
	for i, raw := range records {
		var fields map[string]json.RawMessage
		require.NoError(t, json.Unmarshal([]byte(raw), &fields))
		require.NoError(t, fw.write(&record{id: "00000000-0000-0000-0000-00000000000" + string(rune('0'+i)), extractedAt: 1760778000000, data: json.RawMessage(raw), fields: fields}))
	}
	require.Positive(t, fw.size())
	require.NoError(t, fw.close())
	return buf.Bytes()
}

func expectedRecords(t *testing.T) []map[string]any {
	t.Helper()
	cols := fixtureColumns(t)
	var out []map[string]any
	for _, raw := range fixtureRecords {
		out = append(out, expectedRecord(t, cols, raw))
	}
	return out
}

func TestFormatReadBackJSONL(t *testing.T) {
	for _, compression := range []string{compressionNone, compressionGzip} {
		t.Run(compression, func(t *testing.T) {
			b := encodeFixture(t, formatOf(t, `{"format_type":"jsonl","compression":"`+compression+`"}`), fixtureRecords)
			if compression == compressionGzip {
				b = gunzip(t, b)
			}
			data := readJSONL(t, b)
			require.Len(t, data, len(fixtureRecords))
			for i, raw := range fixtureRecords {
				assert.Equal(t, raw, string(data[i]), "record bytes are kept verbatim")
			}
		})
	}
}

func TestFormatReadBackCSV(t *testing.T) {
	for _, compression := range []string{compressionNone, compressionGzip} {
		t.Run(compression, func(t *testing.T) {
			b := encodeFixture(t, formatOf(t, `{"format_type":"csv","compression":"`+compression+`"}`), fixtureRecords)
			if compression == compressionGzip {
				b = gunzip(t, b)
			}
			assert.Equal(t, expectedRecords(t), readCSV(t, fixtureColumns(t), b))
		})
	}
}

func TestFormatCSVNullAndEmpty(t *testing.T) {
	b := encodeFixture(t, formatOf(t, `{"format_type":"csv"}`), fixtureRecords[1:2])
	rows := parseCSV(t, b)
	require.Len(t, rows, 2)
	byName := map[string]csvField{}
	for i, h := range rows[0] {
		byName[h.value] = rows[1][i]
	}
	assert.Equal(t, csvField{value: "", quoted: true}, byName["name"], "empty string is quoted")
	assert.Equal(t, csvField{value: "", quoted: false}, byName["note"], "null is empty and unquoted")
	assert.Equal(t, csvField{value: "", quoted: false}, byName[colAdditional])
}

func TestFormatReadBackParquet(t *testing.T) {
	for _, compression := range []string{compressionSnappy, compressionZstd, compressionGzip, compressionNone} {
		t.Run(compression, func(t *testing.T) {
			b := encodeFixture(t, formatOf(t, `{"format_type":"parquet","compression":"`+compression+`"}`), fixtureRecords)
			got, _ := readParquet(t, fixtureColumns(t), defaultDecimalScale, b)
			assert.Equal(t, expectedRecords(t), got)
		})
	}
}

func TestFormatParquetLogicalTypes(t *testing.T) {
	b := encodeFixture(t, formatOf(t, `{"format_type":"parquet"}`), fixtureRecords)
	_, el := readParquet(t, fixtureColumns(t), defaultDecimalScale, b)

	for name, e := range el {
		assert.Zero(t, e.NumChildren, "column %q must be a leaf, not a struct", name)
	}
	logical := func(name string) any {
		e, ok := el[name]
		require.True(t, ok, name)
		require.NotNil(t, e.LogicalType, name)
		return e.LogicalType.Value
	}
	assert.IsType(t, &format.DateType{}, logical("day"))
	assert.Equal(t, &format.TimestampType{IsAdjustedToUTC: true, Unit: format.TimeUnit{Value: &format.MicroSeconds{}}}, logical("created_at"))
	assert.Equal(t, &format.TimestampType{IsAdjustedToUTC: false, Unit: format.TimeUnit{Value: &format.MicroSeconds{}}}, logical("local_at"))
	assert.Equal(t, &format.TimeType{IsAdjustedToUTC: false, Unit: format.TimeUnit{Value: &format.MicroSeconds{}}}, logical("opens"))
	assert.Equal(t, &format.DecimalType{Scale: 9, Precision: 38}, logical("amount"))
	physical, ok := el["amount"].Type.Get()
	require.True(t, ok)
	assert.Equal(t, format.FixedLenByteArray, physical)
	assert.IsType(t, &format.JsonType{}, logical("meta"))
	assert.IsType(t, &format.JsonType{}, logical("tags"))
	assert.IsType(t, &format.JsonType{}, logical("mixed"))
	assert.IsType(t, &format.JsonType{}, logical(colAdditional))
	assert.IsType(t, &format.StringType{}, logical("name"))
	assert.IsType(t, &format.IntType{}, logical("id"))
	assert.Equal(t, &format.TimestampType{IsAdjustedToUTC: true, Unit: format.TimeUnit{Value: &format.MilliSeconds{}}}, logical(colExtractedAt))
}

func TestFormatParquetNumberAsString(t *testing.T) {
	b := encodeFixture(t, formatOf(t, `{"format_type":"parquet","parquet_number_type":"string"}`), fixtureRecords)
	got, el := readParquet(t, fixtureColumns(t), 0, b)
	assert.IsType(t, &format.StringType{}, el["amount"].LogicalType.Value)
	assert.Equal(t, expectedRecords(t), got)
}

func TestFormatParquetRejectsValuesThatDoNotFit(t *testing.T) {
	cases := map[string]struct {
		format string
		record string
		want   string
	}{
		"wrong type":             {`{"format_type":"parquet"}`, `{"id":"1"}`, `stream "orders" field "id" value "1": is not a number`},
		"int64 overflow":         {`{"format_type":"parquet"}`, `{"id":9223372036854775808}`, `field "id" value 9223372036854775808: does not fit INT64`},
		"fractional integer":     {`{"format_type":"parquet"}`, `{"id":1.5}`, `is not an integer`},
		"decimal scale":          {`{"format_type":"parquet","decimal_scale":2}`, `{"amount":1.234}`, `field "amount" value 1.234: has more than 2 decimal places`},
		"decimal precision":      {`{"format_type":"parquet"}`, `{"amount":1e30}`, `does not fit DECIMAL(38, 9)`},
		"sub-microsecond":        {`{"format_type":"parquet"}`, `{"created_at":"2026-10-18T09:00:00.1234567Z"}`, `has digits below microseconds`},
		"date":                   {`{"format_type":"parquet"}`, `{"day":"18.10.2026"}`, `is not a date`},
		"offset in local stamp":  {`{"format_type":"parquet"}`, `{"local_at":"2026-10-18T09:00:00Z"}`, `is not a date-time without time zone`},
		"boolean":                {`{"format_type":"parquet"}`, `{"active":"yes"}`, `is not a boolean`},
		"string":                 {`{"format_type":"parquet"}`, `{"name":5}`, `is not a string`},
		"number as string mode":  {`{"format_type":"parquet","parquet_number_type":"string"}`, `{"amount":"1"}`, `is not a number`},
		"time sub-microsecond":   {`{"format_type":"parquet"}`, `{"opens":"08:00:00.0000001"}`, `has digits below microseconds`},
		"decimal negative bound": {`{"format_type":"parquet","decimal_scale":0}`, `{"amount":-100000000000000000000000000000000000000}`, `does not fit DECIMAL(38, 0)`},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			layout := newStreamLayout("orders", fixtureColumns(t))
			fw, err := newFileWriter(formatOf(t, tc.format), layout, io.Discard)
			require.NoError(t, err)
			var fields map[string]json.RawMessage
			require.NoError(t, json.Unmarshal([]byte(tc.record), &fields))
			err = fw.write(&record{id: "x", data: json.RawMessage(tc.record), fields: fields})
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.want)
		})
	}
}

func TestFormatDecimalBounds(t *testing.T) {
	max := "99999999999999999999999999999.999999999"
	b, err := decimalValue(json.RawMessage(max), 9)
	require.NoError(t, err)
	assert.Equal(t, max, decimalString(b, 9))
	b, err = decimalValue(json.RawMessage("-"+max), 9)
	require.NoError(t, err)
	assert.Equal(t, "-"+max, decimalString(b, 9))
	_, err = decimalValue(json.RawMessage("1"+max), 9)
	require.Error(t, err)
}

func gunzip(t *testing.T, b []byte) []byte {
	t.Helper()
	r, err := gzip.NewReader(bytes.NewReader(b))
	require.NoError(t, err)
	out, err := io.ReadAll(r)
	require.NoError(t, err)
	return out
}
