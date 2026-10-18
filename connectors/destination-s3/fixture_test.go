package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/format"
	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
)

// fixtureSchema covers every column kind; "extra" in the records is
// undeclared.
const fixtureSchema = `{"type":"object","properties":{
	"id":{"type":"integer"},
	"amount":{"type":["null","number"]},
	"day":{"type":"string","format":"date"},
	"created_at":{"type":"string","format":"date-time","airbyte_type":"timestamp_with_timezone"},
	"local_at":{"type":"string","format":"date-time","airbyte_type":"timestamp_without_timezone"},
	"opens":{"type":"string","format":"time","airbyte_type":"time_without_timezone"},
	"name":{"type":"string"},
	"note":{"type":["null","string"]},
	"active":{"type":"boolean"},
	"meta":{"type":"object"},
	"tags":{"type":"array","items":{"type":"string"}},
	"mixed":{"type":["string","integer"]},
	"country":{"type":"string"}
}}`

var fixtureRecords = []string{
	`{"id":1,"amount":12345678901234567890.123456789,"day":"2026-10-18","created_at":"2026-10-18T09:00:00.123456Z","local_at":"2026-10-18T11:00:00.5","opens":"08:30:00.25","name":"comma, \"quote\"\nnewline","note":"","active":true,"meta":{"b":1,"a":{"x":[1,2]}},"tags":["x","y"],"mixed":"s","country":"UA","extra":{"k":"v"}}`,
	`{"id":-9223372036854775808,"amount":-0.000000001,"day":"1969-12-31","created_at":"2026-10-18T11:00:00+02:00","local_at":"1999-01-01T00:00:00","opens":"23:59:59.999999","name":"","note":null,"active":false,"meta":{},"tags":[],"mixed":7,"country":"PL"}`,
	`{"id":3,"amount":null,"day":"2000-02-29","created_at":"1970-01-01T00:00:00Z","local_at":"2026-01-01T00:00:00","opens":"00:00:00","name":"ünïcode ✓","active":true,"country":null,"another":[1,"a"]}`,
}

func fixtureCatalog(t *testing.T, streams ...synclet.ConfiguredStream) *synclet.ConfiguredCatalog {
	t.Helper()
	return &synclet.ConfiguredCatalog{Streams: streams}
}

func fixtureStream(name, namespace string, mode synclet.DestinationSyncMode) synclet.ConfiguredStream {
	return synclet.ConfiguredStream{
		Stream: synclet.Stream{
			Name:               name,
			Namespace:          namespace,
			JSONSchema:         json.RawMessage(fixtureSchema),
			SupportedSyncModes: []synclet.SyncMode{synclet.SyncModeFullRefresh, synclet.SyncModeIncremental},
		},
		SyncMode:            synclet.SyncModeFullRefresh,
		DestinationSyncMode: mode,
	}
}

func fixtureColumns(t *testing.T) []column {
	t.Helper()
	cols, err := streamColumns(json.RawMessage(fixtureSchema))
	require.NoError(t, err)
	return cols
}

// expectedRecord is a fixture record as the columnar formats read it back:
// declared and undeclared fields, nulls dropped, values normalized.
func expectedRecord(t *testing.T, cols []column, raw string) map[string]any {
	t.Helper()
	m := decodeObject(t, []byte(raw))
	kinds := map[string]columnKind{}
	for _, c := range cols {
		kinds[c.name] = c.kind
	}
	out := map[string]any{}
	for k, v := range m {
		if v == nil {
			continue
		}
		kind, ok := kinds[k]
		if !ok {
			kind = kindJSON
		}
		out[k] = normalize(t, kind, v)
	}
	return out
}

func decodeObject(t *testing.T, b []byte) map[string]any {
	t.Helper()
	var m map[string]any
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	require.NoError(t, dec.Decode(&m))
	return m
}

func decodeValue(t *testing.T, s string) any {
	t.Helper()
	var v any
	dec := json.NewDecoder(strings.NewReader(s))
	dec.UseNumber()
	require.NoError(t, dec.Decode(&v), s)
	return v
}

// normalize makes equal values compare equal: numbers as exact rationals,
// timestamps with a zone as UTC instants.
func normalize(t *testing.T, kind columnKind, v any) any {
	t.Helper()
	switch x := v.(type) {
	case json.Number:
		r, ok := new(big.Rat).SetString(x.String())
		require.True(t, ok, x)
		return "num:" + r.RatString()
	case map[string]any:
		out := map[string]any{}
		for k, e := range x {
			out[k] = normalize(t, kindJSON, e)
		}
		return out
	case []any:
		out := make([]any, len(x))
		for i, e := range x {
			out[i] = normalize(t, kindJSON, e)
		}
		return out
	case string:
		if kind == kindTimestampTZ {
			ts, err := parseTimestamp(x, true)
			require.NoError(t, err)
			return ts.UTC().Format(time.RFC3339Nano)
		}
	}
	return v
}

// readJSONL returns each line's _airbyte_data bytes, checking the metadata.
func readJSONL(t *testing.T, b []byte) []json.RawMessage {
	t.Helper()
	var out []json.RawMessage
	for _, line := range bytes.Split(bytes.TrimSuffix(b, []byte("\n")), []byte("\n")) {
		var row struct {
			ID          string          `json:"_airbyte_raw_id"`
			ExtractedAt int64           `json:"_airbyte_extracted_at"`
			Data        json.RawMessage `json:"_airbyte_data"`
		}
		require.NoError(t, json.Unmarshal(line, &row))
		require.Len(t, row.ID, 36)
		require.NotZero(t, row.ExtractedAt)
		out = append(out, row.Data)
	}
	return out
}

// csvField is one parsed CSV field and whether it was quoted.
type csvField struct {
	value  string
	quoted bool
}

// parseCSV parses RFC 4180 CSV keeping whether each field was quoted, which
// encoding/csv drops.
func parseCSV(t *testing.T, b []byte) [][]csvField {
	t.Helper()
	var rows [][]csvField
	var row []csvField
	s := string(b)
	for len(s) > 0 {
		var f csvField
		if s[0] == '"' {
			f.quoted = true
			s = s[1:]
			var sb strings.Builder
			for {
				i := strings.IndexByte(s, '"')
				require.GreaterOrEqual(t, i, 0, "unterminated quote")
				sb.WriteString(s[:i])
				s = s[i+1:]
				if strings.HasPrefix(s, `"`) {
					sb.WriteByte('"')
					s = s[1:]
					continue
				}
				break
			}
			f.value = sb.String()
		} else {
			i := strings.IndexAny(s, ",\n")
			if i < 0 {
				i = len(s)
			}
			f.value = s[:i]
			s = s[i:]
		}
		row = append(row, f)
		switch {
		case strings.HasPrefix(s, ","):
			s = s[1:]
		case strings.HasPrefix(s, "\n"):
			s = s[1:]
			rows = append(rows, row)
			row = nil
		case s == "":
		default:
			t.Fatalf("unexpected character after field: %q", s[:1])
		}
	}
	if row != nil {
		rows = append(rows, row)
	}
	return rows
}

// readCSV turns CSV rows back into records by the column kinds.
func readCSV(t *testing.T, cols []column, b []byte) []map[string]any {
	t.Helper()
	rows := parseCSV(t, b)
	require.NotEmpty(t, rows)
	header := rows[0]
	require.Equal(t, colRawID, header[0].value)
	require.Equal(t, colExtractedAt, header[1].value)
	require.Equal(t, colAdditional, header[len(header)-1].value)
	for i, c := range cols {
		require.Equal(t, c.name, header[i+2].value)
		require.True(t, header[i+2].quoted)
	}
	var out []map[string]any
	for _, row := range rows[1:] {
		require.Len(t, row, len(header))
		require.True(t, row[0].quoted)
		require.Len(t, row[0].value, 36)
		_, err := strconv.ParseInt(row[1].value, 10, 64)
		require.NoError(t, err)
		rec := map[string]any{}
		for i, c := range cols {
			f := row[i+2]
			if !f.quoted {
				require.Empty(t, f.value, "unquoted field must be null")
				continue
			}
			var v any
			switch c.kind {
			case kindString, kindDate, kindTimestampTZ, kindTimestampNTZ, kindTimeTZ, kindTimeNTZ:
				v = f.value
			case kindInteger, kindNumber, kindBigInteger:
				v = json.Number(f.value)
			case kindBoolean:
				v = f.value == "true"
			default:
				if json.Valid([]byte(f.value)) {
					v = decodeValue(t, f.value)
				} else {
					v = f.value
				}
			}
			rec[c.name] = normalize(t, c.kind, v)
		}
		if extra := row[len(row)-1]; extra.quoted {
			for k, v := range decodeObject(t, []byte(extra.value)) {
				rec[k] = normalize(t, kindJSON, v)
			}
		}
		out = append(out, rec)
	}
	return out
}

// readParquet turns Parquet rows back into records by the column kinds and
// returns the file's schema elements by name.
func readParquet(t *testing.T, cols []column, scale int, b []byte) ([]map[string]any, map[string]format.SchemaElement) {
	t.Helper()
	f, err := parquet.OpenFile(bytes.NewReader(b), int64(len(b)))
	require.NoError(t, err)
	elements := map[string]format.SchemaElement{}
	for _, e := range f.Metadata().Schema[1:] {
		elements[e.Name] = e
	}
	schema := f.Schema()
	kinds := map[string]columnKind{}
	for _, c := range cols {
		kinds[c.name] = c.kind
	}
	names := map[int]string{}
	for _, path := range schema.Columns() {
		leaf, ok := schema.Lookup(path...)
		require.True(t, ok)
		names[leaf.ColumnIndex] = path[0]
	}
	r := parquet.NewReader(bytes.NewReader(b))
	var out []map[string]any
	rows := make([]parquet.Row, 1)
	for {
		n, err := r.ReadRows(rows)
		if n == 0 && errors.Is(err, io.EOF) {
			break
		}
		require.Equal(t, 1, n)
		rec := map[string]any{}
		for _, v := range rows[0] {
			name := names[v.Column()]
			switch name {
			case colRawID:
				require.Len(t, v.ByteArray(), 36)
				continue
			case colExtractedAt:
				require.NotZero(t, v.Int64())
				continue
			}
			if v.IsNull() {
				continue
			}
			if name == colAdditional {
				for k, e := range decodeObject(t, v.ByteArray()) {
					rec[k] = normalize(t, kindJSON, e)
				}
				continue
			}
			rec[name] = normalize(t, kinds[name], parquetGoValue(t, kinds[name], scale, v))
		}
		out = append(out, rec)
		if errors.Is(err, io.EOF) {
			break
		}
	}
	return out, elements
}

func parquetGoValue(t *testing.T, kind columnKind, scale int, v parquet.Value) any {
	t.Helper()
	switch kind {
	case kindBoolean:
		return v.Boolean()
	case kindInteger:
		return json.Number(strconv.FormatInt(v.Int64(), 10))
	case kindNumber:
		if v.Kind() == parquet.ByteArray {
			return json.Number(string(v.ByteArray()))
		}
		return json.Number(decimalString(v.ByteArray(), scale))
	case kindBigInteger:
		return json.Number(decimalString(v.ByteArray(), 0))
	case kindDate:
		return time.Unix(int64(v.Int32())*86400, 0).UTC().Format(time.DateOnly)
	case kindTimestampTZ:
		return time.UnixMicro(v.Int64()).UTC().Format(time.RFC3339Nano)
	case kindTimestampNTZ:
		return time.UnixMicro(v.Int64()).UTC().Format("2006-01-02T15:04:05.999999999")
	case kindTimeNTZ:
		return time.Time{}.Add(time.Duration(v.Int64()) * time.Microsecond).Format("15:04:05.999999999")
	case kindString, kindTimeTZ:
		return string(v.ByteArray())
	default:
		return decodeValue(t, string(v.ByteArray()))
	}
}

// decimalString renders an unscaled two's complement decimal as text.
func decimalString(b []byte, scale int) string {
	u := new(big.Int).SetBytes(b)
	if len(b) > 0 && b[0]&0x80 != 0 {
		u.Sub(u, new(big.Int).Lsh(big.NewInt(1), uint(8*len(b))))
	}
	return new(big.Rat).SetFrac(u, new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(scale)), nil)).FloatString(scale)
}

func recordMessage(stream, namespace, data string) string {
	return fmt.Sprintf(`{"type":"RECORD","record":{"stream":%q,"namespace":%q,"data":%s,"emitted_at":1760778000000}}`, stream, namespace, data)
}

func stateMessage(stream, namespace string, cursor int) string {
	return fmt.Sprintf(`{"type":"STATE","state":{"type":"STREAM","stream":{"stream_descriptor":{"name":%q,"namespace":%q},"stream_state":{"cursor":%d}}}}`, stream, namespace, cursor)
}
