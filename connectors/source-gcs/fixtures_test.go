package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"io"
	"math/big"
	"slices"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
	"github.com/synclet-io/synclet-go-sdk/testkit"
)

// memStore is an in-memory objectStore.
type memStore struct {
	mu   sync.Mutex
	objs map[string]memObject
}

type memObject struct {
	data     []byte
	modified time.Time
}

func newMemStore() *memStore { return &memStore{objs: map[string]memObject{}} }

func (m *memStore) put(key string, data []byte, modified time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.objs[key] = memObject{data: data, modified: modified.UTC()}
}

func (m *memStore) List(_ context.Context, prefix string) ([]object, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []object
	for k, o := range m.objs {
		if strings.HasPrefix(k, prefix) {
			out = append(out, object{Key: k, Size: int64(len(o.data)), LastModified: o.modified})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out, nil
}

func (m *memStore) Open(_ context.Context, key string) (io.ReadCloser, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	o, ok := m.objs[key]
	if !ok {
		return nil, io.ErrUnexpectedEOF
	}
	return io.NopCloser(bytes.NewReader(o.data)), nil
}

var t0 = time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)

// The dataset every format encodes: an integer, a decimal wider than a
// float64, a string with the delimiter, quotes and a line break, an empty
// string, nulls and booleans.
const bigDecimal = "12345678901234567890.123456789"

const datasetCSV = "id,amount,name,flag\n" +
	"1," + bigDecimal + ",plain,true\n" +
	"2,-0.5,\"comma, \"\"quote\"\"\nnewline\",false\n" +
	"3,0,\"\",\n" +
	"4,,,true\n"

const datasetJSONL = `{"id":1,"amount":` + bigDecimal + `,"name":"plain","flag":true}
{"id":2,"amount":-0.5,"name":"comma, \"quote\"\nnewline","flag":false}

{"id":3,"amount":0,"name":"","flag":null}
{"id":4,"amount":null,"name":null,"flag":true}
`

const datasetTSV = "id\tamount\tname\tflag\n" +
	"1\t" + bigDecimal + "\tplain\ttrue\n" +
	"2\t-0.5\t\"comma, \"\"quote\"\"\nnewline\"\tfalse\n" +
	"3\t0\t\"\"\t\n" +
	"4\t\t\ttrue\n"

// wantDataset is the dataset as records, without metadata fields.
var wantDataset = []map[string]any{
	{"id": json.Number("1"), "amount": json.Number(bigDecimal), "name": "plain", "flag": true},
	{"id": json.Number("2"), "amount": json.Number("-0.5"), "name": "comma, \"quote\"\nnewline", "flag": false},
	{"id": json.Number("3"), "amount": json.Number("0"), "name": "", "flag": nil},
	{"id": json.Number("4"), "amount": nil, "name": nil, "flag": true},
}

const wantDatasetSchema = `{"type":"object","properties":{
	"id":{"type":["null","integer"]},
	"amount":{"type":["null","number"]},
	"name":{"type":["null","string"]},
	"flag":{"type":["null","boolean"]},
	"_synclet_source_file":{"type":"string"},
	"_synclet_source_row":{"type":"integer"},
	"_synclet_source_modified":{"type":"string","format":"date-time","airbyte_type":"timestamp_with_timezone"}}}`

// datasetParquet is the dataset as Parquet, the decimal as DECIMAL(38,9).
func datasetParquet(t *testing.T, opts ...parquet.WriterOption) []byte {
	t.Helper()
	schema := parquet.NewSchema("dataset", parquet.Group{
		"id":     parquet.Optional(parquet.Int(64)),
		"amount": parquet.Optional(parquet.Decimal(9, 38, parquet.FixedLenByteArrayType(16))),
		"name":   parquet.Optional(parquet.String()),
		"flag":   parquet.Optional(parquet.Leaf(parquet.BooleanType)),
	})
	col := func(name string) int { return colIndex(t, schema, name) }
	dec := func(s string) parquet.Value {
		r, ok := new(big.Rat).SetString(s)
		require.True(t, ok)
		unscaled := new(big.Int).Div(new(big.Int).Mul(r.Num(), new(big.Int).Exp(big.NewInt(10), big.NewInt(9), nil)), r.Denom())
		return parquet.FixedLenByteArrayValue(toTwos(unscaled, 16))
	}
	null := parquet.NullValue()
	str := func(s string) parquet.Value { return parquet.ByteArrayValue([]byte(s)) }
	rows := [][4]parquet.Value{
		{parquet.Int64Value(1), dec(bigDecimal), str("plain"), parquet.BooleanValue(true)},
		{parquet.Int64Value(2), dec("-0.5"), str("comma, \"quote\"\nnewline"), parquet.BooleanValue(false)},
		{parquet.Int64Value(3), dec("0"), str(""), null},
		{parquet.Int64Value(4), null, null, parquet.BooleanValue(true)},
	}
	names := []string{"id", "amount", "name", "flag"}
	var out []parquet.Row
	for _, r := range rows {
		row := make(parquet.Row, 4)
		for i, v := range r {
			def := 1
			if v.IsNull() {
				def = 0
			}
			c := col(names[i])
			row[c] = v.Level(0, def, c)
		}
		out = append(out, row)
	}
	return writeParquet(t, schema, out, opts...)
}

func colIndex(t *testing.T, schema *parquet.Schema, path ...string) int {
	t.Helper()
	for i, c := range schema.Columns() {
		if slices.Equal(c, path) {
			return i
		}
	}
	t.Fatalf("no column %v in %v", path, schema.Columns())
	return -1
}

// toTwos encodes n as a big-endian two's complement of size bytes.
func toTwos(n *big.Int, size int) []byte {
	v := new(big.Int).Set(n)
	if v.Sign() < 0 {
		v.Add(v, new(big.Int).Lsh(big.NewInt(1), uint(size*8)))
	}
	b := v.Bytes()
	return append(make([]byte, size-len(b)), b...)
}

func writeParquet(t *testing.T, schema *parquet.Schema, rows []parquet.Row, opts ...parquet.WriterOption) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := parquet.NewWriter(&buf, append([]parquet.WriterOption{schema}, opts...)...)
	_, err := w.WriteRows(rows)
	require.NoError(t, err)
	require.NoError(t, w.Close())
	return buf.Bytes()
}

func gzipped(t *testing.T, b []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := gzip.NewWriter(&buf)
	_, err := w.Write(b)
	require.NoError(t, err)
	require.NoError(t, w.Close())
	return buf.Bytes()
}

func zstded(t *testing.T, b []byte) []byte {
	t.Helper()
	enc, err := zstd.NewWriter(nil)
	require.NoError(t, err)
	return enc.EncodeAll(b, nil)
}

func mustJSON(t testing.TB, v any) json.RawMessage {
	t.Helper()
	b, err := json.Marshal(v)
	require.NoError(t, err)
	return b
}

// testConfig is a configuration for an in-memory store.
func testConfig(streams ...StreamConfig) Config {
	return Config{
		Bucket:      "test",
		Credentials: Credentials{AuthType: authApplicationDefault},
		Streams:     streams,
	}
}

func runDiscover(t *testing.T, src Source, cfg Config) *testkit.Output {
	t.Helper()
	out, err := testkit.RunSource(t.Context(), src, synclet.Request{Command: synclet.CommandDiscover, Config: mustJSON(t, cfg)})
	require.NoError(t, err)
	return out
}

func discoverStreams(t *testing.T, src Source, cfg Config) map[string]synclet.Stream {
	t.Helper()
	out := runDiscover(t, src, cfg)
	require.NoError(t, out.Err)
	cat := out.Catalog()
	require.NotNil(t, cat)
	streams := map[string]synclet.Stream{}
	for _, s := range cat.Streams {
		streams[s.Name] = s
	}
	return streams
}

func catalogOf(mode synclet.SyncMode, streams ...synclet.Stream) *synclet.ConfiguredCatalog {
	cat := &synclet.ConfiguredCatalog{}
	for _, s := range streams {
		dsm := synclet.DestinationSyncModeOverwrite
		if mode == synclet.SyncModeIncremental {
			dsm = synclet.DestinationSyncModeAppend
		}
		cat.Streams = append(cat.Streams, synclet.ConfiguredStream{Stream: s, SyncMode: mode, DestinationSyncMode: dsm})
	}
	return cat
}

func runRead(t *testing.T, src Source, cfg Config, cat *synclet.ConfiguredCatalog, state json.RawMessage) *testkit.Output {
	t.Helper()
	out, err := testkit.RunSource(t.Context(), src, synclet.Request{
		Command: synclet.CommandRead, Config: mustJSON(t, cfg), Catalog: mustJSON(t, cat), State: state,
	})
	require.NoError(t, err)
	return out
}

// records decodes a stream's records with exact numbers.
func records(t *testing.T, out *testkit.Output, name string) []map[string]any {
	t.Helper()
	var recs []map[string]any
	for _, r := range out.Records(synclet.StreamDescriptor{Name: name}) {
		recs = append(recs, decodeJSON(t, r.Data))
	}
	return recs
}

func decodeJSON(t *testing.T, b []byte) map[string]any {
	t.Helper()
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	var m map[string]any
	require.NoError(t, dec.Decode(&m))
	return m
}

// stripMetadata drops the metadata fields, returning the values of
// _synclet_source_row.
func stripMetadata(recs []map[string]any) ([]map[string]any, []string) {
	var rows []string
	out := make([]map[string]any, len(recs))
	for i, r := range recs {
		c := map[string]any{}
		for k, v := range r {
			if !slices.Contains(metadataFields, k) {
				c[k] = v
			}
		}
		rows = append(rows, string(r[fieldRow].(json.Number)))
		out[i] = c
	}
	return out, rows
}

// requireSameRecords compares records with numbers as rationals, so
// 0.500 equals 0.5.
func requireSameRecords(t *testing.T, want, got []map[string]any) {
	t.Helper()
	require.Len(t, got, len(want))
	for i := range want {
		require.Equal(t, normalize(t, want[i]), normalize(t, got[i]), "record %d", i)
	}
}

func normalize(t *testing.T, v any) any {
	switch v := v.(type) {
	case json.Number:
		r, ok := new(big.Rat).SetString(string(v))
		require.True(t, ok, "number %q", v)
		return "number:" + r.RatString()
	case map[string]any:
		m := make(map[string]any, len(v))
		for k, x := range v {
			m[k] = normalize(t, x)
		}
		return m
	case []any:
		s := make([]any, len(v))
		for i, x := range v {
			s[i] = normalize(t, x)
		}
		return s
	}
	return v
}
