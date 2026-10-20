package main

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
)

// datasetStreams puts the dataset in every format, plain, gzip and zstd,
// and returns one stream per format.
func datasetStreams(t *testing.T, store *memStore) []StreamConfig {
	t.Helper()
	files := map[string][]byte{
		"csv/data.csv":         []byte(datasetCSV),
		"tsv/data.tsv":         []byte(datasetTSV),
		"jsonl/data.jsonl":     []byte(datasetJSONL),
		"parquet/data.parquet": datasetParquet(t),
	}
	for key, b := range files {
		store.put(key, b, t0)
		store.put(key+".gz", gzipped(t, b), t0)
		store.put(key+".zst", zstded(t, b), t0)
	}
	return []StreamConfig{
		{Name: "csv", Glob: "csv/*", Format: Format{Filetype: formatCSV}},
		{Name: "tsv", Glob: "tsv/*", Format: Format{Filetype: formatTSV, QuoteChar: ptr(`"`)}},
		{Name: "jsonl", Glob: "jsonl/*", Format: Format{Filetype: formatJSONL}},
		{Name: "parquet", Glob: "parquet/*", Format: Format{Filetype: formatParquet}},
	}
}

func TestFormat(t *testing.T) {
	store := newMemStore()
	cfg := testConfig(datasetStreams(t, store)...)
	src := Source{store: store}
	streams := discoverStreams(t, src, cfg)
	for _, name := range []string{"csv", "tsv", "jsonl", "parquet"} {
		t.Run(name, func(t *testing.T) {
			s := streams[name]
			require.JSONEq(t, wantDatasetSchema, string(s.JSONSchema))
			require.True(t, s.SourceDefinedCursor)
			require.Equal(t, []string{fieldModified}, s.DefaultCursorField)
			require.Equal(t, [][]string{{fieldFile}, {fieldRow}}, s.SourceDefinedPrimaryKey)

			out := runRead(t, src, cfg, catalogOf(synclet.SyncModeFullRefresh, s), nil)
			require.NoError(t, out.Err)
			recs := records(t, out, name)
			require.Len(t, recs, 3*len(wantDataset))
			for i, ext := range []string{"", ".gz", ".zst"} {
				file := recs[i*len(wantDataset) : (i+1)*len(wantDataset)]
				data, rows := stripMetadata(file)
				requireSameRecords(t, wantDataset, data)
				require.Equal(t, []string{"1", "2", "3", "4"}, rows)
				for _, r := range file {
					require.True(t, strings.HasSuffix(r[fieldFile].(string), ext), r[fieldFile])
					require.Equal(t, "2026-10-01T12:00:00Z", r[fieldModified])
				}
			}
			// The decimal keeps every digit, not a float64 rounding.
			require.Equal(t, json.Number(bigDecimal), recs[0]["amount"])
		})
	}
}

func TestCompression(t *testing.T) {
	store := newMemStore()
	store.put("auto/plain", []byte(datasetCSV), t0)
	store.put("auto/gz-no-ext", gzipped(t, []byte(datasetCSV)), t0)
	store.put("auto/zst-no-ext", zstded(t, []byte(datasetCSV)), t0)
	store.put("forced/data.bin", gzipped(t, []byte(datasetCSV)), t0)
	store.put("none/data.csv.gz", []byte(datasetCSV), t0)
	src := Source{store: store}
	cfg := testConfig(
		StreamConfig{Name: "auto", Glob: "auto/*", Format: Format{Filetype: formatCSV}},
		StreamConfig{Name: "forced", Glob: "forced/*", Format: Format{Filetype: formatCSV}, Compression: compressionGzip},
		StreamConfig{Name: "none", Glob: "none/*", Format: Format{Filetype: formatCSV}, Compression: compressionNone},
	)
	streams := discoverStreams(t, src, cfg)
	for name, files := range map[string]int{"auto": 3, "forced": 1, "none": 1} {
		out := runRead(t, src, cfg, catalogOf(synclet.SyncModeFullRefresh, streams[name]), nil)
		require.NoError(t, out.Err, name)
		recs := records(t, out, name)
		require.Len(t, recs, files*len(wantDataset), name)
		for i := range files {
			data, _ := stripMetadata(recs[i*len(wantDataset) : (i+1)*len(wantDataset)])
			requireSameRecords(t, wantDataset, data)
		}
	}

	require.Equal(t, compressionGzip, detectCompression(compressionAuto, "a/b.CSV.GZ", nil))
	require.Equal(t, compressionZstd, detectCompression(compressionAuto, "a/b.zstd", nil))
	require.Equal(t, compressionGzip, detectCompression(compressionAuto, "a/b", gzipMagic))
	require.Equal(t, compressionZstd, detectCompression(compressionAuto, "a/b", zstdMagic))
	require.Equal(t, compressionNone, detectCompression(compressionAuto, "a/b.csv", []byte("id,")))
	require.Equal(t, compressionNone, detectCompression(compressionNone, "a/b.gz", gzipMagic))
}

// A TSV without quoting keeps quote characters as data.
func TestFormatTSVUnquoted(t *testing.T) {
	store := newMemStore()
	store.put("t/a.tsv", []byte("id\tquote\tescaped\n1\t\"x\"\ta\\\tb\n2\t\t\n"), t0)
	src := Source{store: store}
	cfg := testConfig(StreamConfig{Name: "t", Glob: "t/*.tsv", Format: Format{Filetype: formatTSV, EscapeChar: `\`}})
	s := discoverStreams(t, src, cfg)["t"]
	out := runRead(t, src, cfg, catalogOf(synclet.SyncModeFullRefresh, s), nil)
	require.NoError(t, out.Err)
	data, _ := stripMetadata(records(t, out, "t"))
	requireSameRecords(t, []map[string]any{
		{"id": json.Number("1"), "quote": `"x"`, "escaped": "a\tb"},
		{"id": json.Number("2"), "quote": nil, "escaped": nil},
	}, data)
}

func TestFormatCSVOptions(t *testing.T) {
	store := newMemStore()
	store.put("o/a.csv", []byte("# exported 2026-10-01\n1;'it''s';NA;'NA'\r\n2;'a\\;b';;x\r\n"), t0)
	src := Source{store: store}
	cfg := testConfig(StreamConfig{Name: "o", Glob: "o/*.csv", Format: Format{
		Filetype: formatCSV, Delimiter: ptr(";"), QuoteChar: ptr("'"), EscapeChar: `\`,
		Header: ptr(false), ColumnNames: []string{"id", "text", "maybe", "other"}, SkipRows: 1,
		NullValues: &[]string{"NA", ""},
	}})
	s := discoverStreams(t, src, cfg)["o"]
	require.JSONEq(t, `{"type":["null","string"]}`, string(propertyOf(t, s, "maybe")))
	out := runRead(t, src, cfg, catalogOf(synclet.SyncModeFullRefresh, s), nil)
	require.NoError(t, out.Err)
	data, _ := stripMetadata(records(t, out, "o"))
	requireSameRecords(t, []map[string]any{
		{"id": json.Number("1"), "text": "it's", "maybe": nil, "other": "NA"},
		{"id": json.Number("2"), "text": "a;b", "maybe": nil, "other": "x"},
	}, data)
}

func propertyOf(t *testing.T, s synclet.Stream, name string) json.RawMessage {
	t.Helper()
	var schema struct {
		Properties map[string]json.RawMessage `json:"properties"`
	}
	require.NoError(t, json.Unmarshal(s.JSONSchema, &schema))
	return schema.Properties[name]
}

// Inference widens CSV columns and unions JSONL types across files; a given
// schema replaces inference.
func TestSchemaInference(t *testing.T) {
	store := newMemStore()
	store.put("w/1.csv", []byte("a,b,c,d\n1,true,1,x\n"), t0)
	store.put("w/2.csv", []byte("a,b,c,d\n2.5,1,,\n"), t0.Add(1))
	store.put("j/1.jsonl", []byte(`{"a":1,"b":"x","n":{"k":[1,2]}}`+"\n"), t0)
	store.put("j/2.jsonl", []byte(`{"a":1.5,"b":2,"l":[1]}`+"\n"), t0.Add(1))
	src := Source{store: store}
	given := `{"type":"object","properties":{"a":{"type":"string","description":"kept"}}}`
	cfg := testConfig(
		StreamConfig{Name: "w", Glob: "w/*", Format: Format{Filetype: formatCSV}},
		StreamConfig{Name: "j", Glob: "j/*", Format: Format{Filetype: formatJSONL}},
		StreamConfig{Name: "given", Glob: "nothing/*", Format: Format{Filetype: formatCSV}, Schema: given},
		StreamConfig{Name: "newest", Glob: "w/*", Format: Format{Filetype: formatCSV}, SchemaSampleFiles: 1},
	)
	streams := discoverStreams(t, src, cfg)
	for name, want := range map[string]string{"a": `["null","number"]`, "b": `["null","string"]`, "c": `["null","integer"]`, "d": `["null","string"]`} {
		require.JSONEq(t, `{"type":`+want+`}`, string(propertyOf(t, streams["w"], name)), name)
	}
	for name, want := range map[string]string{"a": `["null","number"]`, "b": `["null","integer","string"]`, "n": `["null","object"]`, "l": `["null","array"]`} {
		require.JSONEq(t, `{"type":`+want+`}`, string(propertyOf(t, streams["j"], name)), name)
	}
	require.JSONEq(t, `{"type":"string","description":"kept"}`, string(propertyOf(t, streams["given"], "a")))
	require.NotNil(t, propertyOf(t, streams["given"], fieldModified))
	require.JSONEq(t, `{"type":["null","number"]}`, string(propertyOf(t, streams["newest"], "a")))
	require.JSONEq(t, `{"type":["null","integer"]}`, string(propertyOf(t, streams["newest"], "b")))

	out := runRead(t, src, cfg, catalogOf(synclet.SyncModeFullRefresh, streams["j"]), nil)
	require.NoError(t, out.Err)
	data, _ := stripMetadata(records(t, out, "j"))
	requireSameRecords(t, []map[string]any{
		{"a": json.Number("1"), "b": "x", "n": map[string]any{"k": []any{json.Number("1"), json.Number("2")}}},
		{"a": json.Number("1.5"), "b": json.Number("2"), "l": []any{json.Number("1")}},
	}, data)

	cfg.Streams = cfg.Streams[2:3]
	cfg.Streams[0].Schema = ""
	out = runDiscover(t, src, cfg)
	require.ErrorContains(t, out.Err, `stream "given": no object matches glob "nothing/*"`)
}
