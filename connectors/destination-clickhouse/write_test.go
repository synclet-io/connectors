package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
	"github.com/synclet-io/synclet-go-sdk/testkit"
)

func readTestdata(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("testdata", name))
	require.NoError(t, err)
	return b
}

func runTestdata(t *testing.T, cfg Config, catalogFile, messagesFile string) *testkit.Output {
	t.Helper()
	config, err := json.Marshal(cfg)
	require.NoError(t, err)
	out, err := testkit.RunDestination(t.Context(), NewDestination(), synclet.Request{
		Command: synclet.CommandWrite,
		Config:  config,
		Catalog: readTestdata(t, catalogFile),
		Input:   bytes.NewReader(readTestdata(t, messagesFile)),
	})
	require.NoError(t, err)
	return out
}

func columnTypes(t *testing.T, cfg Config, table string) map[string]string {
	t.Helper()
	got := map[string]string{}
	for _, c := range queryStrings(t, cfg, `SELECT name, type FROM system.columns WHERE database = ? AND table = ?`, cfg.Database, table) {
		got[c[0]] = c[1]
	}
	return got
}

func TestWriteAllTypes(t *testing.T) {
	for _, protocol := range []string{protocolNative, protocolHTTP} {
		t.Run(protocol, func(t *testing.T) {
			cfg := sharedClickHouse(t, "all_types_"+protocol)
			if protocol == protocolHTTP {
				cfg = overHTTP(cfg)
			}
			out := runTestdata(t, cfg, "catalog.json", "messages.jsonl")
			require.NoError(t, out.Err)
			assert.Equal(t, []string{`{"cursor":2}`}, stateValues(out))

			assert.Equal(t, map[string]string{
				"_airbyte_raw_id":       "UUID",
				"_airbyte_extracted_at": "DateTime64(6, 'UTC')",
				"id":                    "Nullable(Int64)",
				"bool_col":              "Nullable(Bool)",
				"int_col":               "Nullable(Int64)",
				"airbyte_int_col":       "Nullable(Int64)",
				"num_col":               "Nullable(Decimal(76, 38))",
				"big_int_col":           "Nullable(Decimal(76, 0))",
				"str_col":               "Nullable(String)",
				"date_col":              "Nullable(Date32)",
				"ts_col":                "Nullable(DateTime64(6, 'UTC'))",
				"ts_ntz_col":            "Nullable(DateTime64(6, 'UTC'))",
				"time_col":              "Nullable(String)",
				"timetz_col":            "Nullable(String)",
				"obj_col":               "Nullable(String)",
				"arr_col":               "Nullable(String)",
				"union_col":             "Nullable(String)",
				"untyped_col":           "Nullable(String)",
			}, columnTypes(t, cfg, "all_types"))

			rows := queryStrings(t, cfg, `SELECT toString(num_col), toString(num_col = toDecimal256('1e-30', 38)), toString(big_int_col), toString(int_col),
  str_col, toString(date_col), toString(ts_col), toString(ts_ntz_col), time_col, timetz_col,
  obj_col, arr_col, union_col, untyped_col, toString(bool_col), toString(_airbyte_extracted_at), toString(airbyte_int_col)
FROM `+quote(cfg.Database, "all_types")+` ORDER BY id`)
			require.Len(t, rows, 2)
			assert.Equal(t, []string{
				"12345678901234567890.123456789012345678", "0", "123456789012345678901234567890", "-9223372036854775808",
				"tab\there, \"quoted\"\nnewline \\ backslash \x00 nul", "2024-02-29", "2024-02-29 11:14:15.123456", "2024-02-29 13:14:15.000000",
				"13:14:15", "13:14:15+02:00",
				`{"a":1,"nested":{"n":99999999999999999999.5}}`, "[1.10,2]", `"x"`, `{"k":"v"}`, "true",
				"2023-11-14 22:13:20.000000", "42",
			}, rows[0])
			assert.Equal(t, []string{
				"0.000000000000000000000000000001", "1", "-1", "0",
				"", "1900-01-01", "1970-01-01 00:00:00.000000", "2262-04-11 23:47:16.854775",
				"00:00:00", "00:00:00Z",
				"{}", "[]", "7", `"s"`, "<NULL>",
				"2023-11-14 22:13:20.000000", "0",
			}, rows[1])

			ids := queryStrings(t, cfg, `SELECT toString(count()), toString(uniqExact(_airbyte_raw_id)) FROM `+quote(cfg.Database, "all_types"))
			assert.Equal(t, [][]string{{"2", "2"}}, ids)
		})
	}
}

func TestWriteValueThatDoesNotFit(t *testing.T) {
	cfg := sharedClickHouse(t, "no_fit")
	s := stream("nums", `{"type":"object","properties":{"n":{"type":"integer"},"x":{"type":"number"},"d":{"type":"string","format":"date"}}}`,
		synclet.DestinationSyncModeAppend, nil, nil)
	for _, c := range []struct{ data, want string }{
		{`{"n": 99999999999999999999}`, `stream "nums": record 1: column "n": value 99999999999999999999 is outside the Int64 range`},
		{`{"x": "twelve"}`, `column "x": value "twelve" is not a number`},
		{`{"x": 1e40}`, `column "x": value 1e40 has more than 38 digits before the decimal point`},
		{`{"x": 0.1234567890123456789012345678901234567891}`, `more than 38 digits after the decimal point`},
		{`{"d": "1812-06-24"}`, `column "d": value "1812-06-24" is outside the Date32 range`},
		{`[1, 2]`, `stream "nums": record 1: not a JSON object`},
	} {
		out := runWrite(t, NewDestination(), cfg, []synclet.ConfiguredStream{s}, rec("nums", c.data), state("nums", `1`))
		require.Error(t, out.Err, c.data)
		assert.Contains(t, out.Err.Error(), c.want)
		assert.Empty(t, out.States())
	}
	assert.Equal(t, 0, countRows(t, cfg, "nums"))
}

func TestWriteUnknownFields(t *testing.T) {
	cfg := sharedClickHouse(t, "unknown_fields")
	s := stream("items", `{"type":"object","properties":{"id":{"type":"integer"}}}`, synclet.DestinationSyncModeAppend, nil, nil)
	out := runWrite(t, NewDestination(), cfg, []synclet.ConfiguredStream{s},
		rec("items", `{"id":1,"extra":"x"}`), rec("items", `{"id":2,"other":1,"extra":"y"}`))
	require.NoError(t, out.Err)
	var warns []string
	for _, l := range out.Logs() {
		if l.Level == synclet.LogLevelWarn {
			warns = append(warns, l.Message)
		}
	}
	require.Len(t, warns, 1)
	assert.Contains(t, warns[0], `stream "items"`)
	assert.Contains(t, warns[0], "extra, other")
	assert.Equal(t, 2, countRows(t, cfg, "items"))

	t.Run("new property adds a Nullable column", func(t *testing.T) {
		s := stream("items", `{"type":"object","properties":{"id":{"type":"integer"},"extra":{"type":"string"}}}`, synclet.DestinationSyncModeAppend, nil, nil)
		out := runWrite(t, NewDestination(), cfg, []synclet.ConfiguredStream{s}, rec("items", `{"id":3,"extra":"z"}`))
		require.NoError(t, out.Err)
		rows := queryStrings(t, cfg, "SELECT toString(id), extra FROM unknown_fields.items ORDER BY id")
		assert.Equal(t, [][]string{{"1", "<NULL>"}, {"2", "<NULL>"}, {"3", "z"}}, rows)
		assert.Equal(t, "Nullable(String)", columnTypes(t, cfg, "items")["extra"])
	})
	t.Run("changed column type fails", func(t *testing.T) {
		s := stream("items", `{"type":"object","properties":{"id":{"type":"string"}}}`, synclet.DestinationSyncModeAppend, nil, nil)
		out := runWrite(t, NewDestination(), cfg, []synclet.ConfiguredStream{s}, rec("items", `{"id":"a"}`))
		require.Error(t, out.Err)
		assert.Contains(t, out.Err.Error(), "column \"id\" of table `unknown_fields`.`items` is Nullable(Int64), the catalog needs Nullable(String)")
	})
}

func TestWriteLongNames(t *testing.T) {
	cfg := sharedClickHouse(t, "long_names")
	name := strings.Repeat("stream_with_a_very_long_name_", 4)[:100]
	field := strings.Repeat("field_with_a_very_long_name_", 4)[:100]
	s := stream(name, `{"type":"object","properties":{"`+field+`":{"type":"string"}}}`, synclet.DestinationSyncModeAppend, nil, nil)
	for i := range 2 {
		out := runWrite(t, NewDestination(), cfg, []synclet.ConfiguredStream{s}, rec(name, `{"`+field+`":"v"}`))
		require.NoError(t, out.Err, "sync %d", i+1)
	}
	table, column := chIdent(name), chIdent(field)
	assert.Len(t, table, 64)
	assert.Equal(t, 2, countRows(t, cfg, table))
	rows := queryStrings(t, cfg, `SELECT table, name FROM system.columns WHERE database = ? AND name NOT LIKE '\_airbyte%'`, cfg.Database)
	assert.Equal(t, [][]string{{table, column}}, rows)

	t.Run("two streams on one table fail before writing", func(t *testing.T) {
		a := stream("dup", `{"type":"object","properties":{}}`, synclet.DestinationSyncModeAppend, nil, nil)
		b := a
		b.Stream.Namespace = "other"
		out := runWrite(t, NewDestination(), cfg, []synclet.ConfiguredStream{a, b})
		require.Error(t, out.Err)
		assert.Contains(t, out.Err.Error(), `both map to "dup"`)
		assert.False(t, tableExists(t, cfg, "dup"))
	})
}

func TestWriteOverwrite(t *testing.T) {
	cfg := sharedClickHouse(t, "overwrite")
	s := stream("rates", `{"type":"object","properties":{"code":{"type":"string"},"day":{"type":"string","format":"date"}}}`,
		synclet.DestinationSyncModeOverwrite, nil, nil)
	write := func(lines ...string) *testkit.Output {
		return runWrite(t, testDestination(2), cfg, []synclet.ConfiguredStream{s}, lines...)
	}
	codes := func() [][]string {
		return queryStrings(t, cfg, "SELECT code FROM overwrite.rates ORDER BY code")
	}

	out := write(rec("rates", `{"code":"a"}`), rec("rates", `{"code":"b"}`), rec("rates", `{"code":"c"}`), state("rates", `1`))
	require.NoError(t, out.Err)
	assert.Equal(t, []string{"1"}, stateValues(out))
	assert.Equal(t, 3, countRows(t, cfg, "rates"))

	out = write(rec("rates", `{"code":"d"}`), rec("rates", `{"code":"e"}`), state("rates", `2`))
	require.NoError(t, out.Err)
	assert.Equal(t, []string{"2"}, stateValues(out))
	assert.Equal(t, [][]string{{"d"}, {"e"}}, codes())
	assert.False(t, tableExists(t, cfg, "_synclet_tmp_rates"))

	out = write(rec("rates", `{"code":"f"}`), rec("rates", `{"code":"g"}`), state("rates", `3`), rec("rates", `{"code":"h","day":"not-a-date"}`))
	require.Error(t, out.Err)
	assert.Empty(t, out.States())
	assert.Equal(t, [][]string{{"d"}, {"e"}}, codes())
	assert.False(t, tableExists(t, cfg, "_synclet_tmp_rates"))

	t.Run("leftover shadow table is replaced", func(t *testing.T) {
		execSQL(t, cfg, "CREATE TABLE overwrite._synclet_tmp_rates (junk String) ENGINE = MergeTree ORDER BY tuple()")
		execSQL(t, cfg, "INSERT INTO overwrite._synclet_tmp_rates VALUES ('x')")
		out := write(rec("rates", `{"code":"i"}`))
		require.NoError(t, out.Err)
		assert.Equal(t, [][]string{{"i"}}, codes())
	})
}

func TestWriteAppend(t *testing.T) {
	cfg := sharedClickHouse(t, "append")
	s := stream("events", `{"type":"object","properties":{"id":{"type":"integer"}}}`, synclet.DestinationSyncModeAppend, nil, nil)
	for range 2 {
		out := runWrite(t, testDestination(1), cfg, []synclet.ConfiguredStream{s}, rec("events", `{"id":1}`), rec("events", `{"id":2}`), state("events", `1`))
		require.NoError(t, out.Err)
		assert.Equal(t, []string{"1"}, stateValues(out))
	}
	assert.Equal(t, 4, countRows(t, cfg, "events"))
	assert.Equal(t, [][]string{{"MergeTree", ""}},
		queryStrings(t, cfg, `SELECT engine, sorting_key FROM system.tables WHERE database = 'append' AND name = 'events'`))
}

func TestWriteDedup(t *testing.T) {
	cfg := sharedClickHouse(t, "dedup")
	s := stream("users", `{"type":"object","properties":{"id":{"type":"integer"},"v":{"type":"string"},"c":{"type":"integer"}}}`,
		synclet.DestinationSyncModeAppendDedup, [][]string{{"id"}}, []string{"c"})
	write := func(lines ...string) *testkit.Output {
		return runWrite(t, testDestination(3), cfg, []synclet.ConfiguredStream{s}, lines...)
	}
	users := func() [][]string {
		return queryStrings(t, cfg, "SELECT toString(id), v, toString(c) FROM dedup.users FINAL ORDER BY id")
	}

	out := write(
		rec("users", `{"id":1,"v":"1@1","c":1}`), rec("users", `{"id":2,"v":"2@5","c":5}`), rec("users", `{"id":1,"v":"1@3","c":3}`),
		rec("users", `{"id":1,"v":"1@2","c":2}`), rec("users", `{"id":2,"v":"2@6","c":6}`), rec("users", `{"id":3,"v":"3@1","c":1}`),
		state("users", `1`),
	)
	require.NoError(t, out.Err)
	assert.Equal(t, [][]string{{"1", "1@3", "3"}, {"2", "2@6", "6"}, {"3", "3@1", "1"}}, users())
	assert.Equal(t, [][]string{{"ReplacingMergeTree", "id"}},
		queryStrings(t, cfg, `SELECT engine, sorting_key FROM system.tables WHERE database = 'dedup' AND name = 'users'`))
	assert.Equal(t, "Int64", columnTypes(t, cfg, "users")["id"])
	assert.Equal(t, "Int64", columnTypes(t, cfg, "users")["c"])
	assert.Equal(t, "Nullable(String)", columnTypes(t, cfg, "users")["v"])

	out = write(rec("users", `{"id":1,"v":"1@9","c":9}`), rec("users", `{"id":3,"v":"3@0","c":0}`), rec("users", `{"id":2,"v":"2@6 again","c":6}`))
	require.NoError(t, out.Err)
	assert.Equal(t, [][]string{{"1", "1@9", "9"}, {"2", "2@6 again", "6"}, {"3", "3@1", "1"}}, users())

	t.Run("without cursor the last arrival wins", func(t *testing.T) {
		s := stream("plain", `{"type":"object","properties":{"id":{"type":"integer"},"v":{"type":"string"}}}`,
			synclet.DestinationSyncModeAppendDedup, [][]string{{"id"}}, nil)
		out := runWrite(t, testDestination(3), cfg, []synclet.ConfiguredStream{s},
			rec("plain", `{"id":1,"v":"a"}`), rec("plain", `{"id":1,"v":"b"}`), rec("plain", `{"id":1,"v":"c"}`),
			rec("plain", `{"id":2,"v":"d"}`), rec("plain", `{"id":2,"v":"e"}`))
		require.NoError(t, out.Err)
		assert.Equal(t, [][]string{{"1", "c"}, {"2", "e"}}, queryStrings(t, cfg, "SELECT toString(id), v FROM dedup.plain FINAL ORDER BY id"))
	})
	t.Run("timestamp cursor and composite key", func(t *testing.T) {
		s := stream("ts", `{"type":"object","properties":{"a":{"type":"string"},"b":{"type":"integer"},"v":{"type":"string"},"u":{"type":"string","format":"date-time"}}}`,
			synclet.DestinationSyncModeAppendDedup, [][]string{{"a"}, {"b"}}, []string{"u"})
		out := runWrite(t, NewDestination(), cfg, []synclet.ConfiguredStream{s},
			rec("ts", `{"a":"x","b":1,"v":"new","u":"2024-01-02T00:00:00Z"}`),
			rec("ts", `{"a":"x","b":1,"v":"old","u":"2024-01-01T00:00:00Z"}`),
			rec("ts", `{"a":"x","b":2,"v":"other","u":"2024-01-01T00:00:00Z"}`))
		require.NoError(t, out.Err)
		assert.Equal(t, [][]string{{"x", "1", "new"}, {"x", "2", "other"}}, queryStrings(t, cfg, "SELECT a, toString(b), v FROM dedup.ts FINAL ORDER BY a, b"))
	})
	t.Run("optimize_final leaves one row per key without FINAL", func(t *testing.T) {
		s := stream("opt", `{"type":"object","properties":{"id":{"type":"integer"},"c":{"type":"integer"}}}`,
			synclet.DestinationSyncModeAppendDedup, [][]string{{"id"}}, []string{"c"})
		opt := cfg
		opt.OptimizeFinal = true
		for i := range 2 {
			out := runWrite(t, testDestination(1), opt, []synclet.ConfiguredStream{s},
				rec("opt", `{"id":1,"c":1}`), rec("opt", `{"id":1,"c":2}`), rec("opt", `{"id":2,"c":1}`), state("opt", `"s"`))
			require.NoError(t, out.Err, "sync %d", i+1)
			assert.Equal(t, []string{`"s"`}, stateValues(out))
		}
		assert.Equal(t, [][]string{{"1", "2"}, {"2", "1"}}, queryStrings(t, cfg, "SELECT toString(id), toString(c) FROM dedup.opt ORDER BY id"))
	})
	t.Run("NULL primary key fails", func(t *testing.T) {
		out := write(rec("users", `{"id":null,"v":"x","c":1}`))
		require.Error(t, out.Err)
		assert.Contains(t, out.Err.Error(), `stream "users": record 1: primary key column "id" is null`)
	})
	t.Run("NULL cursor fails", func(t *testing.T) {
		out := write(rec("users", `{"id":7,"v":"x"}`))
		require.Error(t, out.Err)
		assert.Contains(t, out.Err.Error(), `stream "users": record 1: cursor column "c" is null`)
	})
	t.Run("string cursor fails before writing", func(t *testing.T) {
		s := stream("strcur", `{"type":"object","properties":{"id":{"type":"integer"},"c":{"type":"string"}}}`,
			synclet.DestinationSyncModeAppendDedup, [][]string{{"id"}}, []string{"c"})
		out := runWrite(t, NewDestination(), cfg, []synclet.ConfiguredStream{s}, rec("strcur", `{"id":1,"c":"a"}`))
		require.Error(t, out.Err)
		assert.Contains(t, out.Err.Error(), `cursor field "c" is String`)
		assert.False(t, tableExists(t, cfg, "strcur"))
	})
	t.Run("table with another engine fails", func(t *testing.T) {
		execSQL(t, cfg, "CREATE TABLE dedup.merge (id Int64, _airbyte_raw_id UUID, _airbyte_extracted_at DateTime64(6, 'UTC')) ENGINE = MergeTree ORDER BY id")
		s := stream("merge", `{"type":"object","properties":{"id":{"type":"integer"}}}`,
			synclet.DestinationSyncModeAppendDedup, [][]string{{"id"}}, nil)
		out := runWrite(t, NewDestination(), cfg, []synclet.ConfiguredStream{s}, rec("merge", `{"id":1}`))
		require.Error(t, out.Err)
		assert.Contains(t, out.Err.Error(), "table `dedup`.`merge` is a MergeTree table; append_dedup needs a ReplacingMergeTree")
	})
	t.Run("table with another version fails", func(t *testing.T) {
		s := stream("users", `{"type":"object","properties":{"id":{"type":"integer"},"v":{"type":"string"},"c":{"type":"integer"}}}`,
			synclet.DestinationSyncModeAppendDedup, [][]string{{"id"}}, nil)
		out := runWrite(t, NewDestination(), cfg, []synclet.ConfiguredStream{s}, rec("users", `{"id":1}`))
		require.Error(t, out.Err)
		assert.Contains(t, out.Err.Error(), "table `dedup`.`users` has version column \"c\"; append_dedup needs the cursor \"\"")
	})
	t.Run("table with another sorting key fails", func(t *testing.T) {
		s := stream("users", `{"type":"object","properties":{"id":{"type":"integer"},"v":{"type":"string"},"c":{"type":"integer"}}}`,
			synclet.DestinationSyncModeAppendDedup, [][]string{{"v"}}, []string{"c"})
		out := runWrite(t, NewDestination(), cfg, []synclet.ConfiguredStream{s}, rec("users", `{"id":1,"v":"a","c":1}`))
		require.Error(t, out.Err)
		assert.Contains(t, out.Err.Error(), "table `dedup`.`users` is sorted by (id); append_dedup needs it sorted by the primary key (v)")
	})
}

// countingWriter parses the protocol stream and, on every STATE, counts the
// rows of a table from a second connection.
type countingWriter struct {
	t      *testing.T
	cfg    Config
	tables map[string]string
	mu     sync.Mutex
	buf    bytes.Buffer
	seen   []string
}

func (w *countingWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.buf.Write(p)
	sc := bufio.NewScanner(bytes.NewReader(w.buf.Bytes()))
	w.buf.Reset()
	for sc.Scan() {
		var m synclet.Message
		require.NoError(w.t, json.Unmarshal(sc.Bytes(), &m))
		if m.Type != synclet.MessageTypeState {
			continue
		}
		name := m.State.Stream.StreamDescriptor.Name
		rows := queryStrings(w.t, w.cfg, "SELECT toString(count()) FROM "+quote(w.cfg.Database, w.tables[name]))
		w.seen = append(w.seen, string(m.State.Stream.StreamState)+"="+rows[0][0])
	}
	return len(p), nil
}

func TestWriteStateAfterInsert(t *testing.T) {
	cfg := sharedClickHouse(t, "state_insert")
	app := stream("app", `{"type":"object","properties":{"id":{"type":"integer"}}}`, synclet.DestinationSyncModeAppend, nil, nil)
	ovr := stream("ovr", `{"type":"object","properties":{"id":{"type":"integer"}}}`, synclet.DestinationSyncModeOverwrite, nil, nil)
	d := NewDestination()
	d.maxAge = 0

	input := strings.Join([]string{
		rec("app", `{"id":1}`), rec("app", `{"id":2}`), state("app", `"a1"`),
		rec("ovr", `{"id":1}`), state("ovr", `"o1"`),
		rec("app", `{"id":3}`), state("app", `"a2"`),
		rec("ovr", `{"id":2}`), state("ovr", `"o2"`),
		rec("app", `{"id":4}`), rec("app", `{"id":5}`), rec("app", `{"id":6}`), state("app", `"a3"`),
	}, "\n")
	w := &countingWriter{t: t, cfg: cfg, tables: map[string]string{"app": "app", "ovr": "ovr"}}
	cfg.applyDefaults()
	err := d.Write(context.Background(), cfg, &synclet.ConfiguredCatalog{Streams: []synclet.ConfiguredStream{app, ovr}},
		synclet.NewMessageReader(strings.NewReader(input)), synclet.NewEmitter(w))
	require.NoError(t, err)
	assert.Equal(t, []string{`"a1"=2`, `"a2"=3`, `"a3"=6`, `"o1"=2`, `"o2"=2`}, w.seen)
}

func TestWriteStateOneRowBatches(t *testing.T) {
	cfg := sharedClickHouse(t, "state_rows")
	s := stream("app", `{"type":"object","properties":{"id":{"type":"integer"}}}`, synclet.DestinationSyncModeAppend, nil, nil)
	input := strings.Join([]string{
		state("app", `"s0"`), rec("app", `{"id":1}`), state("app", `"s1"`), rec("app", `{"id":2}`), rec("app", `{"id":3}`), state("app", `"s3"`),
	}, "\n")
	w := &countingWriter{t: t, cfg: cfg, tables: map[string]string{"app": "app"}}
	cfg.applyDefaults()
	err := testDestination(1).Write(context.Background(), cfg, &synclet.ConfiguredCatalog{Streams: []synclet.ConfiguredStream{s}},
		synclet.NewMessageReader(strings.NewReader(input)), synclet.NewEmitter(w))
	require.NoError(t, err)
	assert.Equal(t, []string{`"s0"=0`, `"s1"=1`, `"s3"=3`}, w.seen)
}

func TestWriteStateFailMidBatch(t *testing.T) {
	cfg := sharedClickHouse(t, "fail_mid_batch")
	s := stream("orders", `{"type":"object","properties":{"id":{"type":"integer"},"placed_on":{"type":"string","format":"date"}}}`,
		synclet.DestinationSyncModeAppend, nil, nil)
	out := runWrite(t, testDestination(2), cfg, []synclet.ConfiguredStream{s},
		rec("orders", `{"id":1,"placed_on":"2024-01-01"}`),
		rec("orders", `{"id":2,"placed_on":"2024-01-02"}`),
		state("orders", `"s1"`),
		rec("orders", `{"id":3,"placed_on":"2024-01-03"}`),
		rec("orders", `{"id":4,"placed_on":"2024-13-45"}`),
		state("orders", `"s2"`),
	)
	require.Error(t, out.Err)
	assert.Equal(t, []string{`"s1"`}, stateValues(out))
	assert.Equal(t, 2, countRows(t, cfg, "orders"))
	traces := out.ErrorTraces()
	require.Len(t, traces, 1)
	assert.Contains(t, traces[0].Message, `stream "orders": record 4: column "placed_on"`)
}

// TestWriteStateServerRejectsInsert drops the target table while the sync
// holds a batch, so the server fails the INSERT: no state is echoed for the
// batch.
func TestWriteStateServerRejectsInsert(t *testing.T) {
	cfg := sharedClickHouse(t, "server_rejects")
	s := stream("orders", `{"type":"object","properties":{"id":{"type":"integer"}}}`, synclet.DestinationSyncModeAppend, nil, nil)
	pr, pw := io.Pipe()
	done := make(chan error, 1)
	w := &countingWriter{t: t, cfg: cfg, tables: map[string]string{"orders": "orders"}}
	cfg.applyDefaults()
	go func() {
		done <- testDestination(2).Write(context.Background(), cfg, &synclet.ConfiguredCatalog{Streams: []synclet.ConfiguredStream{s}},
			synclet.NewMessageReader(pr), synclet.NewEmitter(w))
	}()
	write := func(lines ...string) {
		_, err := pw.Write([]byte(strings.Join(lines, "\n") + "\n"))
		require.NoError(t, err)
	}
	write(rec("orders", `{"id":1}`), rec("orders", `{"id":2}`), state("orders", `"s1"`))
	require.Eventually(t, func() bool {
		w.mu.Lock()
		defer w.mu.Unlock()
		return len(w.seen) == 1
	}, 30*time.Second, 50*time.Millisecond)
	write(rec("orders", `{"id":3}`))
	execSQL(t, cfg, "DROP TABLE server_rejects.orders SYNC")
	write(rec("orders", `{"id":4}`), state("orders", `"s2"`))
	require.NoError(t, pw.Close())
	err := <-done
	require.Error(t, err)
	assert.Contains(t, err.Error(), `stream "orders"`)
	assert.Equal(t, []string{`"s1"=2`}, w.seen)
}

// stateKinds lists the echoed states in order: the stream_state of a STREAM
// state, "global" and "legacy" for the others.
func stateKinds(out *testkit.Output) []string {
	var kinds []string
	for _, st := range out.States() {
		switch st.Type {
		case synclet.StateTypeStream:
			kinds = append(kinds, string(st.Stream.StreamState))
		case synclet.StateTypeGlobal:
			kinds = append(kinds, "global")
		default:
			kinds = append(kinds, "legacy")
		}
	}
	return kinds
}

func TestWriteStateGlobalWaitsForSwap(t *testing.T) {
	cfg := sharedClickHouse(t, "global_state")
	streams := []synclet.ConfiguredStream{
		stream("rates", `{"type":"object","properties":{"code":{"type":"string"}}}`, synclet.DestinationSyncModeOverwrite, nil, nil),
		stream("logs", `{"type":"object","properties":{"day":{"type":"string","format":"date"}}}`, synclet.DestinationSyncModeAppend, nil, nil),
	}
	global := `{"type":"STATE","state":{"type":"GLOBAL","global":{"shared_state":{"lsn":1},"stream_states":[]}}}`
	legacy := `{"type":"STATE","state":{"type":"LEGACY","data":{"lsn":2}}}`
	write := func(lines ...string) *testkit.Output {
		return runWrite(t, testDestination(2), cfg, streams, lines...)
	}

	out := write(
		rec("rates", `{"code":"a"}`), rec("rates", `{"code":"b"}`), global,
		rec("logs", `{"day":"2024-01-01"}`), rec("logs", `{"day":"2024-01-02"}`), legacy, state("logs", `1`),
	)
	require.NoError(t, out.Err)
	assert.Equal(t, []string{"1", "global", "legacy"}, stateKinds(out))
	assert.Equal(t, 2, countRows(t, cfg, "rates"))

	out = write(
		rec("rates", `{"code":"c"}`), rec("rates", `{"code":"d"}`), global,
		rec("logs", `{"day":"2024-01-03"}`), rec("logs", `{"day":"2024-01-04"}`), legacy, state("logs", `2`),
		rec("logs", `{"day":"not-a-date"}`),
	)
	require.Error(t, out.Err)
	assert.Equal(t, []string{"2"}, stateKinds(out))
	assert.Equal(t, [][]string{{"a"}, {"b"}}, queryStrings(t, cfg, "SELECT code FROM global_state.rates ORDER BY code"))
}
