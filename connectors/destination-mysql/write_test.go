package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

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

func TestWriteAllTypes(t *testing.T) {
	cfg := sharedMySQL(t, "all_types")
	out := runTestdata(t, cfg, "catalog.json", "messages.jsonl")
	require.NoError(t, out.Err)
	assert.Equal(t, []string{`{"cursor":2}`}, stateValues(out))

	cols := queryStrings(t, cfg, `SELECT COLUMN_NAME, COLUMN_TYPE FROM information_schema.COLUMNS
WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'all_types'`)
	got := map[string]string{}
	for _, c := range cols {
		got[c[0]] = c[1]
	}
	assert.Equal(t, map[string]string{
		"_airbyte_raw_id":       "char(36)",
		"_airbyte_extracted_at": "datetime(6)",
		"id":                    "bigint",
		"bool_col":              "tinyint(1)",
		"int_col":               "bigint",
		"airbyte_int_col":       "bigint",
		"num_col":               "decimal(65,30)",
		"big_int_col":           "decimal(65,0)",
		"str_col":               "longtext",
		"date_col":              "date",
		"ts_col":                "datetime(6)",
		"ts_ntz_col":            "datetime(6)",
		"time_col":              "time(6)",
		"timetz_col":            "longtext",
		"obj_col":               "json",
		"arr_col":               "json",
		"union_col":             "json",
		"untyped_col":           "json",
	}, got)

	rows := queryStrings(t, cfg, `SELECT num_col, big_int_col, int_col, airbyte_int_col, str_col, date_col, ts_col, ts_ntz_col,
  time_col, timetz_col, bool_col, _airbyte_extracted_at, obj_col, arr_col, union_col, untyped_col
FROM all_types ORDER BY id`)
	require.Len(t, rows, 2)
	assert.Equal(t, []string{
		"12345678901234567890.123456789012345678000000000000", "123456789012345678901234567890", "-9223372036854775808", "42",
		"tab\there, \"quoted\"\nnewline \\ backslash \U0001F600", "2024-02-29", "2024-02-29 11:14:15.123456", "2024-02-29 13:14:15.000000",
		"13:14:15.500000", "13:14:15+02:00", "1", "2023-11-14 22:13:20.000000",
	}, rows[0][:12])
	assert.JSONEq(t, `{"a":1,"nested":{"n":0.5,"s":"é"}}`, rows[0][12])
	assert.JSONEq(t, `[1.1,2]`, rows[0][13])
	assert.JSONEq(t, `"x"`, rows[0][14])
	assert.JSONEq(t, `{"k":"v"}`, rows[0][15])
	assert.Equal(t, []string{
		"0.000000000000000000000000000001", "-1", "0", "0", "", "1000-01-01", "1970-01-01 00:00:00.000000", "1970-01-01 00:00:00.000000",
		"00:00:00.000000", "00:00:00Z", "<NULL>", "2023-11-14 22:13:20.000000",
	}, rows[1][:12])
	assert.JSONEq(t, `{}`, rows[1][12])
	assert.JSONEq(t, `[]`, rows[1][13])
	assert.JSONEq(t, `7`, rows[1][14])
	assert.JSONEq(t, `"s"`, rows[1][15])

	ids := queryStrings(t, cfg, `SELECT COUNT(DISTINCT _airbyte_raw_id) FROM all_types WHERE _airbyte_raw_id REGEXP '^[0-9a-f-]{36}$'`)
	assert.Equal(t, "2", ids[0][0])
}

func TestWriteValueThatDoesNotFit(t *testing.T) {
	cfg := sharedMySQL(t, "no_fit")
	s := stream("nums", `{"type":"object","properties":{"n":{"type":"integer"},"x":{"type":"number"},"j":{}}}`, synclet.DestinationSyncModeAppend, nil, nil)
	for _, c := range []struct{ data, want string }{
		{`{"n": 99999999999999999999}`, `column "n": value 99999999999999999999 is outside the BIGINT range`},
		{`{"x": "twelve"}`, `column "x": value "twelve" is not a number`},
		{`{"x": 1e40}`, `column "x": value "1e40" has more than 35 integer digits`},
		{`{"x": 0.1234567890123456789012345678901}`, `more than 30 fraction digits`},
		{`{"j": {"rate": 0.912345678901234567890}}`, `column "j": number 0.912345678901234567890 in a JSON value would be stored as the double`},
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
	cfg := sharedMySQL(t, "unknown_fields")
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

	t.Run("new property adds a nullable column", func(t *testing.T) {
		s := stream("items", `{"type":"object","properties":{"id":{"type":"integer"},"extra":{"type":"string"}}}`, synclet.DestinationSyncModeAppend, nil, nil)
		out := runWrite(t, NewDestination(), cfg, []synclet.ConfiguredStream{s}, rec("items", `{"id":3,"extra":"z"}`))
		require.NoError(t, out.Err)
		rows := queryStrings(t, cfg, `SELECT id, extra FROM items ORDER BY id`)
		assert.Equal(t, [][]string{{"1", "<NULL>"}, {"2", "<NULL>"}, {"3", "z"}}, rows)
	})
	t.Run("changed column type fails", func(t *testing.T) {
		s := stream("items", `{"type":"object","properties":{"id":{"type":"string"}}}`, synclet.DestinationSyncModeAppend, nil, nil)
		out := runWrite(t, NewDestination(), cfg, []synclet.ConfiguredStream{s}, rec("items", `{"id":"a"}`))
		require.Error(t, out.Err)
		assert.Contains(t, out.Err.Error(), "column \"id\" of table `items` is bigint, the catalog needs longtext")
	})
}

func TestWriteLongNames(t *testing.T) {
	cfg := sharedMySQL(t, "long_names")
	first := strings.Repeat("s", 55) + "_first_stream"
	other := strings.Repeat("s", 55) + "_other_stream"
	field65 := strings.Repeat("f", 65)
	fieldA := strings.Repeat("g", 55) + "_first_field"
	fieldB := strings.Repeat("g", 55) + "_other_field"
	schema := `{"type":"object","properties":{"` + field65 + `":{"type":"string"},"` + fieldA + `":{"type":"integer"},"` + fieldB + `":{"type":"integer"}}}`
	streams := []synclet.ConfiguredStream{
		stream(first, schema, synclet.DestinationSyncModeAppend, nil, nil),
		stream(other, schema, synclet.DestinationSyncModeOverwrite, nil, nil),
	}
	data := `{"` + field65 + `":"v","` + fieldA + `":1,"` + fieldB + `":2}`
	for i := range 2 {
		out := runWrite(t, NewDestination(), cfg, streams, rec(first, data), rec(other, data))
		require.NoError(t, out.Err, "sync %d", i+1)
	}
	tFirst, tOther := myIdent(first), myIdent(other)
	assert.Len(t, tFirst, 64)
	assert.NotEqual(t, tFirst, tOther)
	assert.Equal(t, 2, countRows(t, cfg, tFirst))
	assert.Equal(t, 1, countRows(t, cfg, tOther))
	assert.ElementsMatch(t, []string{tFirst, tOther}, tables(t, cfg))
	rows := queryStrings(t, cfg, "SELECT `"+myIdent(field65)+"`, `"+myIdent(fieldA)+"`, `"+myIdent(fieldB)+"` FROM `"+tOther+"`")
	assert.Equal(t, [][]string{{"v", "1", "2"}}, rows)

	t.Run("names differing only in case fail before writing", func(t *testing.T) {
		s := stream("cased", `{"type":"object","properties":{"Name":{"type":"string"},"name":{"type":"string"}}}`, synclet.DestinationSyncModeAppend, nil, nil)
		out := runWrite(t, NewDestination(), cfg, []synclet.ConfiguredStream{s})
		require.Error(t, out.Err)
		assert.Contains(t, out.Err.Error(), `column names "Name" and "name" both map to "name"`)
		assert.False(t, hasTable(t, cfg, "cased"))
	})
	t.Run("two streams on one table fail before writing", func(t *testing.T) {
		a := stream("dup", `{"type":"object","properties":{}}`, synclet.DestinationSyncModeAppend, nil, nil)
		b := a
		b.Stream.Name = "DUP"
		out := runWrite(t, NewDestination(), cfg, []synclet.ConfiguredStream{a, b})
		require.Error(t, out.Err)
		assert.Contains(t, out.Err.Error(), `both map to "DUP"`)
		assert.False(t, hasTable(t, cfg, "dup"))
	})
}

func TestWriteOverwrite(t *testing.T) {
	cfg := sharedMySQL(t, "overwrite")
	s := stream("rates", `{"type":"object","properties":{"code":{"type":"string"},"day":{"type":"string","format":"date"}}}`,
		synclet.DestinationSyncModeOverwrite, nil, nil)
	write := func(lines ...string) *testkit.Output {
		return runWrite(t, testDestination(2), cfg, []synclet.ConfiguredStream{s}, lines...)
	}
	codes := func() [][]string { return queryStrings(t, cfg, `SELECT code FROM rates ORDER BY code`) }

	out := write(rec("rates", `{"code":"a"}`), rec("rates", `{"code":"b"}`), rec("rates", `{"code":"c"}`), state("rates", `1`))
	require.NoError(t, out.Err)
	assert.Equal(t, []string{"1"}, stateValues(out))
	assert.Equal(t, 3, countRows(t, cfg, "rates"))

	out = write(rec("rates", `{"code":"d"}`), rec("rates", `{"code":"e"}`), state("rates", `2`))
	require.NoError(t, out.Err)
	assert.Equal(t, []string{"2"}, stateValues(out))
	assert.Equal(t, [][]string{{"d"}, {"e"}}, codes())
	assert.Equal(t, []string{"rates"}, tables(t, cfg))

	t.Run("a sync failing in Go keeps the old table", func(t *testing.T) {
		out := write(rec("rates", `{"code":"f"}`), rec("rates", `{"code":"g"}`), state("rates", `3`), rec("rates", `{"code":"h","day":"not-a-date"}`))
		require.Error(t, out.Err)
		assert.Empty(t, out.States())
		assert.Equal(t, [][]string{{"d"}, {"e"}}, codes())
		assert.Equal(t, []string{"rates"}, tables(t, cfg))
	})
	t.Run("a sync MySQL fails keeps the old table", func(t *testing.T) {
		out := write(rec("rates", `{"code":"f"}`), rec("rates", `{"code":"g"}`), state("rates", `3`), rec("rates", `{"code":"h","day":"2024-02-30"}`))
		require.Error(t, out.Err)
		assert.Contains(t, out.Err.Error(), `stream "rates": record 3: column "day"`)
		assert.Empty(t, out.States())
		assert.Equal(t, [][]string{{"d"}, {"e"}}, codes())
		assert.Equal(t, []string{"rates"}, tables(t, cfg))
	})
}

func TestWriteAppend(t *testing.T) {
	cfg := sharedMySQL(t, "append")
	s := stream("events", `{"type":"object","properties":{"id":{"type":"integer"}}}`, synclet.DestinationSyncModeAppend, nil, nil)
	for range 2 {
		out := runWrite(t, testDestination(1), cfg, []synclet.ConfiguredStream{s}, rec("events", `{"id":1}`), rec("events", `{"id":2}`), state("events", `1`))
		require.NoError(t, out.Err)
		assert.Equal(t, []string{"1"}, stateValues(out))
	}
	assert.Equal(t, [][]string{{"1"}, {"1"}, {"2"}, {"2"}}, queryStrings(t, cfg, `SELECT id FROM events ORDER BY id`))
}

func TestWriteDedup(t *testing.T) {
	cfg := sharedMySQL(t, "dedup")
	s := stream("users", `{"type":"object","properties":{"id":{"type":"integer"},"v":{"type":"string"},"c":{"type":"integer"}}}`,
		synclet.DestinationSyncModeAppendDedup, [][]string{{"id"}}, []string{"c"})
	write := func(lines ...string) *testkit.Output {
		return runWrite(t, testDestination(4), cfg, []synclet.ConfiguredStream{s}, lines...)
	}
	users := func() [][]string {
		return queryStrings(t, cfg, `SELECT id, v, c FROM users ORDER BY id`)
	}

	out := write(
		// batch 1: a lower cursor later in the same batch
		rec("users", `{"id":1,"v":"1@1","c":1}`), rec("users", `{"id":2,"v":"2@5","c":5}`),
		rec("users", `{"id":1,"v":"1@3","c":3}`), rec("users", `{"id":1,"v":"1@2","c":2}`),
		// batch 2: a lower cursor in a later batch, an equal cursor, a NULL cursor then a higher one
		rec("users", `{"id":1,"v":"1@0","c":0}`), rec("users", `{"id":2,"v":"2@5 again","c":5}`),
		rec("users", `{"id":3,"v":"3@null","c":null}`), rec("users", `{"id":3,"v":"3@1","c":1}`),
		state("users", `1`),
	)
	require.NoError(t, out.Err)
	assert.Equal(t, []string{"1"}, stateValues(out))
	assert.Equal(t, [][]string{{"1", "1@3", "3"}, {"2", "2@5 again", "5"}, {"3", "3@1", "1"}}, users())

	out = write(rec("users", `{"id":1,"v":"1@9","c":9}`), rec("users", `{"id":3,"v":"3@null","c":null}`), rec("users", `{"id":4,"v":"4@null"}`))
	require.NoError(t, out.Err)
	assert.Equal(t, [][]string{{"1", "1@9", "9"}, {"2", "2@5 again", "5"}, {"3", "3@1", "1"}, {"4", "4@null", "<NULL>"}}, users())

	out = write(rec("users", `{"id":4,"v":"4@2","c":2}`))
	require.NoError(t, out.Err)
	assert.Equal(t, []string{"4", "4@2", "2"}, users()[3], "a stored NULL cursor is replaced")

	t.Run("without cursor the last arrival wins", func(t *testing.T) {
		s := stream("plain", `{"type":"object","properties":{"id":{"type":"integer"},"v":{"type":"string"}}}`,
			synclet.DestinationSyncModeAppendDedup, [][]string{{"id"}}, nil)
		out := runWrite(t, testDestination(3), cfg, []synclet.ConfiguredStream{s},
			rec("plain", `{"id":1,"v":"a"}`), rec("plain", `{"id":1,"v":"b"}`), rec("plain", `{"id":1,"v":"c"}`),
			rec("plain", `{"id":2,"v":"d"}`), rec("plain", `{"id":2,"v":"e"}`))
		require.NoError(t, out.Err)
		assert.Equal(t, [][]string{{"1", "c"}, {"2", "e"}}, queryStrings(t, cfg, `SELECT id, v FROM plain ORDER BY id`))
	})
	t.Run("string keys differing in case stay distinct", func(t *testing.T) {
		s := stream("tags", `{"type":"object","properties":{"k":{"type":"string"},"n":{"type":"integer"},"v":{"type":"string"}}}`,
			synclet.DestinationSyncModeAppendDedup, [][]string{{"k"}, {"n"}}, nil)
		out := runWrite(t, NewDestination(), cfg, []synclet.ConfiguredStream{s},
			rec("tags", `{"k":"A","n":1,"v":"upper"}`), rec("tags", `{"k":"a","n":1,"v":"lower"}`),
			rec("tags", `{"k":"é","n":1,"v":"accent"}`), rec("tags", `{"k":"e","n":1,"v":"plain"}`), rec("tags", `{"k":"a","n":1,"v":"lower 2"}`))
		require.NoError(t, out.Err)
		assert.Equal(t, [][]string{{"A", "upper"}, {"a", "lower 2"}, {"e", "plain"}, {"é", "accent"}},
			queryStrings(t, cfg, `SELECT k, v FROM tags ORDER BY k`))
		cols := queryStrings(t, cfg, `SELECT COLUMN_TYPE FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'tags' AND COLUMN_NAME = 'k'`)
		assert.Equal(t, "varchar(255)", cols[0][0])
	})
	t.Run("string key values that do not fit fail", func(t *testing.T) {
		s := stream("tags", `{"type":"object","properties":{"k":{"type":"string"},"n":{"type":"integer"},"v":{"type":"string"}}}`,
			synclet.DestinationSyncModeAppendDedup, [][]string{{"k"}, {"n"}}, nil)
		for _, c := range []struct{ data, want string }{
			{`{"k":"` + strings.Repeat("x", 256) + `","n":1}`, `primary key column "k": value is longer than 255 characters`},
		} {
			out := runWrite(t, NewDestination(), cfg, []synclet.ConfiguredStream{s}, rec("tags", c.data))
			require.Error(t, out.Err)
			assert.Contains(t, out.Err.Error(), c.want)
		}
	})
	t.Run("trailing spaces make distinct keys and cursors", func(t *testing.T) {
		s := stream("spaced", `{"type":"object","properties":{"k":{"type":"string"},"cur":{"type":"string"},"v":{"type":"string"}}}`,
			synclet.DestinationSyncModeAppendDedup, [][]string{{"k"}}, []string{"cur"})
		out := runWrite(t, NewDestination(), cfg, []synclet.ConfiguredStream{s},
			rec("spaced", `{"k":"a","cur":"x","v":"plain"}`), rec("spaced", `{"k":"a ","cur":"x","v":"spaced"}`),
			rec("spaced", `{"k":"b","cur":"x ","v":"stored"}`), rec("spaced", `{"k":"b","cur":"x","v":"lower cursor"}`))
		require.NoError(t, out.Err)
		assert.Equal(t, [][]string{{"a", "x", "plain"}, {"a ", "x", "spaced"}, {"b", "x ", "stored"}},
			queryStrings(t, cfg, `SELECT k, cur, v FROM spaced ORDER BY k`))
		assert.Equal(t, [][]string{{"utf8mb4_0900_bin", "utf8mb4_0900_bin"}},
			queryStrings(t, cfg, `SELECT TABLE_COLLATION, @@collation_connection FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'spaced'`))
	})
	t.Run("the widest key that fits is created", func(t *testing.T) {
		s := stream("wide", `{"type":"object","properties":{"a":{"type":"string"},"b":{"type":"string"},"c":{"type":"string"},"x":{"type":"integer"}}}`,
			synclet.DestinationSyncModeAppendDedup, [][]string{{"a"}, {"b"}, {"c"}, {"x"}}, nil)
		out := runWrite(t, NewDestination(), cfg, []synclet.ConfiguredStream{s}, rec("wide", `{"a":"1","b":"2","c":"3","x":4}`))
		require.NoError(t, out.Err)
		assert.Equal(t, 1, countRows(t, cfg, "wide"))
	})
	t.Run("keys MySQL cannot index fail the plan", func(t *testing.T) {
		four := stream("four", `{"type":"object","properties":{"a":{"type":"string"},"b":{"type":"string"},"c":{"type":"string"},"x":{"type":"integer"},"y":{"type":"integer"}}}`,
			synclet.DestinationSyncModeAppendDedup, [][]string{{"a"}, {"b"}, {"c"}, {"x"}, {"y"}}, nil)
		out := runWrite(t, NewDestination(), cfg, []synclet.ConfiguredStream{four})
		require.Error(t, out.Err)
		assert.Contains(t, out.Err.Error(), "primary key needs 3076 bytes, more than the 3072 bytes of a MySQL key")
		obj := stream("obj", `{"type":"object","properties":{"o":{"type":"object"}}}`, synclet.DestinationSyncModeAppendDedup, [][]string{{"o"}}, nil)
		out = runWrite(t, NewDestination(), cfg, []synclet.ConfiguredStream{obj})
		require.Error(t, out.Err)
		assert.Contains(t, out.Err.Error(), `primary key field "o" is an object`)
		assert.False(t, hasTable(t, cfg, "four"))
	})
	t.Run("NULL primary key fails", func(t *testing.T) {
		out := write(rec("users", `{"id":null,"v":"x","c":1}`))
		require.Error(t, out.Err)
		assert.Contains(t, out.Err.Error(), `stream "users": record 1: primary key column "id" is null`)
	})
	t.Run("table without the primary key constraint fails", func(t *testing.T) {
		execSQL(t, cfg, "CREATE TABLE nopk (id BIGINT, _airbyte_raw_id CHAR(36), _airbyte_extracted_at DATETIME(6))")
		s := stream("nopk", `{"type":"object","properties":{"id":{"type":"integer"}}}`,
			synclet.DestinationSyncModeAppendDedup, [][]string{{"id"}}, nil)
		out := runWrite(t, NewDestination(), cfg, []synclet.ConfiguredStream{s}, rec("nopk", `{"id":1}`))
		require.Error(t, out.Err)
		assert.Contains(t, out.Err.Error(), "table `nopk` has no primary key on (`id`)")
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
		w.seen = append(w.seen, fmt.Sprintf("%s=%d", m.State.Stream.StreamState, countRows(w.t, w.cfg, w.tables[name])))
	}
	return len(p), nil
}

func TestStateAfterCommit(t *testing.T) {
	cfg := sharedMySQL(t, "state_commit")
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

func TestFailMidBatch(t *testing.T) {
	cfg := sharedMySQL(t, "fail_mid_batch")
	s := stream("orders", `{"type":"object","properties":{"id":{"type":"integer"},"placed_on":{"type":"string","format":"date"}}}`,
		synclet.DestinationSyncModeAppend, nil, nil)
	out := runWrite(t, testDestination(3), cfg, []synclet.ConfiguredStream{s},
		rec("orders", `{"id":1,"placed_on":"2024-01-01"}`),
		rec("orders", `{"id":2,"placed_on":"2024-01-02"}`),
		rec("orders", `{"id":3,"placed_on":"2024-01-03"}`),
		state("orders", `"s1"`),
		rec("orders", `{"id":4,"placed_on":"2024-01-04"}`),
		rec("orders", `{"id":5,"placed_on":"2024-13-45"}`),
		state("orders", `"s2"`),
		rec("orders", `{"id":6,"placed_on":"2024-01-06"}`),
		state("orders", `"s3"`),
	)
	require.Error(t, out.Err)
	assert.Equal(t, []string{`"s1"`}, stateValues(out))
	assert.Equal(t, 3, countRows(t, cfg, "orders"))
	traces := out.ErrorTraces()
	require.Len(t, traces, 1)
	assert.Contains(t, traces[0].Message, `stream "orders": record 5: column "placed_on": Incorrect date value`)

	t.Run("a later statement of the batch rolls back the earlier ones", func(t *testing.T) {
		lines := []string{state("orders", `"before"`)}
		for i := 1; i <= 2500; i++ {
			day := "2024-01-01"
			if i == 2200 {
				day = "2024-02-30"
			}
			lines = append(lines, rec("orders", fmt.Sprintf(`{"id":%d,"placed_on":%q}`, 1000+i, day)))
		}
		lines = append(lines, state("orders", `"after"`))
		out := runWrite(t, NewDestination(), cfg, []synclet.ConfiguredStream{s}, lines...)
		require.Error(t, out.Err)
		assert.Contains(t, out.Err.Error(), `stream "orders": record 2200: column "placed_on"`)
		assert.Equal(t, []string{`"before"`}, stateValues(out))
		assert.Equal(t, 3, countRows(t, cfg, "orders"))
	})
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

func TestWriteGlobalStateWaitsForSwap(t *testing.T) {
	cfg := sharedMySQL(t, "global_state")
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
		rec("logs", `{"day":"2024-02-30"}`),
	)
	require.Error(t, out.Err)
	assert.Equal(t, []string{"2"}, stateKinds(out))
	assert.Equal(t, [][]string{{"a"}, {"b"}}, queryStrings(t, cfg, `SELECT code FROM rates ORDER BY code`))
}
