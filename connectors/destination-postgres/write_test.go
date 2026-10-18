package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
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
	cfg := sharedPostgres(t, "all_types")
	out := runTestdata(t, cfg, "catalog.json", "messages.jsonl")
	require.NoError(t, out.Err)
	assert.Equal(t, []string{`{"cursor":2}`}, stateValues(out))

	cols := queryStrings(t, cfg, `SELECT column_name::text, data_type::text FROM information_schema.columns
WHERE table_schema = $1 AND table_name = 'all_types' ORDER BY column_name`, cfg.Schema)
	got := map[string]string{}
	for _, c := range cols {
		got[c[0]] = c[1]
	}
	assert.Equal(t, map[string]string{
		"_airbyte_raw_id":       "uuid",
		"_airbyte_extracted_at": "timestamp with time zone",
		"id":                    "bigint",
		"bool_col":              "boolean",
		"int_col":               "bigint",
		"airbyte_int_col":       "bigint",
		"num_col":               "numeric",
		"big_int_col":           "numeric",
		"str_col":               "text",
		"date_col":              "date",
		"ts_col":                "timestamp with time zone",
		"ts_ntz_col":            "timestamp without time zone",
		"time_col":              "time without time zone",
		"timetz_col":            "time with time zone",
		"obj_col":               "jsonb",
		"arr_col":               "jsonb",
		"union_col":             "jsonb",
		"untyped_col":           "jsonb",
	}, got)

	rows := queryStrings(t, cfg, `SELECT num_col::text, (num_col = '1e-30'::numeric)::text, big_int_col::text, int_col::text,
  str_col, date_col::text, ts_col AT TIME ZONE 'UTC' || '', ts_ntz_col::text, time_col::text, timetz_col::text,
  obj_col::text, arr_col::text, union_col::text, untyped_col::text, coalesce(bool_col::text, '<NULL>'),
  _airbyte_extracted_at AT TIME ZONE 'UTC' || ''
FROM all_types.all_types ORDER BY id`)
	require.Len(t, rows, 2)
	assert.Equal(t, []string{
		"12345678901234567890.123456789012345678", "false", "123456789012345678901234567890", "-9223372036854775808",
		"tab\there, \"quoted\"\nnewline \\ backslash", "2024-02-29", "2024-02-29 11:14:15.123456", "2024-02-29 13:14:15", "13:14:15", "13:14:15+02",
		`{"a": 1, "nested": {"n": 99999999999999999999.5}}`, "[1.10, 2]", `"x"`, `{"k": "v"}`, "true",
		"2023-11-14 22:13:20",
	}, rows[0])
	assert.Equal(t, "0.000000000000000000000000000001", rows[1][0])
	assert.Equal(t, "true", rows[1][1])
	assert.Equal(t, "<NULL>", rows[1][14])
	assert.Equal(t, "7", rows[1][12])

	nulls := queryStrings(t, cfg, `SELECT count(*)::text FROM all_types.all_types WHERE num_col IS NULL OR _airbyte_raw_id IS NULL`)
	assert.Equal(t, "0", nulls[0][0])
}

func TestWriteValueThatDoesNotFit(t *testing.T) {
	cfg := sharedPostgres(t, "no_fit")
	s := stream("nums", `{"type":"object","properties":{"n":{"type":"integer"},"x":{"type":"number"}}}`, synclet.DestinationSyncModeAppend, nil, nil)
	for _, c := range []struct{ data, want string }{
		{`{"n": 99999999999999999999}`, `column "n": value 99999999999999999999 is outside the bigint range`},
		{`{"x": "twelve"}`, `column "x": value "twelve" is not a number`},
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
	cfg := sharedPostgres(t, "unknown_fields")
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
		rows := queryStrings(t, cfg, `SELECT id::text, coalesce(extra, '<NULL>') FROM unknown_fields.items ORDER BY id`)
		assert.Equal(t, [][]string{{"1", "<NULL>"}, {"2", "<NULL>"}, {"3", "z"}}, rows)
	})
	t.Run("changed column type fails", func(t *testing.T) {
		s := stream("items", `{"type":"object","properties":{"id":{"type":"string"}}}`, synclet.DestinationSyncModeAppend, nil, nil)
		out := runWrite(t, NewDestination(), cfg, []synclet.ConfiguredStream{s}, rec("items", `{"id":"a"}`))
		require.Error(t, out.Err)
		assert.Contains(t, out.Err.Error(), `column "id" of table "unknown_fields"."items" is bigint, the catalog needs text`)
	})
}

func TestWriteLongNames(t *testing.T) {
	cfg := sharedPostgres(t, "long_names")
	name := strings.Repeat("stream_with_a_very_long_name_", 4)[:100]
	field := strings.Repeat("field_with_a_very_long_name_", 4)[:100]
	s := stream(name, `{"type":"object","properties":{"`+field+`":{"type":"string"}}}`, synclet.DestinationSyncModeAppend, nil, nil)
	for i := range 2 {
		out := runWrite(t, NewDestination(), cfg, []synclet.ConfiguredStream{s}, rec(name, `{"`+field+`":"v"}`))
		require.NoError(t, out.Err, "sync %d", i+1)
	}
	table, column := pgIdent(name), pgIdent(field)
	assert.Len(t, table, 63)
	assert.Equal(t, 2, countRows(t, cfg, table))
	rows := queryStrings(t, cfg, `SELECT table_name::text, column_name::text FROM information_schema.columns
WHERE table_schema = $1 AND column_name NOT LIKE '\_airbyte%'`, cfg.Schema)
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
	cfg := sharedPostgres(t, "overwrite")
	s := stream("rates", `{"type":"object","properties":{"code":{"type":"string"},"day":{"type":"string","format":"date"}}}`,
		synclet.DestinationSyncModeOverwrite, nil, nil)
	write := func(lines ...string) *testkit.Output {
		return runWrite(t, testDestination(2), cfg, []synclet.ConfiguredStream{s}, lines...)
	}

	out := write(rec("rates", `{"code":"a"}`), rec("rates", `{"code":"b"}`), rec("rates", `{"code":"c"}`), state("rates", `1`))
	require.NoError(t, out.Err)
	assert.Equal(t, 3, countRows(t, cfg, "rates"))

	out = write(rec("rates", `{"code":"d"}`), rec("rates", `{"code":"e"}`), state("rates", `2`))
	require.NoError(t, out.Err)
	assert.Equal(t, []string{"2"}, stateValues(out))
	assert.Equal(t, [][]string{{"d"}, {"e"}}, queryStrings(t, cfg, `SELECT code FROM overwrite.rates ORDER BY code`))

	out = write(rec("rates", `{"code":"f"}`), rec("rates", `{"code":"g"}`), state("rates", `3`), rec("rates", `{"code":"h","day":"not-a-date"}`))
	require.Error(t, out.Err)
	assert.Empty(t, out.States())
	assert.Equal(t, [][]string{{"d"}, {"e"}}, queryStrings(t, cfg, `SELECT code FROM overwrite.rates ORDER BY code`))
	assert.False(t, tableExists(t, cfg, "_synclet_tmp_rates"))
}

func TestWriteAppend(t *testing.T) {
	cfg := sharedPostgres(t, "append")
	s := stream("events", `{"type":"object","properties":{"id":{"type":"integer"}}}`, synclet.DestinationSyncModeAppend, nil, nil)
	for range 2 {
		out := runWrite(t, testDestination(1), cfg, []synclet.ConfiguredStream{s}, rec("events", `{"id":1}`), rec("events", `{"id":2}`), state("events", `1`))
		require.NoError(t, out.Err)
	}
	assert.Equal(t, 4, countRows(t, cfg, "events"))
}

func TestWriteDedup(t *testing.T) {
	cfg := sharedPostgres(t, "dedup")
	s := stream("users", `{"type":"object","properties":{"id":{"type":"integer"},"v":{"type":"string"},"c":{"type":"integer"}}}`,
		synclet.DestinationSyncModeAppendDedup, [][]string{{"id"}}, []string{"c"})
	write := func(lines ...string) *testkit.Output {
		return runWrite(t, testDestination(3), cfg, []synclet.ConfiguredStream{s}, lines...)
	}
	users := func() [][]string {
		return queryStrings(t, cfg, `SELECT id::text, v, c::text FROM dedup.users ORDER BY id`)
	}

	out := write(
		rec("users", `{"id":1,"v":"1@1","c":1}`), rec("users", `{"id":2,"v":"2@5","c":5}`), rec("users", `{"id":1,"v":"1@3","c":3}`),
		rec("users", `{"id":1,"v":"1@2","c":2}`), rec("users", `{"id":2,"v":"2@6","c":6}`), rec("users", `{"id":3,"v":"3@1","c":1}`),
		state("users", `1`),
	)
	require.NoError(t, out.Err)
	assert.Equal(t, [][]string{{"1", "1@3", "3"}, {"2", "2@6", "6"}, {"3", "3@1", "1"}}, users())

	out = write(rec("users", `{"id":1,"v":"1@9","c":9}`), rec("users", `{"id":3,"v":"3@0","c":0}`))
	require.NoError(t, out.Err)
	assert.Equal(t, [][]string{{"1", "1@9", "9"}, {"2", "2@6", "6"}, {"3", "3@1", "1"}}, users())

	t.Run("without cursor the last arrival wins", func(t *testing.T) {
		s := stream("plain", `{"type":"object","properties":{"id":{"type":"integer"},"v":{"type":"string"}}}`,
			synclet.DestinationSyncModeAppendDedup, [][]string{{"id"}}, nil)
		out := runWrite(t, testDestination(3), cfg, []synclet.ConfiguredStream{s},
			rec("plain", `{"id":1,"v":"a"}`), rec("plain", `{"id":1,"v":"b"}`), rec("plain", `{"id":1,"v":"c"}`),
			rec("plain", `{"id":2,"v":"d"}`), rec("plain", `{"id":2,"v":"e"}`))
		require.NoError(t, out.Err)
		assert.Equal(t, [][]string{{"1", "c"}, {"2", "e"}}, queryStrings(t, cfg, `SELECT id::text, v FROM dedup.plain ORDER BY id`))
	})
	t.Run("NULL primary key fails", func(t *testing.T) {
		out := write(rec("users", `{"id":null,"v":"x","c":1}`))
		require.Error(t, out.Err)
		assert.Contains(t, out.Err.Error(), `stream "users": record 1: primary key column "id" is null`)
	})
	t.Run("table without the primary key constraint fails", func(t *testing.T) {
		execSQL(t, cfg, `CREATE TABLE dedup.nopk (id bigint, _airbyte_raw_id uuid, _airbyte_extracted_at timestamptz)`)
		s := stream("nopk", `{"type":"object","properties":{"id":{"type":"integer"}}}`,
			synclet.DestinationSyncModeAppendDedup, [][]string{{"id"}}, nil)
		out := runWrite(t, NewDestination(), cfg, []synclet.ConfiguredStream{s}, rec("nopk", `{"id":1}`))
		require.Error(t, out.Err)
		assert.Contains(t, out.Err.Error(), `table "dedup"."nopk" has no primary key on ("id")`)
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
		rows := queryStrings(w.t, w.cfg, "SELECT count(*)::text FROM "+quote(w.cfg.Schema, w.tables[name]))
		w.seen = append(w.seen, string(m.State.Stream.StreamState)+"="+rows[0][0])
	}
	return len(p), nil
}

func TestStateAfterCommit(t *testing.T) {
	cfg := sharedPostgres(t, "state_commit")
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
	cfg := sharedPostgres(t, "fail_mid_batch")
	s := stream("orders", `{"type":"object","properties":{"id":{"type":"integer"},"placed_on":{"type":"string","format":"date"}}}`,
		synclet.DestinationSyncModeAppend, nil, nil)
	out := runWrite(t, testDestination(2), cfg, []synclet.ConfiguredStream{s},
		rec("orders", `{"id":1,"placed_on":"2024-01-01"}`),
		rec("orders", `{"id":2,"placed_on":"2024-01-02"}`),
		state("orders", `"s1"`),
		rec("orders", `{"id":3,"placed_on":"2024-13-45"}`),
		state("orders", `"s2"`),
	)
	require.Error(t, out.Err)
	assert.Equal(t, []string{`"s1"`}, stateValues(out))
	assert.Equal(t, 2, countRows(t, cfg, "orders"))
	traces := out.ErrorTraces()
	require.Len(t, traces, 1)
	assert.Contains(t, traces[0].Message, `stream "orders": record 3: column "placed_on"`)
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
	cfg := sharedPostgres(t, "global_state")
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
	assert.Equal(t, [][]string{{"a"}, {"b"}}, queryStrings(t, cfg, `SELECT code FROM global_state.rates ORDER BY code`))
}
