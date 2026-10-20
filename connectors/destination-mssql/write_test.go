package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
	"github.com/synclet-io/synclet-go-sdk/testkit"
)

func columnTypes(t *testing.T, cfg Config, table string) map[string]string {
	t.Helper()
	rows := queryStrings(t, cfg, `SELECT COLUMN_NAME, DATA_TYPE,
  COALESCE(CAST(CHARACTER_MAXIMUM_LENGTH AS NVARCHAR(10)), CAST(NUMERIC_PRECISION AS NVARCHAR(10)) + ',' + CAST(NUMERIC_SCALE AS NVARCHAR(10)), CAST(DATETIME_PRECISION AS NVARCHAR(10)), ''),
  COALESCE(COLLATION_NAME, '')
FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = @p1 AND TABLE_NAME = @p2`, cfg.Schema, table)
	got := map[string]string{}
	for _, r := range rows {
		got[r[0]] = strings.TrimSpace(r[1] + " " + r[2] + " " + r[3])
	}
	return got
}

func TestWriteAllTypes(t *testing.T) {
	cfg := sharedMSSQL(t, "all_types")
	out := runTestdata(t, cfg, "catalog.json", "messages.jsonl")
	require.NoError(t, out.Err)
	assert.Equal(t, []string{`{"cursor":2}`}, stateValues(out))

	text := "nvarchar -1 Latin1_General_100_BIN2"
	assert.Equal(t, map[string]string{
		"_airbyte_raw_id":       "uniqueidentifier",
		"_airbyte_extracted_at": "datetimeoffset 7",
		"id":                    "bigint 19,0",
		"bool_col":              "bit",
		"int_col":               "bigint 19,0",
		"airbyte_int_col":       "bigint 19,0",
		"num_col":               "float",
		"big_int_col":           "decimal 38,0",
		"str_col":               text,
		"date_col":              "date 0",
		"ts_col":                "datetimeoffset 7",
		"ts_ntz_col":            "datetime2 7",
		"time_col":              "time 7",
		"timetz_col":            text,
		"obj_col":               text,
		"arr_col":               text,
		"union_col":             text,
		"untyped_col":           text,
	}, columnTypes(t, cfg, "all_types"))

	rows := queryStrings(t, cfg, `SELECT num_col, big_int_col, int_col, airbyte_int_col, str_col, CONVERT(NVARCHAR(10), date_col, 23),
  CONVERT(NVARCHAR(40), ts_col, 121), CONVERT(NVARCHAR(40), ts_ntz_col, 121), CONVERT(NVARCHAR(20), time_col, 121), timetz_col, bool_col,
  CONVERT(NVARCHAR(40), _airbyte_extracted_at, 121), obj_col, arr_col, union_col, untyped_col
FROM all_types ORDER BY id`)
	require.Len(t, rows, 2)
	assert.Equal(t, []string{
		"12345.678901", "123456789012345678901234567890", "-9223372036854775808", "42",
		"tab\there, \"quoted\"\nnewline \\ backslash \U0001F600", "2024-02-29", "2024-02-29 11:14:15.1234560 +00:00", "2024-02-29 13:14:15.0000000",
		"13:14:15.5000000", "13:14:15+02:00", "true", "2023-11-14 22:13:20.0000000 +00:00",
	}, rows[0][:12])
	assert.JSONEq(t, `{"a":1,"nested":{"n":0.5,"s":"é"}}`, rows[0][12])
	assert.Equal(t, `[1.10,2]`, rows[0][13], "JSON text is kept as the source wrote it")
	assert.JSONEq(t, `"x"`, rows[0][14])
	assert.JSONEq(t, `{"k":"v"}`, rows[0][15])
	assert.Equal(t, []string{
		"1e-30", "-1", "0", "0", "", "1000-01-01", "1970-01-01 00:00:00.0000000 +00:00", "1970-01-01 00:00:00.0000000",
		"00:00:00.0000000", "00:00:00Z", "<NULL>", "2023-11-14 22:13:20.0000000 +00:00",
	}, rows[1][:12])
	assert.JSONEq(t, `{}`, rows[1][12])
	assert.JSONEq(t, `[]`, rows[1][13])
	assert.JSONEq(t, `7`, rows[1][14])
	assert.JSONEq(t, `"s"`, rows[1][15])

	ids := queryStrings(t, cfg, `SELECT COUNT(DISTINCT _airbyte_raw_id) FROM all_types`)
	assert.Equal(t, "2", ids[0][0])
}

func TestWriteValueThatDoesNotFit(t *testing.T) {
	cfg := sharedMSSQL(t, "no_fit")
	s := stream("nums", `{"type":"object","properties":{"n":{"type":"integer"},"x":{"type":"number"},"d":{"type":"string","format":"date"}}}`,
		synclet.DestinationSyncModeAppend, nil, nil)
	for _, c := range []struct{ data, want string }{
		{`{"n": 99999999999999999999}`, `column "n": value 99999999999999999999 is outside the BIGINT range`},
		{`{"n": 1.5}`, `column "n": value 1.5 is not an integer`},
		{`{"x": "twelve"}`, `column "x": value "twelve" is not a number`},
		{`{"x": 1e400}`, `column "x": value 1e400 is outside the FLOAT range`},
		{`{"x": 0.912345678901234567890}`, `column "x": value 0.912345678901234567890 has more precision than FLOAT`},
		{`{"d": "2024-02-30"}`, `column "d": value "2024-02-30" is not a valid date`},
		{`[1, 2]`, `stream "nums": record 1: not a JSON object`},
	} {
		out := runWrite(t, NewDestination(), cfg, []synclet.ConfiguredStream{s}, rec("nums", c.data), state("nums", `1`))
		require.Error(t, out.Err, c.data)
		assert.Contains(t, out.Err.Error(), c.want)
		assert.Empty(t, out.States())
	}
	assert.Equal(t, 0, countRows(t, cfg, "nums"))

	out := runWrite(t, NewDestination(), cfg, []synclet.ConfiguredStream{s}, rec("nums", `{"n": 1.0e3, "x": "2.5"}`), rec("nums", `{"n": "-7"}`))
	require.NoError(t, out.Err)
	assert.Equal(t, [][]string{{"-7", "<NULL>"}, {"1000", "2.5"}}, queryStrings(t, cfg, `SELECT n, x FROM nums ORDER BY n`))
}

func TestWriteUnknownFields(t *testing.T) {
	cfg := sharedMSSQL(t, "unknown_fields")
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
		assert.Contains(t, out.Err.Error(), `column "id" of table [dbo].[items] is bigint, the catalog needs nvarchar(max)`)
	})
}

func TestWriteSchemaAndNames(t *testing.T) {
	cfg := sharedMSSQL(t, "names")
	cfg.Schema = "raw ]data"
	first := strings.Repeat("s", 119) + "_first_stream"
	other := strings.Repeat("s", 119) + "_other_stream"
	field129 := strings.Repeat("f", 129)
	odd := "a]b [c].d'e"
	schema := `{"type":"object","properties":{"` + field129 + `":{"type":"string"},"` + odd + `":{"type":"integer"}}}`
	streams := []synclet.ConfiguredStream{
		stream(first, schema, synclet.DestinationSyncModeAppend, nil, nil),
		stream(other, schema, synclet.DestinationSyncModeOverwrite, nil, nil),
	}
	data := `{"` + field129 + `":"v","` + odd + `":1}`
	for i := range 2 {
		out := runWrite(t, NewDestination(), cfg, streams, rec(first, data), rec(other, data))
		require.NoError(t, out.Err, "sync %d", i+1)
	}
	tFirst, tOther := msIdent(first), msIdent(other)
	assert.Len(t, tFirst, 128)
	assert.NotEqual(t, tFirst, tOther)
	assert.Equal(t, 2, countRows(t, cfg, tFirst))
	assert.Equal(t, 1, countRows(t, cfg, tOther))
	assert.ElementsMatch(t, []string{tFirst, tOther}, tables(t, cfg), "the schema is created and holds only the stream tables")
	rows := queryStrings(t, cfg, "SELECT "+quote(msIdent(field129))+", "+quote(odd)+" FROM "+qualified(cfg.Schema, tOther))
	assert.Equal(t, [][]string{{"v", "1"}}, rows)

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
	cfg := sharedMSSQL(t, "overwrite")
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

	t.Run("a failing sync keeps the old table", func(t *testing.T) {
		out := write(rec("rates", `{"code":"f"}`), rec("rates", `{"code":"g"}`), state("rates", `3`), rec("rates", `{"code":"h","day":"not-a-date"}`))
		require.Error(t, out.Err)
		assert.Empty(t, out.States())
		assert.Equal(t, [][]string{{"d"}, {"e"}}, codes())
		assert.Equal(t, []string{"rates"}, tables(t, cfg))
	})
	t.Run("an empty sync empties the table", func(t *testing.T) {
		out := write(state("rates", `4`))
		require.NoError(t, out.Err)
		assert.Equal(t, []string{"4"}, stateValues(out))
		assert.Equal(t, 0, countRows(t, cfg, "rates"))
	})
}

func TestWriteAppend(t *testing.T) {
	cfg := sharedMSSQL(t, "append")
	s := stream("events", `{"type":"object","properties":{"id":{"type":"integer"}}}`, synclet.DestinationSyncModeAppend, nil, nil)
	for range 2 {
		out := runWrite(t, testDestination(1), cfg, []synclet.ConfiguredStream{s}, rec("events", `{"id":1}`), rec("events", `{"id":2}`), state("events", `1`))
		require.NoError(t, out.Err)
		assert.Equal(t, []string{"1"}, stateValues(out))
	}
	assert.Equal(t, [][]string{{"1"}, {"1"}, {"2"}, {"2"}}, queryStrings(t, cfg, `SELECT id FROM events ORDER BY id`))
}

func TestWriteDedup(t *testing.T) {
	cfg := sharedMSSQL(t, "dedup")
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
	t.Run("string keys differing in case or accents stay distinct", func(t *testing.T) {
		s := stream("tags", `{"type":"object","properties":{"k":{"type":"string"},"n":{"type":"integer"},"v":{"type":"string"}}}`,
			synclet.DestinationSyncModeAppendDedup, [][]string{{"k"}, {"n"}}, nil)
		out := runWrite(t, NewDestination(), cfg, []synclet.ConfiguredStream{s},
			rec("tags", `{"k":"A","n":1,"v":"upper"}`), rec("tags", `{"k":"a","n":1,"v":"lower"}`),
			rec("tags", `{"k":"é","n":1,"v":"accent"}`), rec("tags", `{"k":"e","n":1,"v":"plain"}`), rec("tags", `{"k":"a","n":1,"v":"lower 2"}`))
		require.NoError(t, out.Err)
		assert.Equal(t, [][]string{{"A", "upper"}, {"a", "lower 2"}, {"e", "plain"}, {"é", "accent"}},
			queryStrings(t, cfg, `SELECT k, v FROM tags ORDER BY k`))
		assert.Equal(t, "nvarchar 255 Latin1_General_100_BIN2", columnTypes(t, cfg, "tags")["k"])
	})
	t.Run("string key values that do not fit fail", func(t *testing.T) {
		s := stream("tags", `{"type":"object","properties":{"k":{"type":"string"},"n":{"type":"integer"},"v":{"type":"string"}}}`,
			synclet.DestinationSyncModeAppendDedup, [][]string{{"k"}, {"n"}}, nil)
		for _, c := range []struct{ data, want string }{
			{`{"k":"` + strings.Repeat("x", 256) + `","n":1}`, `primary key column "k": value is longer than 255 characters`},
			{`{"k":"a ","n":1}`, `primary key column "k": value "a " ends with a space`},
		} {
			out := runWrite(t, NewDestination(), cfg, []synclet.ConfiguredStream{s}, rec("tags", c.data))
			require.Error(t, out.Err)
			assert.Contains(t, out.Err.Error(), c.want)
		}
	})
	t.Run("the widest key that fits is created", func(t *testing.T) {
		s := stream("wide", `{"type":"object","properties":{"a":{"type":"string"},"b":{"type":"string"},"c":{"type":"string"},"x":{"type":"integer"}}}`,
			synclet.DestinationSyncModeAppendDedup, [][]string{{"a"}, {"b"}, {"c"}, {"x"}}, nil)
		long := strings.Repeat("\U0001F600", 127) + "x"
		out := runWrite(t, NewDestination(), cfg, []synclet.ConfiguredStream{s}, rec("wide", `{"a":"`+long+`","b":"`+long+`","c":"`+long+`","x":4}`))
		require.NoError(t, out.Err)
		assert.Equal(t, 1, countRows(t, cfg, "wide"))
	})
	t.Run("keys SQL Server cannot index fail the plan", func(t *testing.T) {
		four := stream("four", `{"type":"object","properties":{"a":{"type":"string"},"b":{"type":"string"},"c":{"type":"string"},"d":{"type":"string"}}}`,
			synclet.DestinationSyncModeAppendDedup, [][]string{{"a"}, {"b"}, {"c"}, {"d"}}, nil)
		out := runWrite(t, NewDestination(), cfg, []synclet.ConfiguredStream{four})
		require.Error(t, out.Err)
		assert.Contains(t, out.Err.Error(), "primary key needs 2040 bytes, more than the 1700 bytes of a SQL Server index key")
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
		execSQL(t, cfg, "CREATE TABLE nopk (id BIGINT, _airbyte_raw_id UNIQUEIDENTIFIER, _airbyte_extracted_at DATETIMEOFFSET(7))")
		s := stream("nopk", `{"type":"object","properties":{"id":{"type":"integer"}}}`,
			synclet.DestinationSyncModeAppendDedup, [][]string{{"id"}}, nil)
		out := runWrite(t, NewDestination(), cfg, []synclet.ConfiguredStream{s}, rec("nopk", `{"id":1}`))
		require.Error(t, out.Err)
		assert.Contains(t, out.Err.Error(), "table [dbo].[nopk] has no primary key on ([id])")
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
	cfg := sharedMSSQL(t, "state_commit")
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
	cfg := sharedMSSQL(t, "fail_mid_batch")
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
	assert.Contains(t, traces[0].Message, `stream "orders": record 5: column "placed_on": value "2024-13-45" is not a valid date`)

	t.Run("a failure in the database rolls back the whole batch", func(t *testing.T) {
		execSQL(t, cfg, "ALTER TABLE orders ADD CONSTRAINT orders_id_small CHECK (id < 3200)")
		lines := []string{state("orders", `"before"`)}
		for i := 1; i <= 2500; i++ {
			lines = append(lines, rec("orders", fmt.Sprintf(`{"id":%d,"placed_on":"2024-01-01"}`, 1000+i)))
		}
		lines = append(lines, state("orders", `"after"`))
		out := runWrite(t, NewDestination(), cfg, []synclet.ConfiguredStream{s}, lines...)
		require.Error(t, out.Err)
		assert.Contains(t, out.Err.Error(), `stream "orders": load batch into [dbo].[orders]`)
		assert.Contains(t, out.Err.Error(), "orders_id_small")
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
	cfg := sharedMSSQL(t, "global_state")
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
