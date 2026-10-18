package main

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
	"github.com/synclet-io/synclet-go-sdk/testkit"
)

func TestCheckErrors(t *testing.T) {
	base := sharedMySQL(t)
	require.NoError(t, Source{}.Check(t.Context(), base))

	with := func(f func(*Config)) Config {
		c := base
		f(&c)
		return c
	}
	cases := []struct {
		name string
		cfg  Config
		want string
	}{
		{"wrong password", with(func(c *Config) { c.Password = "wrong" }),
			`cannot log in as "root": wrong user name or password`},
		{"unknown user", with(func(c *Config) { c.Username = "nobody" }),
			`cannot log in as "nobody": wrong user name or password`},
		{"closed port", with(func(c *Config) { c.Host, c.Port = "127.0.0.1", closedPort(t) }),
			"cannot reach 127.0.0.1:"},
		{"unknown host", with(func(c *Config) { c.Host = "nohost.invalid" }),
			"cannot reach nohost.invalid:"},
		{"missing database", with(func(c *Config) { c.Database = "nodb" }),
			`database "nodb" does not exist`},
		{"no database privilege", with(func(c *Config) { c.Username, c.Password = "nodb", "nodb" }),
			`user "nodb" has no privilege on database "testdb"`},
		{"no table SELECT", with(func(c *Config) { c.Username, c.Password = "noselect", "noselect" }),
			`user "noselect" may not SELECT from table testdb.secret`},
		{"CDC without REPLICATION CLIENT", with(func(c *Config) {
			c.Username, c.Password, c.ReplicationMethod.Method = "norepl", "norepl", replicationCDC
		}),
			`user "norepl" lacks the REPLICATION CLIENT privilege CDC needs`},
		{"empty host", with(func(c *Config) { c.Host = "" }),
			"host is required"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.ErrorContains(t, Source{}.Check(t.Context(), tc.cfg), tc.want)
		})
	}

	t.Run("CDC", func(t *testing.T) {
		cfg := base
		cfg.ReplicationMethod.Method = replicationCDC
		require.NoError(t, Source{}.Check(t.Context(), cfg))
	})
}

func TestCheckReportsCause(t *testing.T) {
	cfg := sharedMySQL(t)
	cfg.Password = "wrong"
	out, err := testkit.RunSource(t.Context(), Source{}, synclet.Request{Command: synclet.CommandCheck, Config: mustJSON(t, cfg)})
	require.NoError(t, err)
	status := out.ConnectionStatus()
	require.NotNil(t, status)
	require.Equal(t, synclet.ConnectionStatusFailed, status.Status)
	require.Contains(t, status.Message, `cannot log in as "root": wrong user name or password`)
}

func TestDiscover(t *testing.T) {
	cfg := sharedMySQL(t)
	cat, err := Source{}.Discover(t.Context(), cfg)
	require.NoError(t, err)
	byName := map[string]synclet.Stream{}
	for _, s := range cat.Streams {
		byName[s.Name] = s
	}
	users := byName["users"]
	require.Equal(t, testDB, users.Namespace)
	require.Equal(t, [][]string{{"id"}}, users.SourceDefinedPrimaryKey)
	require.Equal(t, []synclet.SyncMode{synclet.SyncModeFullRefresh, synclet.SyncModeIncremental}, users.SupportedSyncModes)
	require.False(t, users.SourceDefinedCursor)
	require.JSONEq(t, `{"type":"object","properties":{
		"id":{"type":["integer","null"]},
		"name":{"type":["string","null"]},
		"updated_at":{"type":["string","null"],"airbyte_type":"timestamp_without_timezone"}}}`, string(users.JSONSchema))
	require.Contains(t, byName, "we.ird`name")

	cfg.TableFilter.TablePatterns = []string{"zero%", "user_"}
	cat, err = Source{}.Discover(t.Context(), cfg)
	require.NoError(t, err)
	var got []string
	for _, s := range cat.Streams {
		got = append(got, s.Name)
	}
	require.Equal(t, []string{"users", "zero_dates"}, got)

	cfg = sharedMySQL(t)
	cfg.ReplicationMethod.Method = replicationCDC
	cat, err = Source{}.Discover(t.Context(), cfg)
	require.NoError(t, err)
	for _, s := range cat.Streams {
		require.True(t, s.SourceDefinedCursor, s.Name)
	}
}

// typeCases is the first row of the types table: each column's declared
// schema and the value it reads as.
var typeCases = []struct {
	column string
	schema string
	value  string
}{
	{"c_bool_int", `{"type":["integer","null"]}`, `1`},
	{"c_tinyint", `{"type":["integer","null"]}`, `-128`},
	{"c_smallint", `{"type":["integer","null"]}`, `32767`},
	{"c_mediumint", `{"type":["integer","null"]}`, `8388607`},
	{"c_int", `{"type":["integer","null"]}`, `-2147483648`},
	{"c_bigint", `{"type":["integer","null"]}`, `9223372036854775807`},
	{"c_bigint_unsigned", `{"type":["integer","null"]}`, `18446744073709551615`},
	{"c_year", `{"type":["integer","null"]}`, `2024`},
	{"c_decimal", `{"type":["number","null"]}`, `12345678901234567890123456789012345.123456789012345678901234567890`},
	{"c_numeric", `{"type":["number","null"]}`, `0.10`},
	{"c_float", `{"type":["number","null"]}`, `1.5`},
	{"c_double", `{"type":["number","null"]}`, `3.141592653589793`},
	{"c_bit1", `{"type":["boolean","null"]}`, `true`},
	{"c_bit12", `{"type":["integer","null"]}`, `2730`},
	{"c_bit64", `{"type":["integer","null"]}`, `18446744073709551615`},
	{"c_char", `{"type":["string","null"]}`, `"ab"`},
	{"c_varchar", `{"type":["string","null"]}`, `"héllo 🎉"`},
	{"c_tinytext", `{"type":["string","null"]}`, `"tiny"`},
	{"c_text", `{"type":["string","null"]}`, `"text"`},
	{"c_mediumtext", `{"type":["string","null"]}`, `"medium"`},
	{"c_longtext", `{"type":["string","null"]}`, `"long"`},
	{"c_enum", `{"type":["string","null"]}`, `"it's big"`},
	{"c_set", `{"type":["string","null"]}`, `"a,c"`},
	{"c_json", `{}`, `{"a": 1.5, "b": [1, 2]}`},
	{"c_binary", `{"type":["string","null"]}`, `"AP8Q"`},
	{"c_varbinary", `{"type":["string","null"]}`, `"3q2+7w=="`},
	{"c_tinyblob", `{"type":["string","null"]}`, `"AA=="`},
	{"c_blob", `{"type":["string","null"]}`, `"YmxvYg=="`},
	{"c_mediumblob", `{"type":["string","null"]}`, `"bWVkaXVtYmxvYg=="`},
	{"c_longblob", `{"type":["string","null"]}`, `"bG9uZ2Jsb2I="`},
	{"c_date", `{"type":["string","null"],"format":"date"}`, `"2024-02-29"`},
	{"c_datetime", `{"type":["string","null"],"airbyte_type":"timestamp_without_timezone"}`, `"2024-02-29T13:14:15.123456"`},
	{"c_datetime0", `{"type":["string","null"],"airbyte_type":"timestamp_without_timezone"}`, `"2024-02-29T13:14:15"`},
	{"c_timestamp", `{"type":["string","null"],"format":"date-time","airbyte_type":"timestamp_with_timezone"}`, `"2024-02-29T11:14:15.120Z"`},
	{"c_time", `{"type":["string","null"]}`, `"838:59:59"`},
	{"c_time_neg", `{"type":["string","null"]}`, `"-01:02:03.50"`},
	{"c_geometry", `{"type":["string","null"]}`, `"POINT(1 2)"`},
	{"c_point", `{"type":["string","null"]}`, `"POINT(3 4)"`},
	{"c_linestring", `{"type":["string","null"]}`, `"LINESTRING(0 0,1 1)"`},
	{"c_polygon", `{"type":["string","null"]}`, `"POLYGON((0 0,1 0,1 1,0 0))"`},
}

func TestTypesRoundTrip(t *testing.T) {
	cfg := sharedMySQL(t)
	stream := discoverStream(t, cfg, "types")

	var schema struct {
		Properties map[string]json.RawMessage `json:"properties"`
	}
	require.NoError(t, json.Unmarshal(stream.JSONSchema, &schema))
	require.Len(t, schema.Properties, len(typeCases)+1)
	for _, tc := range typeCases {
		require.JSONEq(t, tc.schema, string(schema.Properties[tc.column]), "schema of %s", tc.column)
	}

	rows := map[string]map[string]json.RawMessage{}
	for _, row := range rowsOf(t, runRead(t, cfg, fullRefresh(stream), nil), stream.Descriptor()) {
		rows[string(row["id"])] = row
	}
	require.Len(t, rows, 2)
	full, empty := rows["1"], rows["2"]
	require.NotNil(t, full)
	require.NotNil(t, empty)
	for _, tc := range typeCases {
		if tc.column == "c_json" {
			require.JSONEq(t, tc.value, string(full[tc.column]), tc.column)
		} else {
			require.Equal(t, tc.value, string(full[tc.column]), tc.column)
		}
		require.Equal(t, "null", string(empty[tc.column]), "%s of the NULL row", tc.column)
	}
}

func TestZeroDates(t *testing.T) {
	cfg := sharedMySQL(t)
	stream := discoverStream(t, cfg, "zero_dates")

	got := map[string]string{}
	for _, row := range rowsOf(t, runRead(t, cfg, fullRefresh(stream), nil), stream.Descriptor()) {
		got[string(row["id"])] = string(row["d"]) + " " + string(row["dt"]) + " " + string(row["ts"])
	}
	require.Equal(t, map[string]string{
		"1": `"2024-01-02" "2024-01-02T03:04:05" "2024-01-02T03:04:05Z"`,
		"2": `null null null`,
		"3": `null null null`,
	}, got)

	cfg.ZeroDateHandling = zeroDateFail
	out := tryRead(t, cfg, fullRefresh(stream), nil)
	require.ErrorContains(t, out.Err, `stream testdb.zero_dates: column "d" holds the zero date "0000-00-00" in the row with id=2; set zero_date_handling to "null"`)
	traces := out.ErrorTraces()
	require.Len(t, traces, 1)
	require.Equal(t, synclet.FailureTypeConfigError, traces[0].FailureType)
	require.Equal(t, []int{1}, ids(t, out, stream.Descriptor()))
}

func TestZerofill(t *testing.T) {
	cfg := sharedMySQL(t)
	stream := discoverStream(t, cfg, "padded")
	desc := stream.Descriptor()

	rows := rowsOf(t, runRead(t, cfg, fullRefresh(stream), nil), desc)
	require.Len(t, rows, 2)
	require.Equal(t, `42 1.50 2.5`, string(rows[0]["a"])+" "+string(rows[0]["b"])+" "+string(rows[0]["f"]))
	require.Equal(t, `0 0.50 0`, string(rows[1]["a"])+" "+string(rows[1]["b"])+" "+string(rows[1]["f"]))

	out := runRead(t, cfg, incrementalCatalog(stream, "a"), nil)
	require.Equal(t, []int{2, 1}, ids(t, out, desc))
}

// TestBinaryCursor resumes from a VARBINARY and a BIT cursor whose bytes are
// not valid UTF-8.
func TestBinaryCursor(t *testing.T) {
	cfg := sharedMySQL(t)
	stream := discoverStream(t, cfg, "bin_events")
	desc := stream.Descriptor()

	for _, tc := range []struct{ cursor, last, from string }{
		{"vb", "/wE=", "/wA="},
		{"bt", "3841", "3840"},
	} {
		t.Run(tc.cursor, func(t *testing.T) {
			first := runRead(t, cfg, incrementalCatalog(stream, tc.cursor), nil)
			require.Equal(t, []int{1, 2, 3}, ids(t, first, desc))
			require.Equal(t, streamState{CursorField: []string{tc.cursor}, Cursor: ptr(tc.last)}, lastState(t, first))

			resumed := runRead(t, cfg, incrementalCatalog(stream, tc.cursor), stateFor(t, desc, streamState{CursorField: []string{tc.cursor}, Cursor: ptr(tc.from)}))
			require.Equal(t, []int{2, 3}, ids(t, resumed, desc))
		})
	}

	lowest := runRead(t, cfg, incrementalCatalog(stream, "vb"), stateFor(t, desc, streamState{CursorField: []string{"vb"}, Cursor: ptr("AAE=")}))
	require.Equal(t, []int{1, 2, 3}, ids(t, lowest, desc), "the cursor compares as the bytes X'0001', not as the text AAE=")

	out := tryRead(t, cfg, incrementalCatalog(stream, "vb"), stateFor(t, desc, streamState{CursorField: []string{"vb"}, Cursor: ptr("not base64!")}))
	require.ErrorContains(t, out.Err, `stream testdb.bin_events: saved cursor "not base64!" of binary column "vb" is not base64`)
}

func TestOddIdentifiers(t *testing.T) {
	cfg := sharedMySQL(t)
	stream := discoverStream(t, cfg, "we.ird`name")
	desc := stream.Descriptor()

	full := runRead(t, cfg, fullRefresh(stream), nil)
	require.Equal(t, []int{1, 2, 3}, ids(t, full, desc))
	require.JSONEq(t, `{"id":1,"a.b":"x","up`+"`"+`d":10}`, string(full.Records(desc)[0].Data))

	cursor := "up`d"
	first := runRead(t, cfg, incrementalCatalog(stream, cursor), nil)
	require.Equal(t, []int{1, 2, 3}, ids(t, first, desc))
	from := "20"
	resumed := runRead(t, cfg, incrementalCatalog(stream, cursor), stateFor(t, desc, streamState{CursorField: []string{cursor}, Cursor: &from}))
	require.Equal(t, []int{2, 3}, ids(t, resumed, desc))
}

func TestIncrementalResume(t *testing.T) {
	cfg := sharedMySQL(t)
	execSQL(t, cfg, "DROP TABLE IF EXISTS events",
		"CREATE TABLE events (id INT PRIMARY KEY, updated_at DATETIME(6))",
		"INSERT INTO events VALUES (1, '2024-01-01 00:00:00'), (2, '2024-01-02 00:00:00'), (3, '2024-01-03 00:00:00')")
	stream := discoverStream(t, cfg, "events")
	desc := stream.Descriptor()
	cat := incrementalCatalog(stream, "updated_at")
	cursor := func(s string) *string { return &s }

	first := runRead(t, cfg, cat, nil)
	require.Equal(t, []int{1, 2, 3}, ids(t, first, desc))
	t3 := lastState(t, first)
	require.Equal(t, streamState{CursorField: []string{"updated_at"}, Cursor: cursor("2024-01-03 00:00:00.000000")}, t3)

	fromT2 := runRead(t, cfg, cat, stateFor(t, desc, streamState{CursorField: []string{"updated_at"}, Cursor: cursor("2024-01-02 00:00:00.000000")}))
	require.Equal(t, []int{2, 3}, ids(t, fromT2, desc), "rows below the saved cursor are skipped, the row equal to it is read")

	execSQL(t, cfg, "INSERT INTO events VALUES (4, '2024-01-04 00:00:00')",
		"UPDATE events SET updated_at = '2024-01-05 00:00:00' WHERE id = 1")
	resumed := runRead(t, cfg, cat, stateFor(t, desc, t3))
	require.Equal(t, []int{3, 4, 1}, ids(t, resumed, desc))
	require.Equal(t, streamState{CursorField: []string{"updated_at"}, Cursor: cursor("2024-01-05 00:00:00.000000")}, lastState(t, resumed))

	changed := runRead(t, cfg, cat, stateFor(t, desc, streamState{CursorField: []string{"id"}, Cursor: cursor("3")}))
	require.ElementsMatch(t, []int{1, 2, 3, 4}, ids(t, changed, desc))
	require.True(t, warned(changed, "cursor field changed"))

	t.Run("exact DECIMAL and unsigned BIGINT cursors", func(t *testing.T) {
		execSQL(t, cfg, "DROP TABLE IF EXISTS amounts",
			"CREATE TABLE amounts (id INT PRIMARY KEY, amount DECIMAL(20,2), big BIGINT UNSIGNED)",
			"INSERT INTO amounts VALUES (1, 12345678901234567.88, 18446744073709551613), (2, 12345678901234567.89, 18446744073709551614), (3, 12345678901234567.90, 18446744073709551615)")
		amounts := discoverStream(t, cfg, "amounts")
		for _, tc := range []struct{ cursor, from string }{{"amount", "12345678901234567.89"}, {"big", "18446744073709551614"}} {
			out := runRead(t, cfg, incrementalCatalog(amounts, tc.cursor), stateFor(t, amounts.Descriptor(), streamState{CursorField: []string{tc.cursor}, Cursor: &tc.from}))
			require.Equal(t, []int{2, 3}, ids(t, out, amounts.Descriptor()), tc.cursor)
		}
	})
}

func TestIncrementalLegacyState(t *testing.T) {
	cfg := sharedMySQL(t)
	execSQL(t, cfg, "DROP TABLE IF EXISTS legacy_events",
		"CREATE TABLE legacy_events (id INT PRIMARY KEY, amount DECIMAL(20,2), updated_at DATETIME)",
		"INSERT INTO legacy_events VALUES (1, 10.50, '2024-01-01 00:00:00'), (2, 20.50, '2024-01-02 00:00:00'), (3, 30.50, '2024-01-03 00:00:00')")
	stream := discoverStream(t, cfg, "legacy_events")
	desc := stream.Descriptor()

	byTime := runRead(t, cfg, incrementalCatalog(stream, "updated_at"),
		stateFor(t, desc, json.RawMessage(`{"phase":"incremental","cursor_field":"updated_at","cursor_value":"2024-01-02T00:00:00Z","snapshot_done":true}`)))
	require.Equal(t, []int{2, 3}, ids(t, byTime, desc))
	require.Equal(t, streamState{CursorField: []string{"updated_at"}, Cursor: ptr("2024-01-03 00:00:00")}, lastState(t, byTime))

	byAmount := runRead(t, cfg, incrementalCatalog(stream, "amount"),
		stateFor(t, desc, json.RawMessage(`{"phase":"incremental","cursor_field":"amount","cursor_value":20.50}`)))
	require.Equal(t, []int{2, 3}, ids(t, byAmount, desc))

	withoutNamespace := mustJSON(t, []synclet.StateMessage{{
		Type: synclet.StateTypeStream,
		Stream: &synclet.StreamState{StreamDescriptor: synclet.StreamDescriptor{Name: desc.Name},
			StreamState: json.RawMessage(`{"phase":"incremental","cursor_field":"amount","cursor_value":30.50}`)},
	}})
	require.Equal(t, []int{3}, ids(t, runRead(t, cfg, incrementalCatalog(stream, "amount"), withoutNamespace), desc))

	t.Run("binary cursor", func(t *testing.T) {
		execSQL(t, cfg, "DROP TABLE IF EXISTS legacy_bin", "CREATE TABLE legacy_bin (id INT PRIMARY KEY, vb VARBINARY(8))")
		bin := discoverStream(t, cfg, "legacy_bin")
		legacy := stateFor(t, bin.Descriptor(), json.RawMessage(`{"phase":"incremental","cursor_field":"vb","cursor_value":"abcd"}`))
		empty := runRead(t, cfg, incrementalCatalog(bin, "vb"), legacy)
		require.Empty(t, ids(t, empty, bin.Descriptor()))
		saved := lastState(t, empty)
		require.Equal(t, streamState{CursorField: []string{"vb"}, Cursor: ptr("YWJjZA==")}, saved, "the 0.2 bytes abcd, base64-encoded")

		execSQL(t, cfg, "INSERT INTO legacy_bin VALUES (1, 'abc'), (2, 'abcd'), (3, 'bbbb')")
		require.Equal(t, []int{2, 3}, ids(t, runRead(t, cfg, incrementalCatalog(bin, "vb"), legacy), bin.Descriptor()))
		require.Equal(t, []int{2, 3}, ids(t, runRead(t, cfg, incrementalCatalog(bin, "vb"), stateFor(t, bin.Descriptor(), saved)), bin.Descriptor()))
	})

	t.Run("binary cursor with lost bytes", func(t *testing.T) {
		execSQL(t, cfg, "DROP TABLE IF EXISTS legacy_lost", "CREATE TABLE legacy_lost (id INT PRIMARY KEY, vb VARBINARY(8), bt BIT(12))",
			"INSERT INTO legacy_lost VALUES (1, X'FF00', b'111100000000')")
		lost := discoverStream(t, cfg, "legacy_lost")
		// 0.2 saved the bytes X'FF00' as JSON text, which replaced X'FF' with U+FFFD.
		for _, cursor := range []string{"vb", "bt"} {
			state := stateFor(t, lost.Descriptor(), json.RawMessage(`{"phase":"incremental","cursor_field":"`+cursor+`","cursor_value":"\ufffd\u0000"}`))
			out := tryRead(t, cfg, incrementalCatalog(lost, cursor), state)
			require.ErrorContains(t, out.Err, `stream testdb.legacy_lost: the cursor of column "`+cursor+`" saved by version 0.2 lost bytes that are not valid UTF-8; reset the state of this stream`)
			traces := out.ErrorTraces()
			require.Len(t, traces, 1)
			require.Equal(t, synclet.FailureTypeConfigError, traces[0].FailureType)
			require.Empty(t, out.Records(lost.Descriptor()))
		}
	})

	t.Run("BIT cursor", func(t *testing.T) {
		execSQL(t, cfg, "DROP TABLE IF EXISTS legacy_bit", "CREATE TABLE legacy_bit (id INT PRIMARY KEY, bt BIT(12))")
		bits := discoverStream(t, cfg, "legacy_bit")
		// 0.2 saved a BIT cursor as its raw bytes: b'111100000000' is 0x0F00.
		legacy := stateFor(t, bits.Descriptor(), json.RawMessage(`{"phase":"incremental","cursor_field":"bt","cursor_value":"\u000f\u0000"}`))
		empty := runRead(t, cfg, incrementalCatalog(bits, "bt"), legacy)
		saved := lastState(t, empty)
		require.Equal(t, streamState{CursorField: []string{"bt"}, Cursor: ptr("3840")}, saved)

		execSQL(t, cfg, "INSERT INTO legacy_bit VALUES (1, b'000000000001'), (2, b'111100000000'), (3, b'111100000001')")
		require.Equal(t, []int{2, 3}, ids(t, runRead(t, cfg, incrementalCatalog(bits, "bt"), legacy), bits.Descriptor()))
		require.Equal(t, []int{2, 3}, ids(t, runRead(t, cfg, incrementalCatalog(bits, "bt"), stateFor(t, bits.Descriptor(), saved)), bits.Descriptor()))
	})

	snapshot := runRead(t, cfg, incrementalCatalog(stream, "amount"),
		stateFor(t, desc, json.RawMessage(`{"phase":"snapshot","cursor_field":"amount","max_cursor_value":30.50,"snapshot_last_pk":{"id":1}}`)))
	require.Equal(t, []int{1, 2, 3}, ids(t, snapshot, desc))
	require.True(t, warned(snapshot, "unfinished snapshot"))
}

func TestIncrementalCheckpoints(t *testing.T) {
	cfg := sharedMySQL(t)
	execSQL(t, cfg, "DROP TABLE IF EXISTS big",
		"CREATE TABLE big (id INT PRIMARY KEY)",
		"INSERT /*+ SET_VAR(cte_max_recursion_depth = 30000) */ INTO big WITH RECURSIVE g(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM g WHERE n < 25000) SELECT n FROM g")
	stream := discoverStream(t, cfg, "big")

	out := runRead(t, cfg, incrementalCatalog(stream, "id"), nil)
	require.Len(t, out.Records(stream.Descriptor()), 25000)
	var cursors []string
	for _, s := range out.States() {
		var st streamState
		require.NoError(t, json.Unmarshal(s.Stream.StreamState, &st))
		cursors = append(cursors, *st.Cursor)
	}
	require.Equal(t, []string{"10000", "20000", "25000"}, cursors)

	full := runRead(t, cfg, fullRefresh(stream), nil)
	require.Len(t, full.Records(stream.Descriptor()), 25000)
	require.Empty(t, full.States(), "a full refresh saves no state")
}

func TestReadConfigErrors(t *testing.T) {
	cfg := sharedMySQL(t)
	users := discoverStream(t, cfg, "users")
	run := func(cs synclet.ConfiguredStream) *testkit.Output {
		out := tryRead(t, cfg, &synclet.ConfiguredCatalog{Streams: []synclet.ConfiguredStream{cs}}, nil)
		require.Error(t, out.Err)
		traces := out.ErrorTraces()
		require.Len(t, traces, 1)
		require.Equal(t, synclet.FailureTypeConfigError, traces[0].FailureType)
		return out
	}
	missing := users
	missing.Name = "nope"
	require.ErrorContains(t, run(fullRefresh(missing).Streams[0]).Err, "stream testdb.nope: no such table")
	other := users
	other.Namespace = "otherdb"
	require.ErrorContains(t, run(fullRefresh(other).Streams[0]).Err, `namespace "otherdb" is not the configured database "testdb"`)
	require.ErrorContains(t, run(incrementalCatalog(users, "nope").Streams[0]).Err, `cursor column "nope" does not exist`)
}

func TestConformance(t *testing.T) {
	cfg := sharedMySQL(t)
	broken := cfg
	broken.Password = "wrong"
	users := discoverStream(t, cfg, "users")
	types := discoverStream(t, cfg, "types")
	zero := discoverStream(t, cfg, "zero_dates")

	t.Run("full refresh", func(t *testing.T) {
		testkit.RunSourceSuite(t, Source{}, testkit.SourceSuite{
			Config:       mustJSON(t, cfg),
			BrokenConfig: mustJSON(t, broken),
			Catalog:      fullRefresh(users, types, zero),
		})
	})
	t.Run("incremental", func(t *testing.T) {
		testkit.RunSourceSuite(t, Source{}, testkit.SourceSuite{
			Config:       mustJSON(t, cfg),
			BrokenConfig: mustJSON(t, broken),
			Catalog:      incrementalCatalog(users, "updated_at"),
		})
	})
}

func warned(out *testkit.Output, text string) bool {
	for _, l := range out.Logs() {
		if l.Level == synclet.LogLevelWarn && strings.Contains(l.Message, text) {
			return true
		}
	}
	return false
}

func ptr(s string) *string { return &s }
