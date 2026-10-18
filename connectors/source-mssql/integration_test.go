package main

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"net"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
	"github.com/synclet-io/synclet-go-sdk/testkit"
	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/knownhosts"
)

func closedPort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := l.Addr().(*net.TCPAddr).Port
	require.NoError(t, l.Close())
	return port
}

func TestCheckErrors(t *testing.T) {
	base := sharedMSSQL(t)
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
			`cannot log in as "synclet": wrong user name or password`},
		{"unknown user", with(func(c *Config) { c.Username = "nobody" }),
			`cannot log in as "nobody": wrong user name or password`},
		{"closed port", with(func(c *Config) { c.Host, c.Port = "127.0.0.1", closedPort(t) }),
			"cannot reach 127.0.0.1:"},
		{"unknown host", with(func(c *Config) { c.Host = "mssql.invalid" }),
			"cannot reach mssql.invalid:"},
		{"missing database", with(func(c *Config) { c.Database = "nodb" }),
			`database "nodb" does not exist`},
		{"login without a user in the database", with(func(c *Config) { c.Username, c.Password = "nouser", "NoUser-Test1!" }),
			`user "nouser" cannot open database "synclet": the login has no user in it`},
		{"no SELECT permission", with(func(c *Config) { c.Username, c.Password = "noperm", "NoPerm-Test1!" }),
			`user "noperm" may not SELECT from any table or view in schema "dbo"`},
		{"missing schema", with(func(c *Config) { c.Schemas = []string{"nope"} }),
			`schema "nope" does not exist`},
		{"invalid config", with(func(c *Config) { c.Host = "" }),
			"host is required"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.ErrorContains(t, Source{}.Check(t.Context(), tc.cfg), tc.want)
		})
	}
	t.Run("empty schema the user may read", func(t *testing.T) {
		require.NoError(t, Source{}.Check(t.Context(), with(func(c *Config) { c.Schemas = []string{"dbo", "empty"} })))
	})
}

func TestDiscover(t *testing.T) {
	cfg := sharedMSSQL(t)

	cat, err := Source{}.Discover(t.Context(), cfg)
	require.NoError(t, err)
	byName := map[string]synclet.Stream{}
	for _, s := range cat.Streams {
		byName[s.Namespace+"."+s.Name] = s
	}
	require.Contains(t, byName, "dbo.users")
	require.Contains(t, byName, "dbo.order_lines")
	require.Contains(t, byName, "dbo.user_names", "views are listed")
	require.Contains(t, byName, "dbo.types")
	require.NotContains(t, byName, "other.hidden", "schema not configured")

	users := byName["dbo.users"]
	require.Equal(t, [][]string{{"id"}}, users.SourceDefinedPrimaryKey)
	require.Equal(t, []synclet.SyncMode{synclet.SyncModeFullRefresh, synclet.SyncModeIncremental}, users.SupportedSyncModes)
	require.JSONEq(t, `{"type":"object","properties":{
		"id":{"type":["integer","null"]},
		"name":{"type":["string","null"]},
		"updated_at":{"type":["string","null"],"format":"date-time","airbyte_type":"timestamp_with_timezone"}}}`, string(users.JSONSchema))
	require.Equal(t, [][]string{{"order_id"}, {"line"}}, byName["dbo.order_lines"].SourceDefinedPrimaryKey)
	require.Empty(t, byName["dbo.user_names"].SourceDefinedPrimaryKey)
	require.JSONEq(t, `{"type":"object","properties":{"id":{"type":["integer","null"]},"name":{"type":["string","null"]}}}`,
		string(byName["dbo.user_names"].JSONSchema))

	cfg.Schemas = []string{"other"}
	cat, err = Source{}.Discover(t.Context(), cfg)
	require.NoError(t, err)
	require.Len(t, cat.Streams, 1)
	require.Equal(t, "other", cat.Streams[0].Namespace)
	require.Equal(t, "hidden", cat.Streams[0].Name)
}

func fullRefresh(streams ...synclet.Stream) *synclet.ConfiguredCatalog {
	cat := &synclet.ConfiguredCatalog{}
	for _, s := range streams {
		cat.Streams = append(cat.Streams, synclet.ConfiguredStream{
			Stream: s, SyncMode: synclet.SyncModeFullRefresh, DestinationSyncMode: synclet.DestinationSyncModeOverwrite,
		})
	}
	return cat
}

func TestDiscoverColumnGrants(t *testing.T) {
	cfg := sharedMSSQL(t)
	cfg.Username, cfg.Password, cfg.Schemas = "colonly", "ColOnly-Test1!", []string{"colgrants"}
	require.NoError(t, Source{}.Check(t.Context(), cfg))

	cat, err := Source{}.Discover(t.Context(), cfg)
	require.NoError(t, err)
	byName := map[string]synclet.Stream{}
	for _, s := range cat.Streams {
		byName[s.Name] = s
	}
	require.Contains(t, byName, "partial", "table readable through column grants")
	require.Contains(t, byName, "pk_hidden", "table readable through column grants")
	require.NotContains(t, byName, "none", "table without any grant")

	partial, pkHidden := byName["partial"], byName["pk_hidden"]
	require.JSONEq(t, `{"type":"object","properties":{"id":{"type":["integer","null"]},"visible":{"type":["string","null"]}}}`,
		string(partial.JSONSchema))
	require.Equal(t, [][]string{{"id"}}, partial.SourceDefinedPrimaryKey)
	require.JSONEq(t, `{"type":"object","properties":{"name":{"type":["string","null"]}}}`, string(pkHidden.JSONSchema))
	require.Empty(t, pkHidden.SourceDefinedPrimaryKey, "primary key on a column the user may not read")

	out := runRead(t, cfg, fullRefresh(partial, pkHidden), nil)
	require.Len(t, out.Records(partial.Descriptor()), 1)
	require.JSONEq(t, `{"id":1,"visible":"shown"}`, string(out.Records(partial.Descriptor())[0].Data))
	require.Len(t, out.Records(pkHidden.Descriptor()), 1)
	require.JSONEq(t, `{"name":"shown"}`, string(out.Records(pkHidden.Descriptor())[0].Data))
}

func TestReadStreamWithoutNamespace(t *testing.T) {
	cfg := sharedMSSQL(t)
	cfg.Schemas = []string{"twin_a", "twin_b"}
	bare := discoverStream(t, cfg, "twin_b", "twin")
	bare.Namespace = ""

	out, err := testkit.RunSource(t.Context(), Source{}, synclet.Request{
		Command: synclet.CommandRead, Config: mustJSON(t, cfg), Catalog: mustJSON(t, fullRefresh(bare)),
	})
	require.NoError(t, err)
	require.ErrorContains(t, out.Err, `stream twin: a table or view of that name is in schemas "twin_a", "twin_b"`)
	require.Empty(t, out.Records(bare.Descriptor()))
	traces := out.ErrorTraces()
	require.Len(t, traces, 1)
	require.Equal(t, synclet.FailureTypeConfigError, traces[0].FailureType)

	cfg.Schemas = []string{"twin_b"}
	require.Equal(t, []int{2}, ids(t, runRead(t, cfg, fullRefresh(bare), nil), bare.Descriptor()))
}

const (
	sString      = `{"type":["string","null"]}`
	sInteger     = `{"type":["integer","null"]}`
	sNumber      = `{"type":["number","null"]}`
	sTimestampTZ = `{"type":["string","null"],"format":"date-time","airbyte_type":"timestamp_with_timezone"}`
	sTimestamp   = `{"type":["string","null"],"airbyte_type":"timestamp_without_timezone"}`
)

// typeCases is the fixture row of dbo.types: each column's declared schema
// and the value it reads as.
var typeCases = []struct {
	column string
	schema string
	value  string
}{
	{"c_bit", `{"type":["boolean","null"]}`, `true`},
	{"c_tinyint", sInteger, `255`},
	{"c_smallint", sInteger, `-32768`},
	{"c_int", sInteger, `-2147483648`},
	{"c_bigint", sInteger, `9007199254740993`},
	{"c_decimal", sNumber, `1234567890123456789012345678.0123456789`},
	{"c_numeric", sNumber, `-0.50`},
	{"c_money", sNumber, `922337203685477.5807`},
	{"c_smallmoney", sNumber, `-214748.3648`},
	{"c_real", sNumber, `0.1`},
	{"c_float", sNumber, `1.5`},
	{"c_char", sString, `"ab  "`},
	{"c_varchar", sString, `"varchar"`},
	{"c_nchar", sString, `"nç "`},
	{"c_nvarchar", sString, `"héllo"`},
	{"c_text", sString, `"text"`},
	{"c_ntext", sString, `"ntëxt"`},
	{"c_xml", sString, `"\u003ca b=\"1\"\u003ex\u003c/a\u003e"`},
	{"c_variant", sString, `"42"`},
	{"c_uuid", sString, `"6f9619ff-8b86-d011-b42d-00c04fc964ff"`},
	{"c_binary", sString, `"AP8Q"`},
	{"c_varbinary", sString, `"AP8="`},
	{"c_image", sString, `"AQI="`},
	{"c_date", `{"type":["string","null"],"format":"date"}`, `"2024-02-29"`},
	{"c_time", `{"type":["string","null"],"airbyte_type":"time_without_timezone"}`, `"13:45:30.1234567"`},
	{"c_datetime", sTimestamp, `"2024-01-02T03:04:05.123"`},
	{"c_datetime2", sTimestamp, `"2024-01-02T03:04:05.1234567"`},
	{"c_smalldatetime", sTimestamp, `"2024-01-02T03:04:00"`},
	{"c_datetimeoffset", sTimestampTZ, `"2024-01-02T03:04:05.1234567+02:00"`},
	{"c_hierarchyid", sString, `"/1/2/"`},
	{"c_geography", sString, `"POINT (30 10)"`},
	{"c_geometry", sString, `"LINESTRING (0 0, 1 1)"`},
	{"c_alias", sInteger, `7`},
	{"c_sysname", sString, `"sys"`},
}

func TestTypesRoundTrip(t *testing.T) {
	cfg := sharedMSSQL(t)
	stream := discoverStream(t, cfg, "dbo", "types")

	var schema struct {
		Properties map[string]json.RawMessage `json:"properties"`
	}
	require.NoError(t, json.Unmarshal(stream.JSONSchema, &schema))
	require.Len(t, schema.Properties, len(typeCases)+3, "every column but id, c_varchar_max and c_rowversion has a case")
	for _, tc := range typeCases {
		require.JSONEq(t, tc.schema, string(schema.Properties[tc.column]), "schema of %s", tc.column)
	}
	require.JSONEq(t, sString, string(schema.Properties["c_rowversion"]))
	require.JSONEq(t, sString, string(schema.Properties["c_varchar_max"]))

	out := runRead(t, cfg, fullRefresh(stream), nil)
	recs := out.Records(stream.Descriptor())
	require.Len(t, recs, 2)

	rows := map[string]map[string]json.RawMessage{}
	for _, r := range recs {
		var row map[string]json.RawMessage
		require.NoError(t, json.Unmarshal(r.Data, &row))
		rows[string(row["id"])] = row
	}
	full, empty := rows["1"], rows["2"]
	require.NotNil(t, full)
	require.NotNil(t, empty)
	for _, tc := range typeCases {
		require.Equal(t, tc.value, string(full[tc.column]), tc.column)
		require.Equal(t, "null", string(empty[tc.column]), "%s of the NULL row", tc.column)
	}
	var long string
	require.NoError(t, json.Unmarshal(full["c_varchar_max"], &long))
	require.Equal(t, strings.Repeat("x", 9000), long)
	for _, row := range []map[string]json.RawMessage{full, empty} {
		var rv string
		require.NoError(t, json.Unmarshal(row["c_rowversion"], &rv))
		b, err := base64.StdEncoding.DecodeString(rv)
		require.NoError(t, err)
		require.Len(t, b, 8, "rowversion is 8 bytes")
	}
}

func incrementalCatalog(stream synclet.Stream, cursor string) *synclet.ConfiguredCatalog {
	return &synclet.ConfiguredCatalog{Streams: []synclet.ConfiguredStream{{
		Stream:              stream,
		SyncMode:            synclet.SyncModeIncremental,
		CursorField:         []string{cursor},
		DestinationSyncMode: synclet.DestinationSyncModeAppendDedup,
		PrimaryKey:          [][]string{{"id"}},
	}}}
}

func ids(t *testing.T, out *testkit.Output, desc synclet.StreamDescriptor) []int {
	t.Helper()
	var got []int
	for _, r := range out.Records(desc) {
		var row struct {
			ID int `json:"id"`
		}
		require.NoError(t, json.Unmarshal(r.Data, &row))
		got = append(got, row.ID)
	}
	return got
}

func lastState(t *testing.T, out *testkit.Output) streamState {
	t.Helper()
	states := out.States()
	require.NotEmpty(t, states)
	var st streamState
	require.NoError(t, json.Unmarshal(states[len(states)-1].Stream.StreamState, &st))
	return st
}

func stateFor(t *testing.T, desc synclet.StreamDescriptor, st streamState) json.RawMessage {
	t.Helper()
	return mustJSON(t, []synclet.StateMessage{{
		Type:   synclet.StateTypeStream,
		Stream: &synclet.StreamState{StreamDescriptor: desc, StreamState: mustJSON(t, st)},
	}})
}

func cursorText(s string) *string { return &s }

func TestIncrementalResume(t *testing.T) {
	cfg := sharedMSSQL(t)
	execSQL(t, cfg, `DELETE FROM dbo.events;
INSERT INTO dbo.events (id, updated_at) VALUES (1, '2024-01-01T00:00:00'), (2, '2024-01-02T00:00:00.1234567'), (3, '2024-01-03T00:00:00')`)
	stream := discoverStream(t, cfg, "dbo", "events")
	desc := stream.Descriptor()

	t.Run("datetime2 cursor", func(t *testing.T) {
		cat := incrementalCatalog(stream, "updated_at")
		first := runRead(t, cfg, cat, nil)
		require.Equal(t, []int{1, 2, 3}, ids(t, first, desc))
		t3 := lastState(t, first)
		require.Equal(t, streamState{CursorField: []string{"updated_at"}, Cursor: cursorText("2024-01-03T00:00:00")}, t3)

		fromT2 := runRead(t, cfg, cat, stateFor(t, desc, streamState{CursorField: []string{"updated_at"}, Cursor: cursorText("2024-01-02T00:00:00.1234567")}))
		require.Equal(t, []int{2, 3}, ids(t, fromT2, desc), "rows with cursor >= the saved value, the equal one included")

		execSQL(t, cfg, `INSERT INTO dbo.events (id, updated_at) VALUES (4, '2024-01-04T00:00:00');
UPDATE dbo.events SET updated_at = '2024-01-05T00:00:00' WHERE id = 1`)
		resumed := runRead(t, cfg, cat, stateFor(t, desc, t3))
		require.Equal(t, []int{3, 4, 1}, ids(t, resumed, desc))
		require.Equal(t, streamState{CursorField: []string{"updated_at"}, Cursor: cursorText("2024-01-05T00:00:00")}, lastState(t, resumed))

		changed := runRead(t, cfg, cat, stateFor(t, desc, streamState{CursorField: []string{"id"}, Cursor: cursorText("3")}))
		require.ElementsMatch(t, []int{1, 2, 3, 4}, ids(t, changed, desc))
		var warned bool
		for _, l := range changed.Logs() {
			warned = warned || (l.Level == synclet.LogLevelWarn && strings.Contains(l.Message, "cursor field changed"))
		}
		require.True(t, warned, "a WARN log about the changed cursor field")

		execSQL(t, cfg, `INSERT INTO dbo.events (id, updated_at) VALUES (5, NULL)`)
		t.Cleanup(func() { execSQL(t, cfg, `DELETE FROM dbo.events WHERE id = 5`) })
		require.Equal(t, []int{5, 2, 3, 4, 1}, ids(t, runRead(t, cfg, cat, nil), desc), "NULL sorts first in SQL Server")
		require.Equal(t, []int{1}, ids(t, runRead(t, cfg, cat, stateFor(t, desc, lastState(t, resumed))), desc), "NULL cursor only in the first read")
	})

	t.Run("rowversion cursor", func(t *testing.T) {
		cat := incrementalCatalog(stream, "rv")
		first := runRead(t, cfg, cat, nil)
		require.Len(t, ids(t, first, desc), 4)
		saved := lastState(t, first)
		require.Equal(t, []string{"rv"}, saved.CursorField)
		require.Regexp(t, `^0x[0-9A-F]{16}$`, *saved.Cursor)
		last := ids(t, first, desc)[3]

		again := runRead(t, cfg, cat, stateFor(t, desc, saved))
		require.Equal(t, []int{last}, ids(t, again, desc), "only the row at the saved rowversion")

		execSQL(t, cfg, `UPDATE dbo.events SET updated_at = SYSDATETIME() WHERE id = 2`)
		resumed := runRead(t, cfg, cat, stateFor(t, desc, saved))
		require.Equal(t, []int{last, 2}, ids(t, resumed, desc), "an update moves the rowversion forward")
		require.Greater(t, *lastState(t, resumed).Cursor, *saved.Cursor)
	})
}

func TestIncrementalCheckpoints(t *testing.T) {
	cfg := sharedMSSQL(t)
	execSQL(t, cfg, `SELECT TOP 25000 CAST(ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS int) AS id
INTO dbo.big FROM sys.all_columns a CROSS JOIN sys.all_columns b`)
	t.Cleanup(func() { execSQL(t, cfg, `DROP TABLE dbo.big`) })
	stream := discoverStream(t, cfg, "dbo", "big")

	out := runRead(t, cfg, incrementalCatalog(stream, "id"), nil)
	require.Len(t, out.Records(stream.Descriptor()), 25000)
	var cursors []string
	for _, s := range out.States() {
		var st streamState
		require.NoError(t, json.Unmarshal(s.Stream.StreamState, &st))
		cursors = append(cursors, *st.Cursor)
	}
	require.Equal(t, []string{"10000", "20000", "25000"}, cursors)
}

func TestReadConfigErrors(t *testing.T) {
	cfg := sharedMSSQL(t)
	users := discoverStream(t, cfg, "dbo", "users")
	types := discoverStream(t, cfg, "dbo", "types")
	run := func(cs synclet.ConfiguredStream) *testkit.Output {
		out, err := testkit.RunSource(t.Context(), Source{}, synclet.Request{
			Command: synclet.CommandRead, Config: mustJSON(t, cfg),
			Catalog: mustJSON(t, &synclet.ConfiguredCatalog{Streams: []synclet.ConfiguredStream{cs}}),
		})
		require.NoError(t, err)
		require.Error(t, out.Err)
		traces := out.ErrorTraces()
		require.Len(t, traces, 1)
		require.Equal(t, synclet.FailureTypeConfigError, traces[0].FailureType)
		return out
	}

	missing := users
	missing.Name = "nope"
	require.ErrorContains(t, run(synclet.ConfiguredStream{Stream: missing, SyncMode: synclet.SyncModeFullRefresh}).Err, "dbo.nope: no such table")

	extra := users
	extra.JSONSchema = json.RawMessage(`{"type":"object","properties":{"id":{},"ghost":{}}}`)
	require.ErrorContains(t, run(synclet.ConfiguredStream{Stream: extra, SyncMode: synclet.SyncModeFullRefresh}).Err, `column "ghost" does not exist`)

	require.ErrorContains(t, run(synclet.ConfiguredStream{Stream: users, SyncMode: synclet.SyncModeIncremental}).Err, "needs a cursor_field")

	for _, col := range []string{"c_xml", "c_text", "c_geography", "c_variant"} {
		err := run(synclet.ConfiguredStream{Stream: types, SyncMode: synclet.SyncModeIncremental, CursorField: []string{col}}).Err
		require.ErrorContains(t, err, fmt.Sprintf("cursor column %q is of type", col))
	}
}

func TestConformance(t *testing.T) {
	cfg := sharedMSSQL(t)
	broken := cfg
	broken.Password = "wrong"
	users := discoverStream(t, cfg, "dbo", "users")
	types := discoverStream(t, cfg, "dbo", "types")
	view := discoverStream(t, cfg, "dbo", "user_names")
	full := func(s synclet.Stream) synclet.ConfiguredStream {
		return synclet.ConfiguredStream{Stream: s, SyncMode: synclet.SyncModeFullRefresh, DestinationSyncMode: synclet.DestinationSyncModeOverwrite}
	}

	t.Run("full refresh", func(t *testing.T) {
		testkit.RunSourceSuite(t, Source{}, testkit.SourceSuite{
			Config:       mustJSON(t, cfg),
			BrokenConfig: mustJSON(t, broken),
			Catalog:      &synclet.ConfiguredCatalog{Streams: []synclet.ConfiguredStream{full(users), full(types), full(view)}},
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

// encryptOption reports whether a connection made with cfg's ssl_method is
// encrypted. It connects as sa, which may read sys.dm_exec_connections.
func encryptOption(t *testing.T, cfg Config) string {
	t.Helper()
	cfg.Database, cfg.Username, cfg.Password = "master", "sa", msSAPassword
	return queryString(t, cfg, `SELECT encrypt_option FROM sys.dm_exec_connections WHERE session_id = @@SPID`)
}

func TestSSL(t *testing.T) {
	cfg := sharedMSSQL(t)
	with := func(m SSLMethod) Config {
		c := cfg
		c.SSLMethod = m
		return c
	}
	t.Run("trust server certificate encrypts", func(t *testing.T) {
		c := with(SSLMethod{SSLMethod: sslTrust})
		require.NoError(t, Source{}.Check(t.Context(), c))
		require.Equal(t, "TRUE", encryptOption(t, c))
	})
	t.Run("unencrypted", func(t *testing.T) {
		c := with(SSLMethod{SSLMethod: sslUnencrypted})
		require.NoError(t, Source{}.Check(t.Context(), c))
		require.Equal(t, "FALSE", encryptOption(t, c))
	})
	t.Run("verify with an unrelated CA", func(t *testing.T) {
		err := Source{}.Check(t.Context(), with(SSLMethod{SSLMethod: sslVerify, Certificate: newTestCA(t).certPEM}))
		require.ErrorContains(t, err, "server certificate of")
		require.ErrorContains(t, err, "is not trusted (ssl_method encrypted_verify_certificate)")
	})
	t.Run("verify against the system CAs", func(t *testing.T) {
		require.ErrorContains(t, Source{}.Check(t.Context(), with(SSLMethod{SSLMethod: sslVerify})), "is not trusted")
	})
}

func TestSSHTunnel(t *testing.T) {
	cfg := sharedMSSQL(t)
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	sshPub, err := ssh.NewPublicKey(pub)
	require.NoError(t, err)
	block, err := ssh.MarshalPrivateKey(priv, "")
	require.NoError(t, err)
	keyPEM := string(pem.EncodeToMemory(block))

	// db.internal resolves nowhere but on the SSH server, so a read that
	// works went through the tunnel.
	dbAddr := net.JoinHostPort(cfg.Host, fmt.Sprint(cfg.Port))
	if cfg.Host == "localhost" {
		dbAddr = net.JoinHostPort("127.0.0.1", fmt.Sprint(cfg.Port))
	}
	srv := startSSHServer(t, "tunnel-pass", sshPub, map[string]string{"db.internal:1433": dbAddr})
	host, port := srv.hostPort(t)
	tunneled := cfg
	tunneled.Host, tunneled.Port = "db.internal", 1433

	for _, tm := range []TunnelMethod{
		{TunnelMethod: tunnelPassword, TunnelHost: host, TunnelPort: port, TunnelUser: "bastion", TunnelUserPassword: "tunnel-pass"},
		{TunnelMethod: tunnelKey, TunnelHost: host, TunnelPort: port, TunnelUser: "bastion", SSHKey: keyPEM},
	} {
		t.Run(tm.TunnelMethod, func(t *testing.T) {
			c := tunneled
			c.TunnelMethod = tm
			require.NoError(t, Source{}.Check(t.Context(), c))
			users := discoverStream(t, c, "dbo", "users")
			out := runRead(t, c, &synclet.ConfiguredCatalog{Streams: []synclet.ConfiguredStream{{
				Stream: users, SyncMode: synclet.SyncModeFullRefresh, DestinationSyncMode: synclet.DestinationSyncModeOverwrite,
			}}}, nil)
			require.Len(t, out.Records(users.Descriptor()), 4)
		})
	}

	t.Run("wrong tunnel password", func(t *testing.T) {
		c := tunneled
		c.TunnelMethod = TunnelMethod{TunnelMethod: tunnelPassword, TunnelHost: host, TunnelPort: port, TunnelUser: "bastion", TunnelUserPassword: "nope"}
		require.ErrorContains(t, Source{}.Check(t.Context(), c), `cannot open SSH tunnel as "bastion"`)
	})
	pinned := func(hostKey string) Config {
		c := tunneled
		c.TunnelMethod = TunnelMethod{TunnelMethod: tunnelKey, TunnelHost: host, TunnelPort: port, TunnelUser: "bastion", SSHKey: keyPEM, TunnelHostKey: hostKey}
		return c
	}
	authorizedKey := strings.TrimSpace(string(ssh.MarshalAuthorizedKey(srv.hostKey)))
	t.Run("pinned host key", func(t *testing.T) {
		require.NoError(t, Source{}.Check(t.Context(), pinned(authorizedKey)))
		require.NoError(t, Source{}.Check(t.Context(), pinned(authorizedKey+" bastion.example.com")))
		require.NoError(t, Source{}.Check(t.Context(), pinned(knownhosts.Line([]string{knownhosts.Normalize(srv.addr)}, srv.hostKey))))
	})
	t.Run("server with two host keys", func(t *testing.T) {
		ecdsaKey := strings.TrimSpace(string(ssh.MarshalAuthorizedKey(srv.ecdsaKey)))
		require.NoError(t, Source{}.Check(t.Context(), pinned(authorizedKey)), "the Ed25519 key, which the client does not prefer")
		require.NoError(t, Source{}.Check(t.Context(), pinned(ecdsaKey)))
		addr := []string{knownhosts.Normalize(srv.addr)}
		otherPub, _, err := ed25519.GenerateKey(rand.Reader)
		require.NoError(t, err)
		other, err := ssh.NewPublicKey(otherPub)
		require.NoError(t, err)
		keys := "# bastion\n" + knownhosts.Line(addr, other) + "\n\n" + knownhosts.Line(addr, srv.ecdsaKey) + "\n" + knownhosts.Line(addr, srv.hostKey) + "\n"
		require.NoError(t, Source{}.Check(t.Context(), pinned(keys)), "a key on any line")
	})
	t.Run("mismatched host key", func(t *testing.T) {
		otherPub, _, err := ed25519.GenerateKey(rand.Reader)
		require.NoError(t, err)
		other, err := ssh.NewPublicKey(otherPub)
		require.NoError(t, err)
		err = Source{}.Check(t.Context(), pinned(string(ssh.MarshalAuthorizedKey(other))))
		require.ErrorContains(t, err, "host key mismatch")
		err = Source{}.Check(t.Context(), pinned("@revoked "+knownhosts.Line([]string{knownhosts.Normalize(srv.addr)}, srv.hostKey)))
		require.ErrorContains(t, err, "@revoked")
	})
	t.Run("unreachable tunnel host", func(t *testing.T) {
		c := tunneled
		c.TunnelMethod = TunnelMethod{TunnelMethod: tunnelPassword, TunnelHost: "127.0.0.1", TunnelPort: closedPort(t), TunnelUser: "bastion", TunnelUserPassword: "x"}
		require.ErrorContains(t, Source{}.Check(t.Context(), c), "cannot reach SSH tunnel host")
	})
}
