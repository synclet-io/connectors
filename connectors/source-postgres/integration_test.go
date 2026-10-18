package main

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"net"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
	"github.com/synclet-io/synclet-go-sdk/testkit"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
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
	base := sharedPostgres(t)
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
		{"no CONNECT privilege", with(func(c *Config) { c.Database, c.Username, c.Password = "restricted", "noconnect", "noconnect" }),
			`permission denied for database "restricted"`},
		{"no schema USAGE", with(func(c *Config) {
			c.Username, c.Password, c.Schemas = "nousage", "nousage", []string{"public", "private"}
		}),
			`user "nousage" has no USAGE privilege on schema "private"`},
		{"missing schema", with(func(c *Config) { c.Schemas = []string{"nope"} }),
			`schema "nope" does not exist`},
		{"missing database", with(func(c *Config) { c.Database = "nodb" }),
			`database "nodb" does not exist`},
		{"invalid config", with(func(c *Config) { c.Host = "" }),
			"host is required"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.ErrorContains(t, Source{}.Check(t.Context(), tc.cfg), tc.want)
		})
	}
}

func TestDiscover(t *testing.T) {
	cfg := sharedPostgres(t)

	cat, err := Source{}.Discover(t.Context(), cfg)
	require.NoError(t, err)
	byName := map[string]synclet.Stream{}
	for _, s := range cat.Streams {
		byName[s.Namespace+"."+s.Name] = s
	}
	require.Contains(t, byName, "public.users")
	require.Contains(t, byName, "public.order_lines")
	require.Contains(t, byName, "public.user_names")
	require.NotContains(t, byName, "other.hidden", "schema not configured")
	require.NotContains(t, byName, "private.secret", "schema not configured")

	users := byName["public.users"]
	require.Equal(t, [][]string{{"id"}}, users.SourceDefinedPrimaryKey)
	require.Equal(t, []synclet.SyncMode{synclet.SyncModeFullRefresh, synclet.SyncModeIncremental}, users.SupportedSyncModes)
	require.JSONEq(t, `{"type":"object","properties":{
		"id":{"type":["integer","null"]},
		"name":{"type":["string","null"]},
		"updated_at":{"type":["string","null"],"format":"date-time","airbyte_type":"timestamp_with_timezone"}}}`, string(users.JSONSchema))
	require.Equal(t, [][]string{{"order_id"}, {"line"}}, byName["public.order_lines"].SourceDefinedPrimaryKey)
	require.Empty(t, byName["public.user_names"].SourceDefinedPrimaryKey)
	require.JSONEq(t, `{"type":"object","properties":{"id":{"type":["integer","null"]},"name":{"type":["string","null"]}}}`,
		string(byName["public.user_names"].JSONSchema))

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

func TestConnectIgnoresPGEnv(t *testing.T) {
	cfg := sharedPostgres(t)
	setHostilePGEnv(t)
	d, err := connect(t.Context(), cfg)
	require.NoError(t, err)
	defer d.close()
	var app, tz, searchPath string
	require.NoError(t, d.QueryRow(t.Context(),
		`SELECT current_setting('application_name'), current_setting('TimeZone'), current_setting('search_path')`).Scan(&app, &tz, &searchPath))
	require.Equal(t, "synclet-source-postgres", app)
	require.Equal(t, "UTC", tz)
	require.Equal(t, `"$user", public`, searchPath)
}

func TestDiscoverColumnGrants(t *testing.T) {
	cfg := sharedPostgres(t)
	cfg.Username, cfg.Password, cfg.Schemas = "colonly", "colonly", []string{"colgrants"}

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
	cfg := sharedPostgres(t)
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

// typeCases is the fixture row of public.types: each column's declared schema
// and the value it reads as.
var typeCases = []struct {
	column string
	schema string
	value  string
}{
	{"c_numeric", `{"type":["number","null"]}`, `12345678901234567890.0123456789`},
	{"c_numeric_nan", `{"type":["number","null"]}`, `null`},
	{"c_float8", `{"type":["number","null"]}`, `1.5`},
	{"c_float8_inf", `{"type":["number","null"]}`, `null`},
	{"c_real", `{"type":["number","null"]}`, `0.25`},
	{"c_bigint", `{"type":["integer","null"]}`, `9007199254740993`},
	{"c_integer", `{"type":["integer","null"]}`, `-2147483648`},
	{"c_smallint", `{"type":["integer","null"]}`, `32767`},
	{"c_text", `{"type":["string","null"]}`, `"héllo"`},
	{"c_varchar", `{"type":["string","null"]}`, `"varchar"`},
	{"c_char", `{"type":["string","null"]}`, `"ab "`},
	{"c_bytea", `{"type":["string","null"]}`, `"AP8Q"`},
	{"c_json", `{}`, `{"a":1.10}`},
	{"c_jsonb", `{}`, `{"a": null, "b": [1, 2]}`},
	{"c_int_array", `{"type":["array","null"],"items":{"type":["integer","null"]}}`, `[1,2,3]`},
	{"c_text_array", `{"type":["array","null"],"items":{"type":["string","null"]}}`, `["a",null]`},
	{"c_numeric_array", `{"type":["array","null"],"items":{"type":["number","null"]}}`, `[1.5,null,null]`},
	{"c_bytea_array", `{"type":["array","null"],"items":{"type":["string","null"]}}`, `["\\x00ff"]`},
	{"c_uuid", `{"type":["string","null"]}`, `"a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"`},
	{"c_enum", `{"type":["string","null"]}`, `"happy"`},
	{"c_domain", `{"type":["integer","null"]}`, `7`},
	{"c_date", `{"type":["string","null"],"format":"date"}`, `"2024-02-29"`},
	{"c_time", `{"type":["string","null"],"airbyte_type":"time_without_timezone"}`, `"13:45:30.5"`},
	{"c_timetz", `{"type":["string","null"],"format":"time","airbyte_type":"time_with_timezone"}`, `"13:45:30+02:00"`},
	{"c_timestamp", `{"type":["string","null"],"airbyte_type":"timestamp_without_timezone"}`, `"2024-01-02T03:04:05.123456"`},
	{"c_timestamptz", `{"type":["string","null"],"format":"date-time","airbyte_type":"timestamp_with_timezone"}`, `"2024-01-02T01:04:05+00:00"`},
	{"c_interval", `{"type":["string","null"]}`, `"P1Y2M3DT4H5M6S"`},
	{"c_bool", `{"type":["boolean","null"]}`, `true`},
	{"c_point", `{"type":["string","null"]}`, `"(1,2)"`},
	{"c_inet", `{"type":["string","null"]}`, `"192.168.0.1/24"`},
	{"c_int_matrix", `{"type":["array","null"],"items":{"type":["integer","null"]}}`, `[1,2,3,4]`},
	{"c_text_matrix", `{"type":["array","null"],"items":{"type":["string","null"]}}`, `["a","b",null,"d"]`},
	{"c_date_inf", `{"type":["string","null"],"format":"date"}`, `null`},
	{"c_timestamp_inf", `{"type":["string","null"],"airbyte_type":"timestamp_without_timezone"}`, `null`},
	{"c_tstz_inf", `{"type":["string","null"],"format":"date-time","airbyte_type":"timestamp_with_timezone"}`, `null`},
	{"c_tstz_array", `{"type":["array","null"],"items":{"type":["string","null"],"format":"date-time","airbyte_type":"timestamp_with_timezone"}}`, `["2024-01-02T03:04:05+00:00",null,null]`},
}

func TestTypesRoundTrip(t *testing.T) {
	cfg := sharedPostgres(t)
	stream := discoverStream(t, cfg, "public", "types")

	var schema struct {
		Properties map[string]json.RawMessage `json:"properties"`
	}
	require.NoError(t, json.Unmarshal(stream.JSONSchema, &schema))
	require.Len(t, schema.Properties, len(typeCases)+1)
	for _, tc := range typeCases {
		require.JSONEq(t, tc.schema, string(schema.Properties[tc.column]), "schema of %s", tc.column)
	}

	out := runRead(t, cfg, &synclet.ConfiguredCatalog{Streams: []synclet.ConfiguredStream{{
		Stream: stream, SyncMode: synclet.SyncModeFullRefresh, DestinationSyncMode: synclet.DestinationSyncModeOverwrite,
	}}}, nil)
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
		got := full[tc.column]
		if tc.column == "c_jsonb" {
			require.JSONEq(t, tc.value, string(got), tc.column)
			continue
		}
		require.Equal(t, tc.value, string(got), tc.column)
		require.Equal(t, "null", string(empty[tc.column]), "%s of the NULL row", tc.column)
	}
	require.Equal(t, "null", string(empty["c_jsonb"]))
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

func TestIncrementalResume(t *testing.T) {
	cfg := sharedPostgres(t)
	execSQL(t, cfg, `TRUNCATE public.events;
INSERT INTO public.events VALUES (1, '2024-01-01 00:00:00+00'), (2, '2024-01-02 00:00:00+00'), (3, '2024-01-03 00:00:00+00')`)
	stream := discoverStream(t, cfg, "public", "events")
	desc := stream.Descriptor()
	cat := incrementalCatalog(stream, "updated_at")
	cursor := func(s string) *string { return &s }

	first := runRead(t, cfg, cat, nil)
	require.Equal(t, []int{1, 2, 3}, ids(t, first, desc))
	t3 := lastState(t, first)
	require.Equal(t, streamState{CursorField: []string{"updated_at"}, Cursor: cursor("2024-01-03 00:00:00+00")}, t3)

	fromT2 := runRead(t, cfg, cat, stateFor(t, desc, streamState{CursorField: []string{"updated_at"}, Cursor: cursor("2024-01-02 00:00:00+00")}))
	require.Equal(t, []int{2, 3}, ids(t, fromT2, desc))

	execSQL(t, cfg, `INSERT INTO public.events VALUES (4, '2024-01-04 00:00:00+00');
UPDATE public.events SET updated_at = '2024-01-05 00:00:00+00' WHERE id = 1`)
	resumed := runRead(t, cfg, cat, stateFor(t, desc, t3))
	require.Equal(t, []int{3, 4, 1}, ids(t, resumed, desc))
	require.Equal(t, streamState{CursorField: []string{"updated_at"}, Cursor: cursor("2024-01-05 00:00:00+00")}, lastState(t, resumed))

	changed := runRead(t, cfg, cat, stateFor(t, desc, streamState{CursorField: []string{"id"}, Cursor: cursor("3")}))
	require.ElementsMatch(t, []int{1, 2, 3, 4}, ids(t, changed, desc))
	var warned bool
	for _, l := range changed.Logs() {
		warned = warned || (l.Level == synclet.LogLevelWarn && strings.Contains(l.Message, "cursor field changed"))
	}
	require.True(t, warned, "a WARN log about the changed cursor field")

	t.Run("NULL cursor only in the first read", func(t *testing.T) {
		execSQL(t, cfg, `INSERT INTO public.events VALUES (5, NULL)`)
		require.Equal(t, []int{2, 3, 4, 1, 5}, ids(t, runRead(t, cfg, cat, nil), desc))
		require.Equal(t, []int{1}, ids(t, runRead(t, cfg, cat, stateFor(t, desc, lastState(t, resumed))), desc))
	})
}

func TestIncrementalCheckpoints(t *testing.T) {
	cfg := sharedPostgres(t)
	execSQL(t, cfg, `CREATE TABLE public.big AS SELECT g AS id FROM generate_series(1, 25000) g`)
	t.Cleanup(func() { execSQL(t, cfg, `DROP TABLE public.big`) })
	stream := discoverStream(t, cfg, "public", "big")

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
	cfg := sharedPostgres(t)
	users := discoverStream(t, cfg, "public", "users")
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
	require.ErrorContains(t, run(synclet.ConfiguredStream{Stream: missing, SyncMode: synclet.SyncModeFullRefresh}).Err, "public.nope: no such table")

	extra := users
	extra.JSONSchema = json.RawMessage(`{"type":"object","properties":{"id":{},"ghost":{}}}`)
	require.ErrorContains(t, run(synclet.ConfiguredStream{Stream: extra, SyncMode: synclet.SyncModeFullRefresh}).Err, `column "ghost" does not exist`)

	require.ErrorContains(t, run(synclet.ConfiguredStream{Stream: users, SyncMode: synclet.SyncModeIncremental}).Err, "needs a cursor_field")
}

func TestConformance(t *testing.T) {
	cfg := sharedPostgres(t)
	broken := cfg
	broken.Password = "wrong"
	users := discoverStream(t, cfg, "public", "users")
	types := discoverStream(t, cfg, "public", "types")
	view := discoverStream(t, cfg, "public", "user_names")
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

func sslStatus(t *testing.T, cfg Config) bool {
	t.Helper()
	d, err := connect(t.Context(), cfg)
	require.NoError(t, err)
	defer d.close()
	var ssl bool
	require.NoError(t, d.QueryRow(t.Context(), `SELECT ssl FROM pg_stat_ssl WHERE pid = pg_backend_pid()`).Scan(&ssl))
	return ssl
}

func TestSSL(t *testing.T) {
	needDocker(t)
	ca := newTestCA(t)
	certPEM, keyPEM := ca.serverCert(t)
	dir := t.TempDir()
	tlsCfg, err := runPostgres(
		postgres.WithSSLCert(writeFile(t, dir, "ca.pem", ca.certPEM), writeFile(t, dir, "server.pem", certPEM), writeFile(t, dir, "server.key", keyPEM)),
		testcontainers.WithCmd("postgres", "-c", "fsync=off", "-c", "ssl=on",
			"-c", "ssl_cert_file=/tmp/testcontainers-go/postgres/server.cert",
			"-c", "ssl_key_file=/tmp/testcontainers-go/postgres/server.key"),
	)
	require.NoError(t, err)
	tlsCfg.Host = "localhost"
	plain := sharedPostgres(t)
	otherCA := newTestCA(t)

	mode := func(base Config, m, caPEM string) Config {
		base.SSLMode = SSLMode{Mode: m, CACertificate: caPEM}
		return base
	}
	for _, tc := range []struct {
		mode    string
		ca      string
		wantSSL bool
	}{
		{sslDisable, "", false},
		{sslAllow, "", false},
		{sslPrefer, "", true},
		{sslRequire, "", true},
		{sslRequire, ca.certPEM, true},
		{sslVerifyCA, ca.certPEM, true},
		{sslVerifyFull, ca.certPEM, true},
	} {
		t.Run(fmt.Sprintf("ssl server %s ca=%t", tc.mode, tc.ca != ""), func(t *testing.T) {
			require.Equal(t, tc.wantSSL, sslStatus(t, mode(tlsCfg, tc.mode, tc.ca)))
		})
	}

	t.Run("verify-full checks the host name, verify-ca does not", func(t *testing.T) {
		byIP := tlsCfg
		byIP.Host = "127.0.0.1"
		require.ErrorContains(t, Source{}.Check(t.Context(), mode(byIP, sslVerifyFull, ca.certPEM)), "certificate")
		require.True(t, sslStatus(t, mode(byIP, sslVerifyCA, ca.certPEM)))
	})
	for _, m := range []string{sslVerifyCA, sslVerifyFull, sslRequire} {
		t.Run(m+" with a wrong CA", func(t *testing.T) {
			require.ErrorContains(t, Source{}.Check(t.Context(), mode(tlsCfg, m, otherCA.certPEM)), "certificate")
		})
	}
	t.Run("require on a server without SSL", func(t *testing.T) {
		require.ErrorContains(t, Source{}.Check(t.Context(), mode(plain, sslRequire, "")), "does not support SSL")
	})
	t.Run("prefer and allow on a server without SSL", func(t *testing.T) {
		require.False(t, sslStatus(t, mode(plain, sslPrefer, "")))
		require.False(t, sslStatus(t, mode(plain, sslAllow, "")))
	})
}

func TestSSLClientCertificate(t *testing.T) {
	needDocker(t)
	ca := newTestCA(t)
	serverPEM, serverKey := ca.serverCert(t)
	clientPEM, clientKey := ca.clientCert(t, pgUser)
	dir := t.TempDir()
	const hba = "local all all trust\nhostssl all all all scram-sha-256 clientcert=verify-full\n"
	cfg, err := runPostgres(
		postgres.WithSSLCert(writeFile(t, dir, "ca.pem", ca.certPEM), writeFile(t, dir, "server.pem", serverPEM), writeFile(t, dir, "server.key", serverKey)),
		testcontainers.WithFiles(testcontainers.ContainerFile{
			HostFilePath: writeFile(t, dir, "pg_hba.conf", hba), ContainerFilePath: "/tmp/testcontainers-go/postgres/pg_hba.conf", FileMode: 0o644,
		}),
		testcontainers.WithCmd("postgres", "-c", "fsync=off", "-c", "ssl=on",
			"-c", "ssl_cert_file=/tmp/testcontainers-go/postgres/server.cert",
			"-c", "ssl_key_file=/tmp/testcontainers-go/postgres/server.key",
			"-c", "ssl_ca_file=/tmp/testcontainers-go/postgres/ca_cert.pem",
			"-c", "hba_file=/tmp/testcontainers-go/postgres/pg_hba.conf"),
	)
	require.NoError(t, err)
	cfg.Host = "localhost"

	for _, m := range []string{sslVerifyCA, sslVerifyFull} {
		t.Run(m+" with a client certificate", func(t *testing.T) {
			c := cfg
			c.SSLMode = SSLMode{Mode: m, CACertificate: ca.certPEM, ClientCertificate: clientPEM, ClientKey: clientKey}
			require.NoError(t, Source{}.Check(t.Context(), c))
			require.True(t, sslStatus(t, c))
			users := discoverStream(t, c, "public", "users")
			require.Len(t, runRead(t, c, fullRefresh(users), nil).Records(users.Descriptor()), 4)
		})
		t.Run(m+" without a client certificate", func(t *testing.T) {
			c := cfg
			c.SSLMode = SSLMode{Mode: m, CACertificate: ca.certPEM}
			require.ErrorContains(t, Source{}.Check(t.Context(), c), "certificate")
		})
	}
}

func TestSSHTunnel(t *testing.T) {
	cfg := sharedPostgres(t)
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
	srv := startSSHServer(t, "tunnel-pass", sshPub, map[string]string{"db.internal:5432": dbAddr})
	host, port := srv.hostPort(t)
	tunneled := cfg
	tunneled.Host, tunneled.Port = "db.internal", 5432

	for _, tm := range []TunnelMethod{
		{TunnelMethod: tunnelPassword, TunnelHost: host, TunnelPort: port, TunnelUser: "bastion", TunnelUserPassword: "tunnel-pass"},
		{TunnelMethod: tunnelKey, TunnelHost: host, TunnelPort: port, TunnelUser: "bastion", SSHKey: keyPEM},
	} {
		t.Run(tm.TunnelMethod, func(t *testing.T) {
			c := tunneled
			c.TunnelMethod = tm
			require.NoError(t, Source{}.Check(t.Context(), c))
			users := discoverStream(t, c, "public", "users")
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
	t.Run("mismatched host key", func(t *testing.T) {
		otherPub, _, err := ed25519.GenerateKey(rand.Reader)
		require.NoError(t, err)
		other, err := ssh.NewPublicKey(otherPub)
		require.NoError(t, err)
		err = Source{}.Check(t.Context(), pinned(string(ssh.MarshalAuthorizedKey(other))))
		require.ErrorContains(t, err, "host key mismatch")
	})
	t.Run("unreachable tunnel host", func(t *testing.T) {
		c := tunneled
		c.TunnelMethod = TunnelMethod{TunnelMethod: tunnelPassword, TunnelHost: "127.0.0.1", TunnelPort: closedPort(t), TunnelUser: "bastion", TunnelUserPassword: "x"}
		require.ErrorContains(t, Source{}.Check(t.Context(), c), "cannot reach SSH tunnel host")
	})
}
