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
	"time"

	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
	"github.com/synclet-io/synclet-go-sdk/testkit"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.mongodb.org/mongo-driver/v2/bson"
	"golang.org/x/crypto/ssh"
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
	e := sharedMongo(t)
	src := e.src()
	require.NoError(t, src.Check(t.Context(), e.cfg))

	with := func(f func(*DatabaseConfig)) Config {
		c := e.cfg
		f(&c.DatabaseConfig)
		return c
	}
	fakeDNS(t, nil, nil)
	cases := []struct {
		name string
		src  Source
		cfg  Config
		want string
	}{
		{"wrong password", src, with(func(d *DatabaseConfig) { d.Password = "bad-test-pw" }),
			`cannot log in as "synclet": wrong user name or password`},
		{"unknown user", src, with(func(d *DatabaseConfig) { d.Username = "nobody" }),
			`cannot log in as "nobody": wrong user name or password`},
		{"closed port", Source{selectTimeout: 3 * time.Second}, with(func(d *DatabaseConfig) {
			d.ConnectionString = fmt.Sprintf("mongodb://127.0.0.1:%d/?directConnection=true", closedPort(t))
		}), "cannot reach 127.0.0.1:"},
		{"unresolvable SRV record", src, with(func(d *DatabaseConfig) {
			d.ConnectionString = "mongodb+srv://nowhere.example.test/"
		}), "cannot resolve SRV record _mongodb._tcp.nowhere.example.test"},
		{"no permission on the database", src, with(func(d *DatabaseConfig) { d.Username, d.Password = "noperm", "noperm-pw" }),
			`user "noperm" may not read database "synclet"`},
		{"no permission on the collection", src, with(func(d *DatabaseConfig) { d.Username, d.Password = "lister", "lister-pw" }),
			`user "lister" may not read collection "events" of database "synclet"`},
		{"invalid config", src, with(func(d *DatabaseConfig) { d.Database = "" }),
			"database_config.database is required"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.src.Check(t.Context(), tc.cfg)
			require.ErrorContains(t, err, tc.want)
			require.NotContains(t, err.Error(), tc.cfg.DatabaseConfig.Password)
		})
	}
}

func TestDiscoverSRV(t *testing.T) {
	e := sharedMongo(t)
	fakeDNS(t,
		map[string][]*net.SRV{"cluster0.example.test": {{Target: "shard-00-00.example.test.", Port: 27017}}},
		map[string][]string{"cluster0.example.test": {"authSource=admin&replicaSet=" + replicaSet}},
	)
	cfg := e.cfg
	cfg.DatabaseConfig.ClusterType = clusterAtlas
	cfg.DatabaseConfig.ConnectionString = "mongodb+srv://cluster0.example.test/" + testDatabase + "?tls=false"

	src := e.src()
	require.NoError(t, src.Check(t.Context(), cfg))
	cat, err := src.Discover(t.Context(), cfg)
	require.NoError(t, err)
	var names []string
	for _, s := range cat.Streams {
		names = append(names, s.Name)
		require.Equal(t, testDatabase, s.Namespace)
		require.Equal(t, [][]string{{"_id"}}, s.SourceDefinedPrimaryKey)
		require.Equal(t, []string{"_id"}, s.DefaultCursorField)
	}
	require.Subset(t, names, []string{"events", "mixed", "types", "users"})
	require.NotContains(t, names, "user_names", "views are no streams")

	mixed := discoverStream(t, src, cfg, "mixed")
	out := runRead(t, src, cfg, fullRefresh(mixed), nil)
	records := out.Records(mixed.Descriptor())
	require.Len(t, records, len(mixedSample()))
	for _, r := range records {
		validate(t, mixed.JSONSchema, r.Data)
	}
}

func TestTypesRoundTrip(t *testing.T) {
	e := sharedMongo(t)
	src := e.src()
	types := discoverStream(t, src, e.cfg, "types")
	out := runRead(t, src, e.cfg, fullRefresh(types), nil)
	records := out.Records(types.Descriptor())
	require.Len(t, records, 1)
	validate(t, types.JSONSchema, records[0].Data)
	var got map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(records[0].Data, &got))
	for field, want := range map[string]string{
		"_id":           `"65a1b2c3d4e5f60718293a4b"`,
		"c_decimal":     `12345678901234567890.123456789`,
		"c_decimal_nan": `null`,
		"c_int64":       `9007199254740993`,
		"c_double_nan":  `null`,
		"c_date":        `"2024-02-29T13:45:30.123Z"`,
		"c_binary":      `"AAEC/w=="`,
	} {
		require.Equal(t, want, string(got[field]), field)
	}
}

func incrementalCatalog(stream synclet.Stream, cursor string) *synclet.ConfiguredCatalog {
	return &synclet.ConfiguredCatalog{Streams: []synclet.ConfiguredStream{{
		Stream:              stream,
		SyncMode:            synclet.SyncModeIncremental,
		CursorField:         []string{cursor},
		DestinationSyncMode: synclet.DestinationSyncModeAppendDedup,
		PrimaryKey:          [][]string{{"_id"}},
	}}}
}

// ns returns the n field of the records of desc in emitted order.
func ns(t *testing.T, out *testkit.Output, desc synclet.StreamDescriptor) []int {
	t.Helper()
	got := []int{}
	for _, r := range out.Records(desc) {
		var doc struct {
			N int `json:"n"`
		}
		require.NoError(t, json.Unmarshal(r.Data, &doc))
		got = append(got, doc.N)
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

func stateFor(t *testing.T, desc synclet.StreamDescriptor, st any) json.RawMessage {
	t.Helper()
	return mustJSON(t, []synclet.StateMessage{{
		Type:   synclet.StateTypeStream,
		Stream: &synclet.StreamState{StreamDescriptor: desc, StreamState: mustJSON(t, st)},
	}})
}

func dateCursor(d int) string {
	ms := time.Date(2024, 1, d, 0, 0, 0, 0, time.UTC).UnixMilli()
	return fmt.Sprintf(`{"$date":{"$numberLong":"%d"}}`, ms)
}

func TestIncrementalResume(t *testing.T) {
	e := sharedMongo(t)
	src := e.src()
	coll := rootClient(t, e).Database(testDatabase).Collection("events")
	reset := func(t *testing.T) {
		t.Helper()
		_, err := coll.DeleteMany(t.Context(), bson.D{})
		require.NoError(t, err)
		_, err = coll.InsertMany(t.Context(), eventDocs(1, 2, 3))
		require.NoError(t, err)
	}
	stream := discoverStream(t, src, e.cfg, "events")
	desc := stream.Descriptor()

	t.Run("updated_at cursor", func(t *testing.T) {
		reset(t)
		cat := incrementalCatalog(stream, "updated_at")
		first := runRead(t, src, e.cfg, cat, nil)
		require.Equal(t, []int{1, 2, 3}, ns(t, first, desc))
		t3 := lastState(t, first)
		require.Equal(t, []string{"updated_at"}, t3.CursorField)
		require.JSONEq(t, dateCursor(3), string(t3.Cursor))

		fromT2 := runRead(t, src, e.cfg, cat, stateFor(t, desc, streamState{CursorField: []string{"updated_at"}, Cursor: json.RawMessage(dateCursor(2))}))
		require.Equal(t, []int{2, 3}, ns(t, fromT2, desc), "documents with cursor >= the saved value, the equal one included")

		_, err := coll.InsertMany(t.Context(), eventDocs(4))
		require.NoError(t, err)
		_, err = coll.UpdateOne(t.Context(), bson.D{{Key: "n", Value: 1}},
			bson.D{{Key: "$set", Value: bson.D{{Key: "updated_at", Value: bson.NewDateTimeFromTime(time.Date(2024, 1, 5, 0, 0, 0, 0, time.UTC))}}}})
		require.NoError(t, err)
		resumed := runRead(t, src, e.cfg, cat, stateFor(t, desc, t3))
		require.Equal(t, []int{3, 4, 1}, ns(t, resumed, desc))
		require.JSONEq(t, dateCursor(5), string(lastState(t, resumed).Cursor))

		changed := runRead(t, src, e.cfg, cat, stateFor(t, desc, streamState{CursorField: []string{"n"}, Cursor: json.RawMessage(`{"$numberInt":"3"}`)}))
		require.Equal(t, []int{2, 3, 4, 1}, ns(t, changed, desc))
		requireWarn(t, changed, "cursor field changed")

		_, err = coll.InsertOne(t.Context(), bson.D{{Key: "n", Value: int32(5)}})
		require.NoError(t, err)
		err = readConfigError(t, src, e.cfg, cat, nil)
		require.ErrorContains(t, err, `1 documents of collection "events" have no value in cursor field "updated_at"`)
		require.ErrorContains(t, readConfigError(t, src, e.cfg, cat, stateFor(t, desc, lastState(t, resumed))), "allow_missing_cursor")

		allow := e.cfg
		allow.AllowMissingCursor = true
		skipped := runRead(t, src, allow, cat, nil)
		require.Equal(t, []int{2, 3, 4, 1}, ns(t, skipped, desc), "a missing cursor is skipped in the first read too")
		requireWarn(t, skipped, "skipping documents without a cursor value", "count=1")
		again := runRead(t, src, allow, cat, stateFor(t, desc, lastState(t, resumed)))
		require.Equal(t, []int{1}, ns(t, again, desc))
		requireWarn(t, again, "skipping documents without a cursor value", "count=1")
	})

	t.Run("_id cursor", func(t *testing.T) {
		reset(t)
		cat := incrementalCatalog(stream, "_id")
		first := runRead(t, src, e.cfg, cat, nil)
		require.Equal(t, []int{1, 2, 3}, ns(t, first, desc))
		saved := lastState(t, first)
		require.Equal(t, []string{"_id"}, saved.CursorField)
		require.Regexp(t, `^\{"\$oid":"[0-9a-f]{24}"\}$`, string(saved.Cursor))

		require.Equal(t, []int{3}, ns(t, runRead(t, src, e.cfg, cat, stateFor(t, desc, saved)), desc), "only the document at the saved _id")

		_, err := coll.InsertMany(t.Context(), eventDocs(4, 5))
		require.NoError(t, err)
		resumed := runRead(t, src, e.cfg, cat, stateFor(t, desc, saved))
		require.Equal(t, []int{3, 4, 5}, ns(t, resumed, desc))
		require.NotEqual(t, string(saved.Cursor), string(lastState(t, resumed).Cursor))
	})
	reset(t)
}

// requireWarn asserts a WARN log whose message holds every one of parts.
func requireWarn(t *testing.T, out *testkit.Output, parts ...string) {
	t.Helper()
	var logs []string
	for _, l := range out.Logs() {
		if l.Level != synclet.LogLevelWarn {
			continue
		}
		logs = append(logs, l.Message)
		match := true
		for _, p := range parts {
			match = match && strings.Contains(l.Message, p)
		}
		if match {
			return
		}
	}
	t.Fatalf("no WARN log with %q among %q", parts, logs)
}

// readConfigError runs a read that must fail with a config error.
func readConfigError(t *testing.T, src Source, cfg Config, cat *synclet.ConfiguredCatalog, state json.RawMessage) error {
	t.Helper()
	out, err := testkit.RunSource(t.Context(), src, synclet.Request{
		Command: synclet.CommandRead, Config: mustJSON(t, cfg), Catalog: mustJSON(t, cat), State: state,
	})
	require.NoError(t, err)
	require.Error(t, out.Err)
	traces := out.ErrorTraces()
	require.Len(t, traces, 1)
	require.Equal(t, synclet.FailureTypeConfigError, traces[0].FailureType)
	return out.Err
}

func TestCursorTypes(t *testing.T) {
	e := sharedMongo(t)
	src := e.src()
	coll := rootClient(t, e).Database(testDatabase).Collection("cursors")
	dec, err := bson.ParseDecimal128("4")
	require.NoError(t, err)
	day := func(d int) bson.DateTime { return bson.NewDateTimeFromTime(time.Date(2024, 1, d, 0, 0, 0, 0, time.UTC)) }
	_, err = coll.InsertMany(t.Context(), []any{
		bson.D{{Key: "_id", Value: int32(1)}, {Key: "num", Value: int32(1)}, {Key: "when", Value: day(1)}},
		bson.D{{Key: "_id", Value: int32(2)}, {Key: "num", Value: int64(2)}, {Key: "when", Value: day(2)}},
		bson.D{{Key: "_id", Value: int32(3)}, {Key: "num", Value: 3.5}, {Key: "when", Value: "2024-01-03"}},
		bson.D{{Key: "_id", Value: int32(4)}, {Key: "num", Value: dec}, {Key: "when", Value: day(4)}},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = coll.Drop(t.Context()) })
	stream := discoverStream(t, src, e.cfg, "cursors")
	desc := stream.Descriptor()

	t.Run("numeric types are one bracket", func(t *testing.T) {
		cat := incrementalCatalog(stream, "num")
		require.Len(t, runRead(t, src, e.cfg, cat, nil).Records(desc), 4)
		saved := streamState{CursorField: []string{"num"}, Cursor: json.RawMessage(`{"$numberLong":"2"}`)}
		require.Len(t, runRead(t, src, e.cfg, cat, stateFor(t, desc, saved)).Records(desc), 3, "int64 2, double 3.5 and decimal 4")
	})
	t.Run("string and date fail", func(t *testing.T) {
		cat := incrementalCatalog(stream, "when")
		err := readConfigError(t, src, e.cfg, cat, nil)
		require.ErrorContains(t, err, `cursor field "when" of collection "cursors" holds string and date values`, "strings sort before dates")
		require.ErrorContains(t, err, "full refresh")
		saved := streamState{CursorField: []string{"when"}, Cursor: json.RawMessage(`{"$date":{"$numberLong":"0"}}`)}
		err = readConfigError(t, src, e.cfg, cat, stateFor(t, desc, saved))
		require.ErrorContains(t, err, `cursor field "when" of collection "cursors" holds date and string values`)
	})
}

func TestIncrementalCheckpoints(t *testing.T) {
	e := sharedMongo(t)
	src := e.src()
	coll := rootClient(t, e).Database(testDatabase).Collection("big")
	docs := make([]any, 25_000)
	for i := range docs {
		docs[i] = bson.D{{Key: "_id", Value: int32(i + 1)}}
	}
	_, err := coll.InsertMany(t.Context(), docs)
	require.NoError(t, err)
	t.Cleanup(func() { _ = coll.Drop(t.Context()) })
	stream := discoverStream(t, src, e.cfg, "big")

	out := runRead(t, src, e.cfg, incrementalCatalog(stream, "_id"), nil)
	require.Len(t, out.Records(stream.Descriptor()), 25_000)
	var cursors []string
	for _, s := range out.States() {
		var st streamState
		require.NoError(t, json.Unmarshal(s.Stream.StreamState, &st))
		cursors = append(cursors, string(st.Cursor))
	}
	require.Equal(t, []string{`{"$numberInt":"10000"}`, `{"$numberInt":"20000"}`, `{"$numberInt":"25000"}`}, cursors)
}

func TestReadConfigErrors(t *testing.T) {
	e := sharedMongo(t)
	src := e.src()
	users := discoverStream(t, src, e.cfg, "users")
	run := func(cs synclet.ConfiguredStream, state json.RawMessage) error {
		out, err := testkit.RunSource(t.Context(), src, synclet.Request{
			Command: synclet.CommandRead, Config: mustJSON(t, e.cfg),
			Catalog: mustJSON(t, &synclet.ConfiguredCatalog{Streams: []synclet.ConfiguredStream{cs}}),
			State:   state,
		})
		require.NoError(t, err)
		require.Error(t, out.Err)
		traces := out.ErrorTraces()
		require.Len(t, traces, 1)
		require.Equal(t, synclet.FailureTypeConfigError, traces[0].FailureType)
		return out.Err
	}
	full := func(s synclet.Stream) synclet.ConfiguredStream {
		return synclet.ConfiguredStream{Stream: s, SyncMode: synclet.SyncModeFullRefresh}
	}

	missing := users
	missing.Name = "nope"
	require.ErrorContains(t, run(full(missing), nil), "synclet.nope: no such collection")

	view := users
	view.Name = "user_names"
	require.ErrorContains(t, run(full(view), nil), "synclet.user_names: no such collection")

	other := users
	other.Namespace = "other"
	require.ErrorContains(t, run(full(other), nil), `namespace "other" is not the configured database "synclet"`)

	inc := synclet.ConfiguredStream{Stream: users, SyncMode: synclet.SyncModeIncremental, CursorField: []string{"$where"}}
	require.ErrorContains(t, run(inc, nil), `cursor field "$where" is not a field path`)

	inc.CursorField = []string{"_id"}
	require.ErrorContains(t, run(inc, stateFor(t, users.Descriptor(), map[string]any{"cursor_field": []string{"_id"}, "cursor": map[string]string{"$oid": "zz"}})),
		"invalid state")
	require.ErrorContains(t, run(inc, stateFor(t, users.Descriptor(), "not an object")), "invalid state")
}

func TestConformance(t *testing.T) {
	e := sharedMongo(t)
	src := e.src()
	broken := e.cfg
	broken.DatabaseConfig.Password = "wrong"
	users := discoverStream(t, src, e.cfg, "users")
	types := discoverStream(t, src, e.cfg, "types")
	mixed := discoverStream(t, src, e.cfg, "mixed")
	events := discoverStream(t, src, e.cfg, "events")
	full := func(s synclet.Stream) synclet.ConfiguredStream {
		return synclet.ConfiguredStream{Stream: s, SyncMode: synclet.SyncModeFullRefresh, DestinationSyncMode: synclet.DestinationSyncModeOverwrite}
	}

	t.Run("full refresh", func(t *testing.T) {
		testkit.RunSourceSuite(t, src, testkit.SourceSuite{
			Config:       mustJSON(t, e.cfg),
			BrokenConfig: mustJSON(t, broken),
			Catalog:      &synclet.ConfiguredCatalog{Streams: []synclet.ConfiguredStream{full(users), full(types), full(mixed)}},
		})
	})
	t.Run("incremental", func(t *testing.T) {
		testkit.RunSourceSuite(t, src, testkit.SourceSuite{
			Config:       mustJSON(t, e.cfg),
			BrokenConfig: mustJSON(t, broken),
			Catalog:      incrementalCatalog(events, "updated_at"),
		})
	})
}

func TestTLS(t *testing.T) {
	needDocker(t)
	ca := newTestCA(t)
	const host = "mongo.test"
	ctr, err := testcontainers.Run(t.Context(), mongoImage,
		testcontainers.WithExposedPorts("27017/tcp"),
		testcontainers.WithFiles(
			testcontainers.ContainerFile{Reader: strings.NewReader(ca.serverPEM(t, host)), ContainerFilePath: "/etc/mongo/server.pem", FileMode: 0o644},
			testcontainers.ContainerFile{Reader: strings.NewReader(ca.certPEM), ContainerFilePath: "/etc/mongo/ca.pem", FileMode: 0o644},
		),
		testcontainers.WithCmd("mongod", "--bind_ip_all", "--tlsMode", "requireTLS", "--tlsCertificateKeyFile", "/etc/mongo/server.pem",
			"--tlsCAFile", "/etc/mongo/ca.pem", "--tlsAllowConnectionsWithoutCertificates"),
		testcontainers.WithWaitStrategy(wait.ForListeningPort("27017/tcp").WithStartupTimeout(2*time.Minute)),
	)
	track(ctr)
	require.NoError(t, err)
	addr, err := ctr.PortEndpoint(t.Context(), "27017/tcp", "")
	require.NoError(t, err)
	src := redirectSource(addr)
	src.selectTimeout = 3 * time.Second

	cfg := Config{
		DatabaseConfig: DatabaseConfig{
			ConnectionString: "mongodb://" + host + ":27017/?tls=true&directConnection=true",
			Database:         "app",
		},
		TLSCACertificate: ca.certPEM,
	}
	var c *client
	require.Eventually(t, func() bool {
		c, err = src.connect(t.Context(), cfg)
		return err == nil
	}, time.Minute, time.Second, "connect with the CA")
	_, err = c.Database("app").Collection("things").InsertOne(t.Context(), bson.D{{Key: "x", Value: int32(1)}})
	c.close()
	require.NoError(t, err)

	require.NoError(t, src.Check(t.Context(), cfg), "the CA is given")

	untrusted := cfg
	untrusted.TLSCACertificate = ""
	err = src.Check(t.Context(), untrusted)
	require.ErrorContains(t, err, "server certificate of mongo.test:27017 is not trusted")

	plain := cfg
	plain.TLSCACertificate = ""
	plain.DatabaseConfig.ConnectionString = "mongodb://" + host + ":27017/?directConnection=true"
	require.ErrorContains(t, src.Check(t.Context(), plain), "cannot reach mongo.test:27017", "a plain connection to a requireTLS server")
}

func TestSSHTunnel(t *testing.T) {
	e := sharedMongo(t)
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	sshPub, err := ssh.NewPublicKey(pub)
	require.NoError(t, err)
	block, err := ssh.MarshalPrivateKey(priv, "")
	require.NoError(t, err)
	keyPEM := string(pem.EncodeToMemory(block))

	// db.internal and the member address resolve nowhere but on the SSH
	// server, so a read that works went through the tunnel.
	srv := startSSHServer(t, "tunnel-pass", sshPub, map[string]string{
		"db.internal:27017": e.addr,
		e.member:            e.addr,
	})
	host, port := srv.hostPort(t)
	tunneled := e.cfg
	tunneled.DatabaseConfig.ConnectionString = "mongodb://db.internal:27017/?replicaSet=" + replicaSet
	src := Source{selectTimeout: 10 * time.Second}

	for _, tm := range []TunnelMethod{
		{TunnelMethod: tunnelPassword, TunnelHost: host, TunnelPort: port, TunnelUser: "bastion", TunnelUserPassword: "tunnel-pass"},
		{TunnelMethod: tunnelKey, TunnelHost: host, TunnelPort: port, TunnelUser: "bastion", SSHKey: keyPEM, TunnelHostKey: string(ssh.MarshalAuthorizedKey(srv.hostKey))},
	} {
		t.Run(tm.TunnelMethod, func(t *testing.T) {
			c := tunneled
			c.TunnelMethod = tm
			require.NoError(t, src.Check(t.Context(), c))
			users := discoverStream(t, src, c, "users")
			out := runRead(t, src, c, fullRefresh(users), nil)
			require.Len(t, out.Records(users.Descriptor()), 4)
		})
	}

	t.Run("wrong tunnel password", func(t *testing.T) {
		c := tunneled
		c.TunnelMethod = TunnelMethod{TunnelMethod: tunnelPassword, TunnelHost: host, TunnelPort: port, TunnelUser: "bastion", TunnelUserPassword: "nope"}
		require.ErrorContains(t, src.Check(t.Context(), c), `cannot open SSH tunnel as "bastion"`)
	})
	t.Run("mismatched host key", func(t *testing.T) {
		otherPub, _, err := ed25519.GenerateKey(rand.Reader)
		require.NoError(t, err)
		other, err := ssh.NewPublicKey(otherPub)
		require.NoError(t, err)
		c := tunneled
		c.TunnelMethod = TunnelMethod{TunnelMethod: tunnelKey, TunnelHost: host, TunnelPort: port, TunnelUser: "bastion", SSHKey: keyPEM, TunnelHostKey: string(ssh.MarshalAuthorizedKey(other))}
		require.ErrorContains(t, src.Check(t.Context(), c), "host key mismatch")
	})
	t.Run("unreachable tunnel host", func(t *testing.T) {
		c := tunneled
		c.TunnelMethod = TunnelMethod{TunnelMethod: tunnelPassword, TunnelHost: "127.0.0.1", TunnelPort: closedPort(t), TunnelUser: "bastion", TunnelUserPassword: "x"}
		require.ErrorContains(t, src.Check(t.Context(), c), "cannot reach SSH tunnel host")
	})
}
