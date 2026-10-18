package main

import (
	"context"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
	"github.com/synclet-io/synclet-go-sdk/testkit"
	"github.com/testcontainers/testcontainers-go"
	tcmysql "github.com/testcontainers/testcontainers-go/modules/mysql"
)

const (
	testImage    = "mysql:8.0.40"
	testDB       = "testdb"
	testUser     = "root"
	testPassword = "testpassword"
)

var (
	containersMu sync.Mutex
	containers   []testcontainers.Container
	sharedOnce   sync.Once
	sharedCfg    Config
	sharedErr    error
)

func TestMain(m *testing.M) {
	code := m.Run()
	containersMu.Lock()
	for _, c := range containers {
		_ = testcontainers.TerminateContainer(c)
	}
	containersMu.Unlock()
	os.Exit(code)
}

func needDocker(t *testing.T) {
	t.Helper()
	if testing.Short() {
		t.Skip("starts MySQL in Docker; skipped under -short")
	}
}

// runMySQL starts a MySQL 8.0 container loaded with testdata/fixture.sql and
// returns a configuration that reaches it as root. MySQL 8.0 runs with
// log_bin=ON, binlog_format=ROW and binlog_row_image=FULL, which CDC needs.
func runMySQL() (Config, error) {
	ctx := context.Background()
	ctr, err := tcmysql.Run(ctx, testImage,
		tcmysql.WithDatabase(testDB),
		tcmysql.WithUsername(testUser),
		tcmysql.WithPassword(testPassword),
		tcmysql.WithScripts(filepath.Join("testdata", "fixture.sql")),
	)
	if ctr != nil {
		containersMu.Lock()
		containers = append(containers, ctr)
		containersMu.Unlock()
	}
	if err != nil {
		return Config{}, err
	}
	host, err := ctr.Host(ctx)
	if err != nil {
		return Config{}, err
	}
	port, err := ctr.MappedPort(ctx, "3306/tcp")
	if err != nil {
		return Config{}, err
	}
	return Config{
		Host:     host,
		Port:     int(port.Num()),
		Database: testDB,
		Username: testUser,
		Password: testPassword,
	}, nil
}

// sharedMySQL is the one container every test reads from.
func sharedMySQL(t *testing.T) Config {
	t.Helper()
	needDocker(t)
	sharedOnce.Do(func() { sharedCfg, sharedErr = runMySQL() })
	require.NoError(t, sharedErr)
	return sharedCfg
}

func execSQL(t *testing.T, cfg Config, statements ...string) {
	t.Helper()
	d, err := connect(t.Context(), cfg)
	require.NoError(t, err)
	defer func() { require.NoError(t, d.close()) }()
	for _, s := range statements {
		_, err := d.ExecContext(t.Context(), s)
		require.NoError(t, err, s)
	}
}

func discoverStream(t *testing.T, cfg Config, name string) synclet.Stream {
	t.Helper()
	cat, err := Source{}.Discover(t.Context(), cfg)
	require.NoError(t, err)
	for _, s := range cat.Streams {
		if s.Name == name {
			return s
		}
	}
	t.Fatalf("discover: no stream %s", name)
	return synclet.Stream{}
}

func mustJSON(t *testing.T, v any) json.RawMessage {
	t.Helper()
	b, err := json.Marshal(v)
	require.NoError(t, err)
	return b
}

func runRead(t *testing.T, cfg Config, cat *synclet.ConfiguredCatalog, state json.RawMessage) *testkit.Output {
	t.Helper()
	out := tryRead(t, cfg, cat, state)
	require.NoError(t, out.Err)
	return out
}

func tryRead(t *testing.T, cfg Config, cat *synclet.ConfiguredCatalog, state json.RawMessage) *testkit.Output {
	t.Helper()
	out, err := testkit.RunSource(t.Context(), Source{}, synclet.Request{
		Command: synclet.CommandRead,
		Config:  mustJSON(t, cfg),
		Catalog: mustJSON(t, cat),
		State:   state,
	})
	require.NoError(t, err)
	return out
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

func incrementalCatalog(stream synclet.Stream, cursor string) *synclet.ConfiguredCatalog {
	return &synclet.ConfiguredCatalog{Streams: []synclet.ConfiguredStream{{
		Stream:              stream,
		SyncMode:            synclet.SyncModeIncremental,
		CursorField:         []string{cursor},
		DestinationSyncMode: synclet.DestinationSyncModeAppendDedup,
		PrimaryKey:          [][]string{{"id"}},
	}}}
}

// rowsOf returns the records of a stream as maps of raw JSON values.
func rowsOf(t *testing.T, out *testkit.Output, desc synclet.StreamDescriptor) []map[string]json.RawMessage {
	t.Helper()
	var rows []map[string]json.RawMessage
	for _, r := range out.Records(desc) {
		var row map[string]json.RawMessage
		require.NoError(t, json.Unmarshal(r.Data, &row))
		rows = append(rows, row)
	}
	return rows
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

func stateFor(t *testing.T, desc synclet.StreamDescriptor, st any) json.RawMessage {
	t.Helper()
	return mustJSON(t, []synclet.StateMessage{{
		Type:   synclet.StateTypeStream,
		Stream: &synclet.StreamState{StreamDescriptor: desc, StreamState: mustJSON(t, st)},
	}})
}

func closedPort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := l.Addr().(*net.TCPAddr).Port
	require.NoError(t, l.Close())
	return port
}
