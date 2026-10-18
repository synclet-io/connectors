package main

import (
	"context"
	"encoding/json"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
	"github.com/synclet-io/synclet-go-sdk/testkit"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/clickhouse"
)

const (
	chImage    = "clickhouse/clickhouse-server:25.8"
	chUser     = "synclet"
	chPassword = "synclet-test"
)

var (
	containersMu sync.Mutex
	containers   []testcontainers.Container
	sharedOnce   sync.Once
	sharedCfg    Config
	sharedHTTP   int
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
		t.Skip("starts ClickHouse in Docker; skipped under -short")
	}
}

// runClickHouse starts a ClickHouse container whose user may manage other
// users and returns a native-protocol configuration for it and its mapped
// HTTP port.
func runClickHouse() (Config, int, error) {
	ctx := context.Background()
	ctr, err := clickhouse.Run(ctx, chImage,
		clickhouse.WithUsername(chUser),
		clickhouse.WithPassword(chPassword),
		testcontainers.WithEnv(map[string]string{"CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT": "1"}),
	)
	if ctr != nil {
		containersMu.Lock()
		containers = append(containers, ctr)
		containersMu.Unlock()
	}
	if err != nil {
		return Config{}, 0, err
	}
	host, err := ctr.Host(ctx)
	if err != nil {
		return Config{}, 0, err
	}
	native, err := ctr.MappedPort(ctx, "9000/tcp")
	if err != nil {
		return Config{}, 0, err
	}
	httpPort, err := ctr.MappedPort(ctx, "8123/tcp")
	if err != nil {
		return Config{}, 0, err
	}
	return Config{
		Host:     host,
		Port:     int(native.Num()),
		Protocol: protocolNative,
		Database: "default",
		Username: chUser,
		Password: chPassword,
	}, int(httpPort.Num()), nil
}

// sharedClickHouse is one server every Docker test writes to; each test uses
// its own database, created here.
func sharedClickHouse(t *testing.T, database string) Config {
	t.Helper()
	needDocker(t)
	sharedOnce.Do(func() { sharedCfg, sharedHTTP, sharedErr = runClickHouse() })
	require.NoError(t, sharedErr)
	cfg := sharedCfg
	execSQL(t, cfg, "CREATE DATABASE IF NOT EXISTS "+quote(database))
	cfg.Database = database
	return cfg
}

// overHTTP returns cfg pointed at the server's HTTP interface.
func overHTTP(cfg Config) Config {
	cfg.Protocol = protocolHTTP
	cfg.Port = sharedHTTP
	return cfg
}

func execSQL(t *testing.T, cfg Config, sql string, args ...any) {
	t.Helper()
	ctx := context.Background()
	d, err := connect(ctx, cfg)
	require.NoError(t, err)
	defer d.close()
	require.NoError(t, d.Exec(ctx, sql, args...))
}

// queryStrings runs sql, whose columns must be Nullable(String) or String,
// and returns every row with NULL as "<NULL>".
func queryStrings(t *testing.T, cfg Config, sql string, args ...any) [][]string {
	t.Helper()
	ctx := context.Background()
	d, err := connect(ctx, cfg)
	require.NoError(t, err)
	defer d.close()
	rows, err := d.Query(ctx, sql, args...)
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()
	n := len(rows.Columns())
	var out [][]string
	for rows.Next() {
		vals := make([]*string, n)
		dest := make([]any, n)
		for i := range vals {
			dest[i] = &vals[i]
		}
		require.NoError(t, rows.Scan(dest...))
		row := make([]string, n)
		for i, v := range vals {
			if v == nil {
				row[i] = "<NULL>"
				continue
			}
			row[i] = *v
		}
		out = append(out, row)
	}
	require.NoError(t, rows.Err())
	return out
}

func countRows(t *testing.T, cfg Config, table string) int {
	t.Helper()
	rows := queryStrings(t, cfg, "SELECT toString(count()) FROM "+quote(cfg.Database, table))
	n, err := strconv.Atoi(rows[0][0])
	require.NoError(t, err)
	return n
}

func tableExists(t *testing.T, cfg Config, table string) bool {
	t.Helper()
	rows := queryStrings(t, cfg, "SELECT toString(count()) FROM system.tables WHERE database = ? AND name = ?", cfg.Database, table)
	return rows[0][0] == "1"
}

// stream builds a configured stream from a JSON schema.
func stream(name, schema string, mode synclet.DestinationSyncMode, pk [][]string, cursor []string) synclet.ConfiguredStream {
	return synclet.ConfiguredStream{
		Stream:              synclet.Stream{Name: name, JSONSchema: json.RawMessage(schema)},
		SyncMode:            synclet.SyncModeIncremental,
		DestinationSyncMode: mode,
		PrimaryKey:          pk,
		CursorField:         cursor,
	}
}

func rec(streamName, data string) string {
	return `{"type":"RECORD","record":{"stream":"` + streamName + `","data":` + data + `,"emitted_at":1700000000000}}`
}

func state(streamName, value string) string {
	return `{"type":"STATE","state":{"type":"STREAM","stream":{"stream_descriptor":{"name":"` + streamName + `"},"stream_state":` + value + `}}}`
}

// runWrite runs a write through the SDK's transport with lines as stdin.
func runWrite(t *testing.T, d Destination, cfg Config, streams []synclet.ConfiguredStream, lines ...string) *testkit.Output {
	t.Helper()
	catalog, err := json.Marshal(synclet.ConfiguredCatalog{Streams: streams})
	require.NoError(t, err)
	config, err := json.Marshal(cfg)
	require.NoError(t, err)
	out, err := testkit.RunDestination(t.Context(), d, synclet.Request{
		Command: synclet.CommandWrite,
		Config:  config,
		Catalog: catalog,
		Input:   strings.NewReader(strings.Join(lines, "\n") + "\n"),
	})
	require.NoError(t, err)
	return out
}

// stateValues returns the stream_state of every echoed STREAM state.
func stateValues(out *testkit.Output) []string {
	var vals []string
	for _, st := range out.States() {
		if st.Stream != nil {
			vals = append(vals, string(st.Stream.StreamState))
		}
	}
	return vals
}

func testDestination(maxRows int) Destination {
	d := NewDestination()
	d.maxRows = maxRows
	return d
}

// closedPort returns a local TCP port nothing listens on.
func closedPort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := l.Addr().(*net.TCPAddr).Port
	require.NoError(t, l.Close())
	return port
}
