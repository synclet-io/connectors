package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"net"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
	"github.com/synclet-io/synclet-go-sdk/testkit"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/mysql"
)

const (
	mysqlImage    = "mysql:8.4"
	mysqlDatabase = "synclet"
	mysqlPassword = "synclet-test"
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

// runMySQL starts a MySQL 8.4 container and returns a root configuration
// that reaches it without SSL.
func runMySQL() (Config, error) {
	ctx := context.Background()
	ctr, err := mysql.Run(ctx, mysqlImage,
		mysql.WithDatabase(mysqlDatabase),
		mysql.WithUsername("root"),
		mysql.WithPassword(mysqlPassword),
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
		Database: mysqlDatabase,
		Username: "root",
		Password: mysqlPassword,
		SSLMode:  SSLMode{Mode: sslDisabled},
	}, nil
}

// sharedMySQL is one server every Docker test writes to; each test gets a
// fresh database of its own.
func sharedMySQL(t *testing.T, database string) Config {
	t.Helper()
	needDocker(t)
	sharedOnce.Do(func() { sharedCfg, sharedErr = runMySQL() })
	require.NoError(t, sharedErr)
	execSQL(t, sharedCfg, "DROP DATABASE IF EXISTS "+quote(database))
	execSQL(t, sharedCfg, "CREATE DATABASE "+quote(database))
	cfg := sharedCfg
	cfg.Database = database
	return cfg
}

func execSQL(t *testing.T, cfg Config, query string, args ...any) {
	t.Helper()
	ctx := context.Background()
	d, err := connect(ctx, cfg)
	require.NoError(t, err)
	defer d.close()
	_, err = d.ExecContext(ctx, query, args...)
	require.NoError(t, err)
}

// queryStrings runs query and returns every row as text, "<NULL>" for NULL.
func queryStrings(t *testing.T, cfg Config, query string, args ...any) [][]string {
	t.Helper()
	ctx := context.Background()
	d, err := connect(ctx, cfg)
	require.NoError(t, err)
	defer d.close()
	rows, err := d.QueryContext(ctx, query, args...)
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()
	cols, err := rows.Columns()
	require.NoError(t, err)
	var out [][]string
	for rows.Next() {
		vals := make([]sql.NullString, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		require.NoError(t, rows.Scan(ptrs...))
		row := make([]string, len(cols))
		for i, v := range vals {
			row[i] = "<NULL>"
			if v.Valid {
				row[i] = v.String
			}
		}
		out = append(out, row)
	}
	require.NoError(t, rows.Err())
	return out
}

func countRows(t *testing.T, cfg Config, table string) int {
	t.Helper()
	rows := queryStrings(t, cfg, "SELECT COUNT(*) FROM "+quote(table))
	n := 0
	for _, c := range rows[0][0] {
		n = n*10 + int(c-'0')
	}
	return n
}

func hasTable(t *testing.T, cfg Config, table string) bool {
	t.Helper()
	rows := queryStrings(t, cfg, "SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?", table)
	return rows[0][0] != "0"
}

// tables lists the tables of the configured database.
func tables(t *testing.T, cfg Config) []string {
	t.Helper()
	var names []string
	for _, r := range queryStrings(t, cfg, "SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() ORDER BY TABLE_NAME") {
		names = append(names, r[0])
	}
	return names
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
