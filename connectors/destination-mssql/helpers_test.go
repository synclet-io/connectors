package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
	"github.com/synclet-io/synclet-go-sdk/testkit"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/mssql"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	msImage      = "mcr.microsoft.com/mssql/server:2022-latest"
	msSAPassword = "Synclet-SA-Test1!"
	// dockerEnv enables the tests that start SQL Server in Docker.
	dockerEnv = "MSSQL_TEST_DOCKER"
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
	if os.Getenv(dockerEnv) == "" || testing.Short() {
		t.Skip("starts SQL Server in Docker; set " + dockerEnv + "=1 to run")
	}
}

// runMSSQL starts a throwaway SQL Server container on a free host port and
// returns an sa configuration that reaches it.
func runMSSQL() (Config, error) {
	ctx := context.Background()
	ctr, err := mssql.Run(ctx, msImage,
		mssql.WithAcceptEULA(),
		mssql.WithPassword(msSAPassword),
		testcontainers.WithImagePlatform("linux/amd64"),
		testcontainers.WithWaitStrategyAndDeadline(5*time.Minute,
			wait.ForListeningPort("1433/tcp"),
			wait.ForLog("Recovery is complete."),
		),
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
	port, err := ctr.MappedPort(ctx, "1433/tcp")
	if err != nil {
		return Config{}, err
	}
	cfg := Config{Host: host, Port: int(port.Num()), Database: "master", Schema: "dbo", Username: "sa", Password: msSAPassword}
	for deadline := time.Now().Add(2 * time.Minute); ; {
		d, err := connect(ctx, cfg)
		if err == nil {
			d.close()
			return cfg, nil
		}
		if time.Now().After(deadline) {
			return Config{}, err
		}
		time.Sleep(2 * time.Second)
	}
}

// sharedMSSQL is one server every Docker test writes to; each test gets a
// fresh database of its own, written to as sa.
func sharedMSSQL(t *testing.T, database string) Config {
	t.Helper()
	needDocker(t)
	sharedOnce.Do(func() { sharedCfg, sharedErr = runMSSQL() })
	require.NoError(t, sharedErr)
	execSQL(t, sharedCfg, "IF DB_ID(@p1) IS NOT NULL EXEC(N'ALTER DATABASE '+@p2+' SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE '+@p2)",
		database, quote(database))
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
	rows := queryStrings(t, cfg, "SELECT COUNT(*) FROM "+qualified(cfg.Schema, table))
	n, err := strconv.Atoi(rows[0][0])
	require.NoError(t, err)
	return n
}

func hasTable(t *testing.T, cfg Config, table string) bool {
	t.Helper()
	rows := queryStrings(t, cfg, "SELECT COUNT(*) FROM sys.tables WHERE schema_id = SCHEMA_ID(@p1) AND name = @p2", cfg.Schema, table)
	return rows[0][0] != "0"
}

// tables lists the tables of the configured schema.
func tables(t *testing.T, cfg Config) []string {
	t.Helper()
	var names []string
	for _, r := range queryStrings(t, cfg, "SELECT name FROM sys.tables WHERE schema_id = SCHEMA_ID(@p1) ORDER BY name", cfg.Schema) {
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
