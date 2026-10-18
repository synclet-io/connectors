package main

import (
	"context"
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
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

const (
	pgImage    = "postgres:16-alpine"
	pgDatabase = "synclet"
	pgUser     = "synclet"
	pgPassword = "synclet-test"
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
		t.Skip("starts Postgres in Docker; skipped under -short")
	}
}

// runPostgres starts a postgres:16 container and returns a configuration
// that reaches it without SSL.
func runPostgres() (Config, error) {
	ctx := context.Background()
	ctr, err := postgres.Run(ctx, pgImage,
		postgres.WithDatabase(pgDatabase),
		postgres.WithUsername(pgUser),
		postgres.WithPassword(pgPassword),
		postgres.BasicWaitStrategies(),
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
	port, err := ctr.MappedPort(ctx, "5432/tcp")
	if err != nil {
		return Config{}, err
	}
	return Config{
		Host:     host,
		Port:     int(port.Num()),
		Database: pgDatabase,
		Username: pgUser,
		Password: pgPassword,
		SSLMode:  SSLMode{Mode: sslDisable},
	}, nil
}

// sharedPostgres is one server every Docker test writes to; each test uses
// its own schema.
func sharedPostgres(t *testing.T, schema string) Config {
	t.Helper()
	needDocker(t)
	sharedOnce.Do(func() { sharedCfg, sharedErr = runPostgres() })
	require.NoError(t, sharedErr)
	cfg := sharedCfg
	cfg.Schema = schema
	return cfg
}

func execSQL(t *testing.T, cfg Config, sql string, args ...any) {
	t.Helper()
	ctx := context.Background()
	d, err := connect(ctx, cfg)
	require.NoError(t, err)
	defer d.close()
	_, err = d.Exec(ctx, sql, args...)
	require.NoError(t, err)
}

// queryStrings runs sql and returns every row as its columns cast to text by
// the query.
func queryStrings(t *testing.T, cfg Config, sql string, args ...any) [][]string {
	t.Helper()
	ctx := context.Background()
	d, err := connect(ctx, cfg)
	require.NoError(t, err)
	defer d.close()
	rows, err := d.Query(ctx, sql, args...)
	require.NoError(t, err)
	defer rows.Close()
	var out [][]string
	for rows.Next() {
		vals, err := rows.Values()
		require.NoError(t, err)
		row := make([]string, len(vals))
		for i, v := range vals {
			if v == nil {
				row[i] = "<NULL>"
				continue
			}
			s, ok := v.(string)
			require.True(t, ok, "column %d is not cast to text", i)
			row[i] = s
		}
		out = append(out, row)
	}
	require.NoError(t, rows.Err())
	return out
}

func countRows(t *testing.T, cfg Config, table string) int {
	t.Helper()
	rows := queryStrings(t, cfg, "SELECT count(*)::text FROM "+quote(cfg.Schema, table))
	n := 0
	for _, c := range rows[0][0] {
		n = n*10 + int(c-'0')
	}
	return n
}

func tableExists(t *testing.T, cfg Config, table string) bool {
	t.Helper()
	rows := queryStrings(t, cfg, "SELECT (to_regclass($1) IS NOT NULL)::text", quote(cfg.Schema, table))
	return rows[0][0] == "true"
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
