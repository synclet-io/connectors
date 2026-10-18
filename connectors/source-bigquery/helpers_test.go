package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
	"github.com/synclet-io/synclet-go-sdk/testkit"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"google.golang.org/api/option"
)

const (
	emulatorImage   = "ghcr.io/goccy/bigquery-emulator:0.8.1"
	emulatorProject = "test"
	emulatorDataset = "ds"
)

var (
	containersMu sync.Mutex
	containers   []testcontainers.Container
	sharedOnce   sync.Once
	sharedSrc    Source
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
		t.Skip("starts the BigQuery emulator in Docker; skipped under -short")
	}
}

// runEmulator starts the BigQuery emulator with project test and dataset ds,
// loads testdata/fixture.sql and returns a Source that reaches it without
// credentials.
func runEmulator() (Source, error) {
	ctx := context.Background()
	ctr, err := testcontainers.Run(ctx, emulatorImage,
		testcontainers.WithCmd("--project="+emulatorProject, "--dataset="+emulatorDataset),
		testcontainers.WithExposedPorts("9050/tcp"),
		testcontainers.WithWaitStrategy(wait.ForListeningPort("9050/tcp")),
	)
	if ctr != nil {
		containersMu.Lock()
		containers = append(containers, ctr)
		containersMu.Unlock()
	}
	if err != nil {
		return Source{}, err
	}
	endpoint, err := ctr.PortEndpoint(ctx, "9050/tcp", "http")
	if err != nil {
		return Source{}, err
	}
	src := Source{clientOpts: []option.ClientOption{option.WithEndpoint(endpoint), option.WithoutAuthentication()}}
	sql, err := os.ReadFile(filepath.Join("testdata", "fixture.sql"))
	if err != nil {
		return Source{}, err
	}
	for stmt := range strings.SplitSeq(string(sql), ";\n") {
		if strings.TrimSpace(stmt) == "" {
			continue
		}
		if err := src.exec(ctx, stmt); err != nil {
			return Source{}, fmt.Errorf("fixture: %w", err)
		}
	}
	return src, nil
}

func (s Source) exec(ctx context.Context, sql string) error {
	c, err := bigquery.NewClient(ctx, emulatorProject, s.clientOpts...)
	if err != nil {
		return err
	}
	defer func() { _ = c.Close() }()
	_, err = c.Query(sql).Read(ctx)
	return err
}

// sharedEmulator is one emulator every test reads from, with the
// configuration that reaches dataset ds.
func sharedEmulator(t *testing.T) (Source, Config) {
	t.Helper()
	needDocker(t)
	sharedOnce.Do(func() { sharedSrc, sharedErr = runEmulator() })
	require.NoError(t, sharedErr)
	return sharedSrc, Config{ProjectID: emulatorProject, Datasets: []string{emulatorDataset}}
}

func execSQL(t *testing.T, src Source, sql string) {
	t.Helper()
	require.NoError(t, src.exec(context.Background(), sql))
}

func discoverStream(t *testing.T, src Source, cfg Config, namespace, name string) synclet.Stream {
	t.Helper()
	cat, err := src.Discover(t.Context(), cfg)
	require.NoError(t, err)
	for _, s := range cat.Streams {
		if s.Namespace == namespace && s.Name == name {
			return s
		}
	}
	t.Fatalf("discover: no stream %s.%s", namespace, name)
	return synclet.Stream{}
}

func runRead(t *testing.T, src Source, cfg Config, cat *synclet.ConfiguredCatalog, state json.RawMessage) *testkit.Output {
	t.Helper()
	out, err := testkit.RunSource(t.Context(), src, synclet.Request{
		Command: synclet.CommandRead,
		Config:  mustJSON(t, cfg),
		Catalog: mustJSON(t, cat),
		State:   state,
	})
	require.NoError(t, err)
	require.NoError(t, out.Err)
	return out
}

func fullRefresh(s synclet.Stream) synclet.ConfiguredStream {
	return synclet.ConfiguredStream{Stream: s, SyncMode: synclet.SyncModeFullRefresh, DestinationSyncMode: synclet.DestinationSyncModeOverwrite}
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

// rawRecords returns the data of desc's records as compact JSON.
func rawRecords(t *testing.T, out *testkit.Output, desc synclet.StreamDescriptor) []string {
	t.Helper()
	var got []string
	for _, r := range out.Records(desc) {
		got = append(got, string(r.Data))
	}
	return got
}
