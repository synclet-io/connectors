package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
)

var testStart = time.Date(2026, 10, 18, 9, 0, 0, 0, time.UTC)

// memStore is an in-memory objectStore that appends every upload and
// delete to a log shared with the emitter.
type memStore struct {
	mu      sync.Mutex
	objects map[string][]byte
	log     *eventLog
}

type eventLog struct {
	mu     sync.Mutex
	events []string
}

func (l *eventLog) add(e string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.events = append(l.events, e)
}

// Write receives the emitter's protocol lines.
func (l *eventLog) Write(p []byte) (int, error) {
	for _, line := range strings.Split(strings.TrimSpace(string(p)), "\n") {
		l.add("emit " + line)
	}
	return len(p), nil
}

func newMemStore() *memStore {
	return &memStore{objects: map[string][]byte{}, log: &eventLog{}}
}

func (s *memStore) put(_ context.Context, key string, body io.Reader) error {
	b, err := io.ReadAll(body)
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.objects[key] = b
	s.mu.Unlock()
	s.log.add("put " + key)
	return nil
}

func (s *memStore) list(_ context.Context, prefix string) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var keys []string
	for k := range s.objects {
		if strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	return keys, nil
}

func (s *memStore) delete(_ context.Context, keys []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, k := range keys {
		delete(s.objects, k)
		s.log.add("delete " + k)
	}
	return nil
}

func (s *memStore) keys() []string {
	keys, _ := s.list(context.Background(), "")
	return keys
}

func testConfig(t *testing.T, extra string) Config {
	t.Helper()
	raw := `{"bucket":"b","bucket_path":"p","credentials":{"auth_type":"access_key","access_key_id":"k","secret_access_key":"s"}`
	if extra != "" {
		raw += "," + extra
	}
	cfg := parseConfig(t, raw+"}")
	require.NoError(t, cfg.validate())
	return cfg
}

// runMemWrite runs writeMessages against store and returns the error.
func runMemWrite(t *testing.T, store *memStore, cfg Config, catalog *synclet.ConfiguredCatalog, lines []string, tune func(*syncWriter)) error {
	t.Helper()
	w, err := newSyncWriter(cfg, catalog, store, testStart, t.TempDir())
	if err != nil {
		return err
	}
	if tune != nil {
		tune(w)
	}
	in := synclet.NewMessageReader(strings.NewReader(strings.Join(lines, "\n") + "\n"))
	return writeMessages(t.Context(), w, in, synclet.NewEmitter(store.log))
}

func idRecord(id int, country string) string {
	return fmt.Sprintf(`{"id":%d,"country":%q}`, id, country)
}

// uploadedIDs returns the ids of the records in a JSONL object.
func uploadedIDs(t *testing.T, b []byte) []int {
	t.Helper()
	var ids []int
	for _, data := range readJSONL(t, b) {
		var r struct{ ID int }
		require.NoError(t, json.Unmarshal(data, &r))
		ids = append(ids, r.ID)
	}
	return ids
}

func TestStateAfterUpload(t *testing.T) {
	store := newMemStore()
	cfg := testConfig(t, `"path_template":"{stream}/{field:country}/{part}"`)
	catalog := fixtureCatalog(t, fixtureStream("orders", "", synclet.DestinationSyncModeAppend), fixtureStream("users", "", synclet.DestinationSyncModeAppend))
	lines := []string{
		recordMessage("orders", "", idRecord(1, "UA")),
		recordMessage("users", "", idRecord(2, "PL")),
		recordMessage("orders", "", idRecord(3, "PL")),
		stateMessage("orders", "", 1),
		recordMessage("orders", "", idRecord(4, "UA")),
		stateMessage("users", "", 2),
		stateMessage("orders", "", 3),
		recordMessage("users", "", idRecord(5, "DE")),
	}
	require.NoError(t, runMemWrite(t, store, cfg, catalog, lines, nil))

	// Replay the shared log: at every echoed state, each record that came
	// before that state in the input must already be in an uploaded object.
	uploaded := map[int]bool{}
	var states []int
	for _, e := range store.log.events {
		switch {
		case strings.HasPrefix(e, "put "):
			for _, id := range uploadedIDs(t, store.objects[strings.TrimPrefix(e, "put ")]) {
				uploaded[id] = true
			}
		case strings.HasPrefix(e, "emit "):
			var msg synclet.Message
			require.NoError(t, json.Unmarshal([]byte(strings.TrimPrefix(e, "emit ")), &msg))
			require.Equal(t, synclet.MessageTypeState, msg.Type)
			var st struct{ Cursor int }
			require.NoError(t, json.Unmarshal(msg.State.Stream.StreamState, &st))
			states = append(states, st.Cursor)
			want := map[int][]int{1: {1, 2, 3}, 2: {1, 2, 3, 4}, 3: {1, 2, 3, 4}}[st.Cursor]
			for _, id := range want {
				assert.True(t, uploaded[id], "state %d echoed before record %d was uploaded", st.Cursor, id)
			}
		}
	}
	assert.Equal(t, []int{1, 2, 3}, states, "states are echoed in input order")
	assert.True(t, uploaded[5], "the last record is uploaded at the end")
}

func TestStateNotEchoedWhenUploadFails(t *testing.T) {
	store := &failingStore{memStore: newMemStore()}
	cfg := testConfig(t, "")
	catalog := fixtureCatalog(t, fixtureStream("orders", "", synclet.DestinationSyncModeAppend))
	lines := []string{recordMessage("orders", "", idRecord(1, "UA")), stateMessage("orders", "", 1)}
	w, err := newSyncWriter(cfg, catalog, store, testStart, t.TempDir())
	require.NoError(t, err)
	in := synclet.NewMessageReader(strings.NewReader(strings.Join(lines, "\n") + "\n"))
	err = writeMessages(t.Context(), w, in, synclet.NewEmitter(store.log))
	require.ErrorContains(t, err, "upload refused")
	assert.Empty(t, store.log.events)
}

type failingStore struct{ *memStore }

func (s *failingStore) put(context.Context, string, io.Reader) error {
	return fmt.Errorf("upload refused")
}

func TestOverwriteRemovesPreviousObjectsAfterSuccess(t *testing.T) {
	store := newMemStore()
	store.objects["p/orders/2026_10_01/old.jsonl"] = []byte("old")
	store.objects["p/orders_archive/2026_10_01/keep.jsonl"] = []byte("other stream sharing a name prefix")
	store.objects["p/users/2026_10_01/old.jsonl"] = []byte("append stream")
	cfg := testConfig(t, "")
	catalog := fixtureCatalog(t,
		fixtureStream("orders", "", synclet.DestinationSyncModeOverwrite),
		fixtureStream("users", "", synclet.DestinationSyncModeAppend))
	lines := []string{
		recordMessage("orders", "", idRecord(1, "UA")),
		stateMessage("orders", "", 1),
		recordMessage("users", "", idRecord(2, "UA")),
	}
	require.NoError(t, runMemWrite(t, store, cfg, catalog, lines, nil))

	ms := testStart.UnixMilli()
	assert.Equal(t, []string{
		"p/orders/2026_10_18/" + fmt.Sprint(ms) + "_RUN_0.jsonl",
		"p/orders_archive/2026_10_01/keep.jsonl",
		"p/users/2026_10_01/old.jsonl",
		"p/users/2026_10_18/" + fmt.Sprint(ms) + "_RUN_1.jsonl",
	}, withoutRunID(store.keys()))
	events := store.log.events
	cleanup := slices.Index(events, "delete p/orders/2026_10_01/old.jsonl")
	require.GreaterOrEqual(t, cleanup, 0)
	for _, e := range events[cleanup+1:] {
		assert.False(t, strings.HasPrefix(e, "put "), "previous objects are removed after the last upload, found %s after it", e)
	}
}

// emittedStates returns the log position of each echoed state, keyed by
// stream name, or by state type for a state without a stream.
func emittedStates(t *testing.T, events []string) map[string]int {
	t.Helper()
	pos := map[string]int{}
	for i, e := range events {
		line, ok := strings.CutPrefix(e, "emit ")
		if !ok {
			continue
		}
		var msg synclet.Message
		require.NoError(t, json.Unmarshal([]byte(line), &msg))
		require.Equal(t, synclet.MessageTypeState, msg.Type)
		name := string(msg.State.Type)
		if msg.State.Stream != nil {
			name = msg.State.Stream.StreamDescriptor.Name
		}
		pos[name] = i
	}
	return pos
}

func TestOverwriteStatesHeldUntilCleanup(t *testing.T) {
	store := newMemStore()
	store.objects["p/orders/2026_10_01/old.jsonl"] = []byte("old")
	cfg := testConfig(t, "")
	catalog := fixtureCatalog(t,
		fixtureStream("orders", "", synclet.DestinationSyncModeOverwrite),
		fixtureStream("users", "", synclet.DestinationSyncModeAppend))
	lines := []string{
		recordMessage("orders", "", idRecord(1, "UA")),
		stateMessage("orders", "", 1),
		recordMessage("users", "", idRecord(2, "UA")),
		stateMessage("users", "", 2),
		`{"type":"STATE","state":{"type":"GLOBAL","global":{"shared_state":{"lsn":3}}}}`,
		recordMessage("users", "", idRecord(3, "UA")),
	}
	require.NoError(t, runMemWrite(t, store, cfg, catalog, lines, nil))

	events := store.log.events
	cleanup := slices.Index(events, "delete p/orders/2026_10_01/old.jsonl")
	require.GreaterOrEqual(t, cleanup, 0)
	pos := emittedStates(t, events)
	require.Len(t, pos, 3)
	assert.Less(t, pos["users"], cleanup, "an append stream's state is echoed once its records are uploaded")
	assert.Greater(t, pos["orders"], cleanup, "an overwrite stream's state waits for the cleanup")
	assert.Greater(t, pos["GLOBAL"], cleanup, "a GLOBAL state waits for the cleanup while an overwrite stream exists")
}

func TestOverwriteStatesNotEchoedWhenSyncFails(t *testing.T) {
	store := newMemStore()
	cfg := testConfig(t, "")
	catalog := fixtureCatalog(t, fixtureStream("orders", "", synclet.DestinationSyncModeOverwrite))
	lines := []string{
		recordMessage("orders", "", idRecord(1, "UA")),
		stateMessage("orders", "", 1),
		recordMessage("orders", "", `[1,2]`),
	}
	require.Error(t, runMemWrite(t, store, cfg, catalog, lines, nil))
	assert.Empty(t, emittedStates(t, store.log.events))
}

func TestOverwriteKeepsPreviousObjectsWhenSyncFails(t *testing.T) {
	store := newMemStore()
	store.objects["p/orders/2026_10_01/old.jsonl"] = []byte("old")
	cfg := testConfig(t, "")
	catalog := fixtureCatalog(t, fixtureStream("orders", "", synclet.DestinationSyncModeOverwrite))
	lines := []string{
		recordMessage("orders", "", idRecord(1, "UA")),
		stateMessage("orders", "", 1),
		recordMessage("orders", "", `[1,2]`),
	}
	err := runMemWrite(t, store, cfg, catalog, lines, nil)
	require.ErrorContains(t, err, "record data is not a JSON object")
	assert.Contains(t, store.keys(), "p/orders/2026_10_01/old.jsonl")
}

func TestOverwriteEmptySyncRemovesEverything(t *testing.T) {
	store := newMemStore()
	store.objects["p/orders/2026_10_01/old.jsonl"] = []byte("old")
	cfg := testConfig(t, "")
	catalog := fixtureCatalog(t, fixtureStream("orders", "", synclet.DestinationSyncModeOverwrite))
	require.NoError(t, runMemWrite(t, store, cfg, catalog, nil, nil))
	assert.Empty(t, store.keys())
}

func TestOverwriteOverlappingPrefixesRejected(t *testing.T) {
	cfg := testConfig(t, `"path_template":"{namespace}/{stream}/{part}"`)
	catalog := fixtureCatalog(t,
		fixtureStream("x", "", synclet.DestinationSyncModeOverwrite),
		fixtureStream("y", "x", synclet.DestinationSyncModeAppend))
	_, err := newSyncWriter(cfg, catalog, newMemStore(), testStart, t.TempDir())
	require.ErrorContains(t, err, `overwrite stream "x" writes under "p/x/", which overlaps stream "y" under "p/x/y/"`)
}

func TestAppendAddsObjects(t *testing.T) {
	store := newMemStore()
	cfg := testConfig(t, "")
	catalog := fixtureCatalog(t, fixtureStream("orders", "", synclet.DestinationSyncModeAppend))
	require.NoError(t, runMemWrite(t, store, cfg, catalog, []string{recordMessage("orders", "", idRecord(1, "UA"))}, nil))
	w, err := newSyncWriter(cfg, catalog, store, testStart.Add(time.Hour), t.TempDir())
	require.NoError(t, err)
	in := synclet.NewMessageReader(strings.NewReader(recordMessage("orders", "", idRecord(2, "UA")) + "\n"))
	require.NoError(t, writeMessages(t.Context(), w, in, synclet.NewEmitter(io.Discard)))
	assert.Len(t, store.keys(), 2)
}

func TestDestinationSyncModeAppendDedupRejected(t *testing.T) {
	cfg := testConfig(t, "")
	catalog := fixtureCatalog(t, fixtureStream("orders", "", synclet.DestinationSyncModeAppendDedup))
	_, err := newSyncWriter(cfg, catalog, newMemStore(), testStart, t.TempDir())
	require.ErrorContains(t, err, `destination sync mode "append_dedup" is not supported`)
}

func TestRecordOfUnknownStreamFails(t *testing.T) {
	cfg := testConfig(t, "")
	catalog := fixtureCatalog(t, fixtureStream("orders", "", synclet.DestinationSyncModeAppend))
	err := runMemWrite(t, newMemStore(), cfg, catalog, []string{recordMessage("users", "", idRecord(1, "UA"))}, nil)
	require.ErrorContains(t, err, `record for stream "users"`)
}

func TestRotationByRecordCount(t *testing.T) {
	store := newMemStore()
	cfg := testConfig(t, `"max_records_per_file":2`)
	catalog := fixtureCatalog(t, fixtureStream("orders", "", synclet.DestinationSyncModeAppend))
	var lines []string
	for i := 1; i <= 5; i++ {
		lines = append(lines, recordMessage("orders", "", idRecord(i, "UA")))
	}
	require.NoError(t, runMemWrite(t, store, cfg, catalog, lines, nil))
	var counts []int
	for _, k := range store.keys() {
		counts = append(counts, len(uploadedIDs(t, store.objects[k])))
	}
	assert.Equal(t, []int{2, 2, 1}, counts)
}

func TestRotationBySize(t *testing.T) {
	store := newMemStore()
	cfg := testConfig(t, "")
	catalog := fixtureCatalog(t, fixtureStream("orders", "", synclet.DestinationSyncModeAppend))
	big := strings.Repeat("x", 600<<10)
	var lines []string
	for i := 1; i <= 4; i++ {
		lines = append(lines, recordMessage("orders", "", fmt.Sprintf(`{"id":%d,"name":%q}`, i, big)))
	}
	require.NoError(t, runMemWrite(t, store, cfg, catalog, lines, func(w *syncWriter) { w.maxBytes = 1 << 20 }))
	assert.Len(t, store.keys(), 2, "each file is rotated once it passes 1 MB")
}

func TestOpenFilesAreBounded(t *testing.T) {
	store := newMemStore()
	cfg := testConfig(t, `"path_template":"{stream}/{field:country}/{part}"`)
	catalog := fixtureCatalog(t, fixtureStream("orders", "", synclet.DestinationSyncModeAppend))
	var lines []string
	for i, c := range []string{"A", "B", "C", "A", "D"} {
		lines = append(lines, recordMessage("orders", "", idRecord(i, c)))
	}
	require.NoError(t, runMemWrite(t, store, cfg, catalog, lines, func(w *syncWriter) { w.maxOpen = 2 }))
	var all []int
	for _, k := range store.keys() {
		all = append(all, uploadedIDs(t, store.objects[k])...)
	}
	sort.Ints(all)
	assert.Equal(t, []int{0, 1, 2, 3, 4}, all)
	assert.Len(t, store.keys(), 5, "with two open files, A is closed when C opens and B when A reopens")
}

func TestWriteParquetFilesReadBack(t *testing.T) {
	store := newMemStore()
	cfg := testConfig(t, `"format":{"format_type":"parquet"}`)
	catalog := fixtureCatalog(t, fixtureStream("orders", "shop", synclet.DestinationSyncModeAppend))
	var lines []string
	for _, r := range fixtureRecords {
		lines = append(lines, recordMessage("orders", "shop", r))
	}
	require.NoError(t, runMemWrite(t, store, cfg, catalog, lines, nil))
	keys := store.keys()
	require.Len(t, keys, 1)
	assert.True(t, strings.HasSuffix(keys[0], ".parquet"))
	got, _ := readParquet(t, fixtureColumns(t), defaultDecimalScale, store.objects[keys[0]])
	assert.Equal(t, expectedRecords(t), got)
	assert.True(t, bytes.HasPrefix(store.objects[keys[0]], []byte("PAR1")))
}

func TestSyncsStartingInSameMillisecondWriteDistinctKeys(t *testing.T) {
	store := newMemStore()
	cfg := testConfig(t, "")
	catalog := fixtureCatalog(t, fixtureStream("orders", "", synclet.DestinationSyncModeAppend))
	require.NoError(t, runMemWrite(t, store, cfg, catalog, []string{recordMessage("orders", "", idRecord(1, "UA"))}, nil))
	require.NoError(t, runMemWrite(t, store, cfg, catalog, []string{recordMessage("orders", "", idRecord(2, "UA"))}, nil))
	keys := store.keys()
	require.Len(t, keys, 2)
	assert.NotEqual(t, keys[0], keys[1])
}
