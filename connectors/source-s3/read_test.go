package main

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
	"github.com/synclet-io/synclet-go-sdk/testkit"
)

func csvFile(rows ...string) []byte {
	return []byte("id,v\n" + strings.Join(rows, "\n") + "\n")
}

// recordFiles lists the file and row of each record, as "key:row".
func recordFiles(t *testing.T, out *testkit.Output, name string) []string {
	t.Helper()
	var got []string
	for _, r := range records(t, out, name) {
		got = append(got, r[fieldFile].(string)+":"+string(r[fieldRow].(json.Number)))
	}
	return got
}

// lastState is the stream's last STATE as {"cursor","keys"}.
func lastState(t *testing.T, out *testkit.Output) streamState {
	t.Helper()
	states := out.States()
	require.NotEmpty(t, states)
	var st streamState
	require.NoError(t, json.Unmarshal(states[len(states)-1].Stream.StreamState, &st))
	return st
}

func TestIncremental(t *testing.T) {
	t1 := t0.Add(time.Hour)
	cases := []struct {
		name      string
		change    func(m *memStore)
		wantFiles []string
		wantState streamState
	}{
		{
			name:      "no change",
			change:    func(*memStore) {},
			wantState: streamState{Cursor: "2026-10-01T12:00:00Z", Keys: []string{"in/a.csv", "in/b.csv"}},
		},
		{
			name:      "object added later",
			change:    func(m *memStore) { m.put("in/c.csv", csvFile("3,c"), t1) },
			wantFiles: []string{"in/c.csv:1"},
			wantState: streamState{Cursor: "2026-10-01T13:00:00Z", Keys: []string{"in/c.csv"}},
		},
		{
			name:      "object rewritten",
			change:    func(m *memStore) { m.put("in/a.csv", csvFile("1,a", "10,a2"), t1) },
			wantFiles: []string{"in/a.csv:1", "in/a.csv:2"},
			wantState: streamState{Cursor: "2026-10-01T13:00:00Z", Keys: []string{"in/a.csv"}},
		},
		{
			name:      "object added at the saved cursor",
			change:    func(m *memStore) { m.put("in/0.csv", csvFile("0,z"), t0) },
			wantFiles: []string{"in/0.csv:1"},
			wantState: streamState{Cursor: "2026-10-01T12:00:00Z", Keys: []string{"in/a.csv", "in/b.csv", "in/0.csv"}},
		},
		{
			name: "two new objects with the same time",
			change: func(m *memStore) {
				m.put("in/e.csv", csvFile("5,e"), t1)
				m.put("in/d.csv", csvFile("4,d"), t1)
			},
			wantFiles: []string{"in/d.csv:1", "in/e.csv:1"},
			wantState: streamState{Cursor: "2026-10-01T13:00:00Z", Keys: []string{"in/d.csv", "in/e.csv"}},
		},
		{
			name:      "older object is not read",
			change:    func(m *memStore) { m.put("in/old.csv", csvFile("9,o"), t0.Add(-time.Hour)) },
			wantState: streamState{Cursor: "2026-10-01T12:00:00Z", Keys: []string{"in/a.csv", "in/b.csv"}},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			store := newMemStore()
			store.put("in/a.csv", csvFile("1,a"), t0)
			store.put("in/b.csv", csvFile("2,b"), t0)
			store.put("other/x.csv", csvFile("7,x"), t1)
			src := Source{store: store}
			cfg := testConfig(StreamConfig{Name: "s", Glob: "in/*.csv", Format: Format{Filetype: formatCSV}})
			cat := catalogOf(synclet.SyncModeIncremental, discoverStreams(t, src, cfg)["s"])

			first := runRead(t, src, cfg, cat, nil)
			require.NoError(t, first.Err)
			require.Equal(t, []string{"in/a.csv:1", "in/b.csv:1"}, recordFiles(t, first, "s"))
			require.Len(t, first.States(), 2, "a STATE after each object")
			require.Equal(t, streamState{Cursor: "2026-10-01T12:00:00Z", Keys: []string{"in/a.csv", "in/b.csv"}}, lastState(t, first))

			c.change(store)
			second := runRead(t, src, cfg, cat, first.NextState())
			require.NoError(t, second.Err)
			require.Equal(t, c.wantFiles, recordFiles(t, second, "s"))
			require.Equal(t, c.wantState, lastState(t, second))
		})
	}
}

func TestIncrementalStateErrors(t *testing.T) {
	store := newMemStore()
	store.put("in/a.csv", csvFile("1,a"), t0)
	src := Source{store: store}
	cfg := testConfig(StreamConfig{Name: "s", Glob: "in/*.csv", Format: Format{Filetype: formatCSV}})
	s := discoverStreams(t, src, cfg)["s"]
	cat := catalogOf(synclet.SyncModeIncremental, s)

	bad := mustJSON(t, []synclet.StateMessage{{Type: synclet.StateTypeStream, Stream: &synclet.StreamState{
		StreamDescriptor: synclet.StreamDescriptor{Name: "s"}, StreamState: json.RawMessage(`{"cursor":"yesterday"}`),
	}}})
	require.ErrorContains(t, runRead(t, src, cfg, cat, bad).Err, `stream "s": invalid state: cursor`)

	cat.Streams[0].CursorField = []string{"v"}
	require.ErrorContains(t, runRead(t, src, cfg, cat, nil).Err, "the cursor is _synclet_source_modified")

	empty := runRead(t, src, cfg, catalogOf(synclet.SyncModeIncremental, s), nil)
	require.NoError(t, empty.Err)
	store.objs = map[string]memObject{}
	none := runRead(t, src, cfg, catalogOf(synclet.SyncModeIncremental, s), nil)
	require.NoError(t, none.Err)
	require.Len(t, none.States(), 1, "an incremental stream without objects still emits its state")

	missing := catalogOf(synclet.SyncModeFullRefresh, synclet.Stream{Name: "ghost", JSONSchema: s.JSONSchema})
	require.ErrorContains(t, runRead(t, src, cfg, missing, nil).Err, `stream "ghost" is not in the configuration`)
}

func TestBadRows(t *testing.T) {
	store := newMemStore()
	store.put("c/a.csv", []byte("id,v\n1,a\n2,b,extra\n3,c\n"), t0)
	store.put("c/b.csv", []byte("id,v\nnot-a-number,d\n"), t0)
	store.put("j/a.jsonl", []byte("{\"id\":1}\n{broken\n[1]\n{\"id\":2,\"new\":true}\n{\"id\":3}\n"), t0)
	src := Source{store: store}
	schema := `{"type":"object","properties":{"id":{"type":"integer"},"v":{"type":["null","string"]}}}`
	streams := []StreamConfig{
		{Name: "c", Glob: "c/*", Format: Format{Filetype: formatCSV}, Schema: schema},
		{Name: "j", Glob: "j/*", Format: Format{Filetype: formatJSONL}, Schema: schema},
	}
	cfg := testConfig(streams...)
	cat := discoverStreams(t, src, cfg)

	out := runRead(t, src, cfg, catalogOf(synclet.SyncModeFullRefresh, cat["c"]), nil)
	require.ErrorContains(t, out.Err, `stream "c": file "c/a.csv" row 2: line 3: 3 fields, the header has 2`)
	out = runRead(t, src, cfg, catalogOf(synclet.SyncModeFullRefresh, cat["j"]), nil)
	require.ErrorContains(t, out.Err, `stream "j": file "j/a.jsonl" row 2: line 2: invalid character 'b'`)

	for i := range cfg.Streams {
		cfg.Streams[i].SkipBadRows = true
	}
	out = runRead(t, src, cfg, catalogOf(synclet.SyncModeFullRefresh, cat["c"], cat["j"]), nil)
	require.NoError(t, out.Err)
	require.Equal(t, []string{"c/a.csv:1", "c/a.csv:3"}, recordFiles(t, out, "c"))
	require.Equal(t, []string{"j/a.jsonl:1", "j/a.jsonl:5"}, recordFiles(t, out, "j"))
	var logs []string
	for _, l := range out.Logs() {
		logs = append(logs, l.Message)
	}
	require.Contains(t, strings.Join(logs, "\n"), `file=c/a.csv rows=1`)
	require.Contains(t, strings.Join(logs, "\n"), `file=c/b.csv rows=1`)
	require.Contains(t, strings.Join(logs, "\n"), `file=j/a.jsonl rows=3`)

	// A value that does not fit the given schema is a bad row too.
	cfg.Streams[0].SkipBadRows = false
	store.put("c/a.csv", []byte("id,v\n1,a\n"), t0)
	out = runRead(t, src, cfg, catalogOf(synclet.SyncModeFullRefresh, cat["c"]), nil)
	require.ErrorContains(t, out.Err, `file "c/b.csv" row 1: field "id": value "not-a-number" is not of type integer`)
}
