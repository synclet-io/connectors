package engine

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
)

var startUnix = time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC).Unix()

const incStream = `  - name: items
    path: items
    params: {"updated[gte]": "{{ since }}"}
    primary_key: [id]
    schema: ` + itemSchema + `
    records: {path: $.data}
    incremental: {cursor_field: updated, cursor_format: unix_s, start: "{{ config.start_date }}", lookback: 1h}
`

func TestIncrementalCursor(t *testing.T) {
	body := fmt.Sprintf(`{"data":[{"id":"a","updated":%d},{"id":"old","updated":%d},{"id":"b","updated":%d}]}`, startUnix+10, startUnix-5, startUnix+20)

	t.Run("first sync", func(t *testing.T) {
		a := newAPI(t).json("/v1/items", body)
		src := newSource(t, manifestYAML("", incStream), a, newClock())
		out := read(t, src, testConfig, catalog(t, src, testConfig, synclet.SyncModeIncremental, "items"), "")
		require.NoError(t, out.Err)
		assert.Equal(t, []string{fmt.Sprintf("/v1/items?updated%%5Bgte%%5D=%d", startUnix)}, a.keys())
		assert.Equal(t, []string{`"a"`, `"b"`}, values(out, "items", "id"))
		assert.Equal(t, []string{fmt.Sprintf(`{"updated":%d}`, startUnix+20)}, states(out, "items"))
		var logged bool
		for _, l := range out.Logs() {
			logged = logged || strings.Contains(l.Message, "items: 1 record(s) older than since")
		}
		assert.True(t, logged)
	})

	t.Run("resume with lookback", func(t *testing.T) {
		saved := startUnix + 7200
		a := newAPI(t).json("/v1/items", fmt.Sprintf(`{"data":[{"id":"a","updated":%d},{"id":"b","updated":%d}]}`, saved-4000, saved-60))
		src := newSource(t, manifestYAML("", incStream), a, newClock())
		state := fmt.Sprintf(`[{"type":"STREAM","stream":{"stream_descriptor":{"name":"items"},"stream_state":{"updated":%d}}}]`, saved)
		out := read(t, src, testConfig, catalog(t, src, testConfig, synclet.SyncModeIncremental, "items"), state)
		require.NoError(t, out.Err)
		assert.Equal(t, []string{fmt.Sprintf("/v1/items?updated%%5Bgte%%5D=%d", saved-3600)}, a.keys())
		assert.Equal(t, []string{`"b"`}, values(out, "items", "id"))
		assert.Equal(t, []string{fmt.Sprintf(`{"updated":%d}`, saved-60)}, states(out, "items"))
	})

	t.Run("lookback never before start", func(t *testing.T) {
		a := newAPI(t).json("/v1/items", `{"data":[]}`)
		src := newSource(t, manifestYAML("", incStream), a, newClock())
		state := fmt.Sprintf(`[{"type":"STREAM","stream":{"stream_descriptor":{"name":"items"},"stream_state":{"updated":%d}}}]`, startUnix+60)
		out := read(t, src, testConfig, catalog(t, src, testConfig, synclet.SyncModeIncremental, "items"), state)
		require.NoError(t, out.Err)
		assert.Equal(t, []string{fmt.Sprintf("/v1/items?updated%%5Bgte%%5D=%d", startUnix)}, a.keys())
		assert.Empty(t, out.States(), "a stream without windows that emits no record emits no state")
	})

	t.Run("full refresh emits no state", func(t *testing.T) {
		a := newAPI(t).json("/v1/items", body)
		src := newSource(t, manifestYAML("", incStream), a, newClock())
		out := read(t, src, testConfig, catalog(t, src, testConfig, synclet.SyncModeFullRefresh, "items"), "")
		require.NoError(t, out.Err)
		assert.Equal(t, []string{`"a"`, `"b"`}, values(out, "items", "id"))
		assert.Empty(t, out.States())
	})

	t.Run("a record without a cursor fails", func(t *testing.T) {
		a := newAPI(t).json("/v1/items", `{"data":[{"id":"a"}]}`)
		src := newSource(t, manifestYAML("", incStream), a, newClock())
		out := read(t, src, testConfig, catalog(t, src, testConfig, synclet.SyncModeIncremental, "items"), "")
		msg, _ := traceMessage(out)
		assert.Contains(t, msg, "has no value at cursor field updated")
		assert.Empty(t, out.Records(synclet.StreamDescriptor{Name: "items"}))
	})

	t.Run("a bad state is a config error", func(t *testing.T) {
		a := newAPI(t)
		src := newSource(t, manifestYAML("", incStream), a, newClock())
		state := `[{"type":"STREAM","stream":{"stream_descriptor":{"name":"items"},"stream_state":{"updated":"soon"}}}]`
		out := read(t, src, testConfig, catalog(t, src, testConfig, synclet.SyncModeIncremental, "items"), state)
		msg, ft := traceMessage(out)
		assert.Equal(t, synclet.FailureTypeConfigError, ft)
		assert.Contains(t, msg, "state of items")
	})

	t.Run("incremental on a full-refresh stream is a config error", func(t *testing.T) {
		a := newAPI(t)
		src := newSource(t, manifestYAML("", itemsStream), a, newClock())
		cat := catalog(t, src, testConfig, synclet.SyncModeFullRefresh, "items")
		cat.Streams[0].SyncMode = synclet.SyncModeIncremental
		out := read(t, src, testConfig, cat, "")
		_, ft := traceMessage(out)
		assert.Equal(t, synclet.FailureTypeConfigError, ft)
	})
}

const dayStream = `  - name: days
    path: "days/{{ since }}.json"
    primary_key: [day, id]
    schema: ` + itemSchema + `
    records: {path: $.data, add: {day: "{{ since }}"}}
    incremental: {cursor_field: day, cursor_format: date, start: "{{ config.start_date }}", window: {step: 1d, closed_only: true}}
`

func TestClosedWindows(t *testing.T) {
	cfg := `{"api_key":"k1","start_date":"2026-10-15"}`
	api := func(t *testing.T) *fakeAPI {
		return newAPI(t).
			json("/v1/days/2026-10-15.json", `{"data":[{"id":"x"}]}`).
			json("/v1/days/2026-10-16.json", `{"data":[]}`).
			json("/v1/days/2026-10-17.json", `{"data":[{"id":"y"}]}`)
	}
	top := "check: {stream: days}\n"

	t.Run("days up to yesterday, a state per day", func(t *testing.T) {
		a := api(t)
		src := newSource(t, strings.Replace(manifestYAML(top, dayStream), "check: {stream: items}\n", "", 1), a, newClock())
		out := read(t, src, cfg, catalog(t, src, cfg, synclet.SyncModeIncremental, "days"), "")
		require.NoError(t, out.Err)
		assert.Equal(t, []string{"/v1/days/2026-10-15.json", "/v1/days/2026-10-16.json", "/v1/days/2026-10-17.json"}, a.keys())
		assert.Equal(t, []string{`"2026-10-15"`, `"2026-10-17"`}, values(out, "days", "day"))
		assert.Equal(t, []string{`{"day":"2026-10-15"}`, `{"day":"2026-10-16"}`, `{"day":"2026-10-17"}`}, states(out, "days"))
	})

	t.Run("resume after the saved day", func(t *testing.T) {
		a := api(t)
		src := newSource(t, strings.Replace(manifestYAML(top, dayStream), "check: {stream: items}\n", "", 1), a, newClock())
		state := `[{"type":"STREAM","stream":{"stream_descriptor":{"name":"days"},"stream_state":{"day":"2026-10-16"}}}]`
		out := read(t, src, cfg, catalog(t, src, cfg, synclet.SyncModeIncremental, "days"), state)
		require.NoError(t, out.Err)
		assert.Equal(t, []string{"/v1/days/2026-10-17.json"}, a.keys())
		assert.Equal(t, []string{`{"day":"2026-10-17"}`}, states(out, "days"))
	})

	t.Run("full refresh reads every day without state", func(t *testing.T) {
		a := api(t)
		src := newSource(t, strings.Replace(manifestYAML(top, dayStream), "check: {stream: items}\n", "", 1), a, newClock())
		out := read(t, src, cfg, catalog(t, src, cfg, synclet.SyncModeFullRefresh, "days"), "")
		require.NoError(t, out.Err)
		assert.Len(t, a.keys(), 3)
		assert.Empty(t, out.States())
	})
}

func TestOpenWindows(t *testing.T) {
	stream := `  - name: items
    path: items
    params: {from: "{{ since }}", to: "{{ until }}"}
    primary_key: [id]
    schema: ` + itemSchema + `
    records: {path: $.data}
    incremental: {cursor_field: updated, cursor_format: rfc3339, start: "{{ config.start_date }}", window: {step: 1d}}
`
	cfg := `{"api_key":"k1","start_date":"2026-10-16T00:00:00Z"}`
	a := newAPI(t).
		json("/v1/items?from=2026-10-16T00%3A00%3A00Z&to=2026-10-17T00%3A00%3A00Z", `{"data":[{"id":"a","updated":"2026-10-16T05:00:00Z"}]}`).
		json("/v1/items?from=2026-10-17T00%3A00%3A00Z&to=2026-10-18T00%3A00%3A00Z", `{"data":[]}`).
		json("/v1/items?from=2026-10-18T00%3A00%3A00Z&to=2026-10-18T12%3A00%3A00Z", `{"data":[{"id":"b","updated":"2026-10-18T11:00:00Z"}]}`)
	src := newSource(t, manifestYAML("", stream), a, newClock())
	out := read(t, src, cfg, catalog(t, src, cfg, synclet.SyncModeIncremental, "items"), "")
	require.NoError(t, out.Err)
	assert.Len(t, a.keys(), 3)
	assert.Equal(t, []string{`{"updated":"2026-10-16T05:00:00Z"}`, `{"updated":"2026-10-17T00:00:00Z"}`, `{"updated":"2026-10-18T11:00:00Z"}`}, states(out, "items"))
}

const valuesStream = `  - name: items
    path: "accounts/{{ partition.value }}/items"
    partition: {values: "{{ config.accounts }}"}
    params: {since: "{{ since }}"}
    primary_key: [id]
    schema: ` + itemSchema + `
    records: {path: $.data, add: {account: "{{ partition.value }}"}}
    incremental: {cursor_field: updated, cursor_format: rfc3339, start: "{{ config.start_date }}"}
`

func TestPartitionValues(t *testing.T) {
	t.Run("a state per value", func(t *testing.T) {
		cfg := `{"api_key":"k1","start_date":"2026-10-01T00:00:00Z","accounts":["a1","a2"]}`
		a := newAPI(t).
			json("/v1/accounts/a1/items", `{"data":[{"id":"1","updated":"2026-10-05T00:00:00Z"}]}`).
			json("/v1/accounts/a2/items", `{"data":[{"id":"2","updated":"2026-10-07T00:00:00Z"}]}`)
		src := newSource(t, manifestYAML("", valuesStream), a, newClock())
		out := read(t, src, cfg, catalog(t, src, cfg, synclet.SyncModeIncremental, "items"), "")
		require.NoError(t, out.Err)
		assert.Equal(t, []string{`"a1"`, `"a2"`}, values(out, "items", "account"))
		assert.Equal(t, []string{
			`{"partitions":{"a1":{"updated":"2026-10-05T00:00:00Z"}}}`,
			`{"partitions":{"a1":{"updated":"2026-10-05T00:00:00Z"},"a2":{"updated":"2026-10-07T00:00:00Z"}}}`,
		}, states(out, "items"))
	})

	t.Run("a new value starts from start and a removed one keeps its entry", func(t *testing.T) {
		cfg := `{"api_key":"k1","start_date":"2026-10-01T00:00:00Z","accounts":["a1","a3"]}`
		a := newAPI(t).
			json("/v1/accounts/a1/items", `{"data":[{"id":"1","updated":"2026-10-09T00:00:00Z"}]}`).
			json("/v1/accounts/a3/items", `{"data":[{"id":"3","updated":"2026-10-02T00:00:00Z"}]}`)
		src := newSource(t, manifestYAML("", valuesStream), a, newClock())
		state := `[{"type":"STREAM","stream":{"stream_descriptor":{"name":"items"},"stream_state":{"partitions":{"a1":{"updated":"2026-10-05T00:00:00Z"},"a2":{"updated":"2026-10-07T00:00:00Z"}}}}}]`
		out := read(t, src, cfg, catalog(t, src, cfg, synclet.SyncModeIncremental, "items"), state)
		require.NoError(t, out.Err)
		assert.Equal(t, []string{
			"/v1/accounts/a1/items?since=2026-10-05T00%3A00%3A00Z",
			"/v1/accounts/a3/items?since=2026-10-01T00%3A00%3A00Z",
		}, a.keys())
		ss := states(out, "items")
		require.NotEmpty(t, ss)
		assert.JSONEq(t, `{"partitions":{"a1":{"updated":"2026-10-09T00:00:00Z"},"a2":{"updated":"2026-10-07T00:00:00Z"},"a3":{"updated":"2026-10-02T00:00:00Z"}}}`, ss[len(ss)-1])
	})
}

const parentStreams = `  - name: items
    path: items
    primary_key: [id]
    schema: ` + itemSchema + `
    records: {path: $.data}
    incremental: {cursor_field: updated, cursor_format: unix_s, start: "{{ config.start_date }}"}
  - name: subitems
    path: "items/{{ parent.id }}/subs"
    partition: {parent: items}
    primary_key: [id]
    schema: ` + itemSchema + `
    records: {path: $.data, add: {parent_id: "{{ parent.id }}"}}
    incremental: {cursor_field: updated, cursor_format: unix_s}
    on_404: skip
  - name: notes
    path: "items/{{ parent.id }}/notes"
    partition: {parent: items}
    primary_key: [id]
    schema: ` + itemSchema + `
    records: {path: $.data, add: {parent_id: "{{ parent.id }}"}}
`

func TestParentPass(t *testing.T) {
	api := func(t *testing.T) *fakeAPI {
		return newAPI(t).
			json("/v1/items", fmt.Sprintf(`{"data":[{"id":"p1","updated":%d},{"id":"p2","updated":%d},{"id":"p3","updated":%d}]}`, startUnix+100, startUnix+200, startUnix+300)).
			json("/v1/items/p1/subs", fmt.Sprintf(`{"data":[{"id":"s1","updated":%d}]}`, startUnix+50)).
			on("/v1/items/p2/subs", reply{status: 404, body: `{"message":"gone"}`}).
			json("/v1/items/p3/subs", fmt.Sprintf(`{"data":[{"id":"s3","updated":%d}]}`, startUnix+400)).
			json("/v1/items/p1/notes", `{"data":[{"id":"n1"}]}`).
			json("/v1/items/p2/notes", `{"data":[]}`).
			json("/v1/items/p3/notes", `{"data":[{"id":"n3"}]}`)
	}

	t.Run("one pass, parent order then page order", func(t *testing.T) {
		a := api(t)
		src := newSource(t, manifestYAML("", parentStreams), a, newClock())
		cat := catalog(t, src, testConfig, synclet.SyncModeIncremental, "items", "subitems")
		cat.Streams = append(cat.Streams, catalog(t, src, testConfig, synclet.SyncModeFullRefresh, "notes").Streams...)
		out := read(t, src, testConfig, cat, "")
		require.NoError(t, out.Err)
		assert.Equal(t, []string{"/v1/items", "/v1/items/p1/subs", "/v1/items/p1/notes", "/v1/items/p2/subs", "/v1/items/p2/notes", "/v1/items/p3/subs", "/v1/items/p3/notes"}, a.keys())
		var order []string
		for _, m := range out.Messages {
			if m.Type == synclet.MessageTypeRecord {
				order = append(order, m.Record.Stream)
			}
		}
		assert.Equal(t, []string{"items", "subitems", "notes", "items", "items", "subitems", "notes"}, order)
		assert.Equal(t, []string{`"p1"`, `"p3"`}, values(out, "subitems", "parent_id"))
		assert.Equal(t, []string{fmt.Sprintf(`{"updated":%d}`, startUnix+300)}, states(out, "items"))
		assert.Equal(t, []string{fmt.Sprintf(`{"updated":%d}`, startUnix+400)}, states(out, "subitems"))
	})

	t.Run("child state and its own since", func(t *testing.T) {
		a := api(t)
		src := newSource(t, manifestYAML("", parentStreams), a, newClock())
		state := fmt.Sprintf(`[{"type":"STREAM","stream":{"stream_descriptor":{"name":"subitems"},"stream_state":{"updated":%d}}}]`, startUnix+60)
		out := read(t, src, testConfig, catalog(t, src, testConfig, synclet.SyncModeIncremental, "subitems"), state)
		require.NoError(t, out.Err)
		assert.Empty(t, out.Records(synclet.StreamDescriptor{Name: "items"}), "a parent outside the catalog emits nothing")
		assert.Equal(t, []string{`"s3"`}, values(out, "subitems", "id"))
		assert.Equal(t, []string{fmt.Sprintf(`{"updated":%d}`, startUnix+400)}, states(out, "subitems"))
	})

	t.Run("check of a child reads its first parent", func(t *testing.T) {
		a := api(t)
		yaml := strings.Replace(manifestYAML("", parentStreams), "check: {stream: items}", "check: {stream: notes}", 1)
		st := checkStatus(t, newSource(t, yaml, a, newClock()), testConfig)
		require.Equal(t, synclet.ConnectionStatusSucceeded, st.Status, st.Message)
		assert.Equal(t, []string{"/v1/items", "/v1/items/p1/notes"}, a.keys())
	})
}

func TestDiscover(t *testing.T) {
	src := newSource(t, manifestYAML("", parentStreams), newAPI(t), newClock())
	cat := catalog(t, src, testConfig, synclet.SyncModeFullRefresh, "items", "subitems", "notes")
	items, notes := cat.Streams[0].Stream, cat.Streams[2].Stream
	assert.Equal(t, []synclet.SyncMode{synclet.SyncModeFullRefresh, synclet.SyncModeIncremental}, items.SupportedSyncModes)
	assert.True(t, items.SourceDefinedCursor)
	assert.Equal(t, []string{"updated"}, items.DefaultCursorField)
	assert.Equal(t, [][]string{{"id"}}, items.SourceDefinedPrimaryKey)
	assert.Equal(t, []synclet.SyncMode{synclet.SyncModeFullRefresh}, notes.SupportedSyncModes)
	assert.False(t, notes.SourceDefinedCursor)
	assert.Contains(t, string(items.JSONSchema), `"properties"`)
}

func TestConfigValidation(t *testing.T) {
	src := newSource(t, manifestYAML("", itemsStream), newAPI(t), newClock())
	st := checkStatus(t, src, `{"start_date":"x"}`)
	assert.Equal(t, synclet.ConnectionStatusFailed, st.Status)
	assert.Contains(t, st.Message, "api_key")

	out := runCmd(t, src, synclet.Request{Command: synclet.CommandDiscover, Config: []byte(`{"api_key":7}`)})
	msg, ft := traceMessage(out)
	assert.Equal(t, synclet.FailureTypeConfigError, ft)
	assert.Contains(t, msg, "api_key")
}

func TestSpec(t *testing.T) {
	src := newSource(t, manifestYAML("", incStream), newAPI(t), newClock())
	out := runCmd(t, src, synclet.Request{Command: synclet.CommandSpec})
	spec := out.Spec()
	require.NotNil(t, spec)
	assert.True(t, spec.SupportsIncremental)
	assert.True(t, spec.ConnectionSpecification.Properties.Properties["api_key"].IsSecret)
	assert.Equal(t, "0.5.2", spec.ProtocolVersion)
}
