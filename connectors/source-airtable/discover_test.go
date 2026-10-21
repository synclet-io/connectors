package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
	"github.com/synclet-io/synclet-go-sdk/testkit"
)

const (
	tasksStream    = "Projects/Tasks"
	allTypesStream = "Projects/All Types"
	contactsStream = "CRM (appBase0000000002)/Contacts"
	dealsStream    = "CRM (appBase0000000003)/Deals"
)

func discover(t *testing.T, src Source, cfg Config) *testkit.Output {
	t.Helper()
	raw, err := json.Marshal(cfg)
	require.NoError(t, err)
	out, err := testkit.RunSource(t.Context(), src, synclet.Request{Command: synclet.CommandDiscover, Config: raw})
	require.NoError(t, err)
	return out
}

type schemaOf struct {
	Properties map[string]json.RawMessage `json:"properties"`
	BaseID     string                     `json:"x-airtable-base-id"`
	TableID    string                     `json:"x-airtable-table-id"`
}

func TestDiscoverStreams(t *testing.T) {
	srv, h := fixtureServer(t, nil)
	out := discover(t, testSource(srv), testCfg)
	require.NoError(t, out.Err)
	var names []string
	byName := map[string]synclet.Stream{}
	for _, s := range out.Catalog().Streams {
		names = append(names, s.Name)
		byName[s.Name] = s
	}
	assert.Equal(t, []string{tasksStream, allTypesStream, contactsStream, dealsStream}, names, "bases with the same name carry their id")
	reqs := h.list()
	require.Len(t, reqs, 5)
	assert.Equal(t, []string{"meta/bases", "meta/bases?offset=itrBases2"}, reqs[:2])
	assert.ElementsMatch(t, []string{"meta/bases/" + base1 + "/tables", "meta/bases/" + base2 + "/tables", "meta/bases/" + base3 + "/tables"}, reqs[2:])

	tasks := byName[tasksStream]
	assert.Equal(t, []synclet.SyncMode{synclet.SyncModeFullRefresh, synclet.SyncModeIncremental}, tasks.SupportedSyncModes)
	assert.False(t, tasks.SourceDefinedCursor)
	assert.Equal(t, []string{"Last Modified"}, tasks.DefaultCursorField)
	assert.Equal(t, [][]string{{"_airtable_id"}}, tasks.SourceDefinedPrimaryKey)

	var s schemaOf
	require.NoError(t, json.Unmarshal(tasks.JSONSchema, &s))
	assert.Equal(t, base1, s.BaseID)
	assert.Equal(t, "tblTasks00000001", s.TableID)
	assert.ElementsMatch(t, []string{"_airtable_id", "_airtable_created_time", "Name", "Status", "Hours", "Last Modified", "Score", "Owner Names"}, keys(s.Properties))
	assert.JSONEq(t, `{"type":["string","null"],"x-airtable-field-id":"fldTaskName000001"}`, string(s.Properties["Name"]))
	assert.JSONEq(t, `{"type":"string"}`, string(s.Properties["_airtable_id"]))
	assert.JSONEq(t, `{"type":["string","null"],"format":"date-time","airbyte_type":"timestamp_with_timezone"}`, string(s.Properties["_airtable_created_time"]))

	contacts := byName[contactsStream]
	assert.Equal(t, []synclet.SyncMode{synclet.SyncModeFullRefresh}, contacts.SupportedSyncModes, "a table without a last-modified-time field is full refresh only")
	assert.Empty(t, contacts.DefaultCursorField)
}

func keys(m map[string]json.RawMessage) []string {
	var out []string
	for k := range m {
		out = append(out, k)
	}
	return out
}

func TestDiscoverTypeMapping(t *testing.T) {
	const (
		str       = `{"type":["string","null"]}`
		num       = `{"type":["number","null"]}`
		integer   = `{"type":["integer","null"]}`
		ts        = `{"type":["string","null"],"format":"date-time","airbyte_type":"timestamp_with_timezone"}`
		strArray  = `{"type":["array","null"],"items":{"type":"string"}}`
		collab    = `{"type":["object","null"],"properties":{"id":{"type":["string","null"]},"email":{"type":["string","null"]},"name":{"type":["string","null"]}}}`
		thumbnail = `{"type":["object","null"],"properties":{"url":{"type":["string","null"]},"width":{"type":["integer","null"]},"height":{"type":["integer","null"]}}}`
	)
	want := map[string]string{
		"Single Line": str,
		"Long Text":   str,
		"Rich":        str,
		"Email":       str,
		"Website":     str,
		"Phone":       str,
		"Stage":       str,
		"Sync Source": str,
		"Amount":      num,
		"Progress":    num,
		"Price":       num,
		"Time Spent":  num,
		"Ticket":      integer,
		"Link Count":  integer,
		"Stars":       integer,
		"Done":        `{"type":["boolean","null"]}`,
		"Due":         `{"type":["string","null"],"format":"date"}`,
		"Starts At":   ts,
		"Created":     ts,
		"Modified":    ts,
		"Tags":        strArray,
		"Linked":      strArray,
		"Owner":       collab,
		"Created By":  collab,
		"Modified By": collab,
		"Watchers":    `{"type":["array","null"],"items":` + collab + `}`,
		"Files": `{"type":["array","null"],"items":{"type":["object","null"],"properties":{
			"id":{"type":["string","null"]},"url":{"type":["string","null"]},"filename":{"type":["string","null"]},
			"size":{"type":["integer","null"]},"type":{"type":["string","null"]},"width":{"type":["integer","null"]},"height":{"type":["integer","null"]},
			"thumbnails":{"type":["object","null"],"properties":{"small":` + thumbnail + `,"large":` + thumbnail + `,"full":` + thumbnail + `}}}}}`,
		"Barcode":         `{"type":["object","null"],"properties":{"text":{"type":["string","null"]},"type":{"type":["string","null"]}}}`,
		"Open":            `{"type":["object","null"],"properties":{"label":{"type":["string","null"]},"url":{"type":["string","null"]}}}`,
		"Summary":         `{"type":["object","null"],"properties":{"state":{"type":["string","null"]},"value":{"type":["string","null"]},"isStale":{"type":["boolean","null"]},"errorType":{"type":["string","null"]}}}`,
		"Total":           num,
		"Next Due":        ts,
		"Broken Formula":  `{}`,
		"Sum Hours":       num,
		"Linked Names":    `{"type":["array","null"],"items":` + str + `}`,
		"Linked Modified": `{"type":["array","null"],"items":` + ts + `}`,
		"Linked Scores":   `{"type":["array","null"],"items":` + num + `}`,
		"Future":          `{}`,
	}
	srv, _ := fixtureServer(t, nil)
	out := discover(t, testSource(srv), Config{PersonalAccessToken: testToken, BaseIDs: []string{base1}})
	require.NoError(t, out.Err)
	var stream synclet.Stream
	for _, s := range out.Catalog().Streams {
		if s.Name == allTypesStream {
			stream = s
		}
	}
	var s schemaOf
	require.NoError(t, json.Unmarshal(stream.JSONSchema, &s))
	assert.Len(t, s.Properties, len(want)+2)
	for name, schema := range want {
		raw, ok := s.Properties[name]
		if !assert.True(t, ok, name) {
			continue
		}
		var got map[string]any
		require.NoError(t, json.Unmarshal(raw, &got))
		assert.Regexp(t, `^fldAllTypes\d{6}$`, got[hintField], name)
		delete(got, hintField)
		b, err := json.Marshal(got)
		require.NoError(t, err)
		assert.JSONEq(t, schema, string(b), name)
	}
}

func TestDiscoverConfiguredBases(t *testing.T) {
	t.Run("one base of a shared name keeps the plain name", func(t *testing.T) {
		srv, _ := fixtureServer(t, nil)
		out := discover(t, testSource(srv), Config{PersonalAccessToken: testToken, BaseIDs: []string{base2}})
		require.NoError(t, out.Err)
		require.Len(t, out.Catalog().Streams, 1)
		assert.Equal(t, "CRM/Contacts", out.Catalog().Streams[0].Name)
	})

	t.Run("a base the token cannot see", func(t *testing.T) {
		srv, _ := fixtureServer(t, nil)
		out := discover(t, testSource(srv), Config{PersonalAccessToken: testToken, BaseIDs: []string{missingBase}})
		require.Error(t, out.Err)
		require.NotEmpty(t, out.ErrorTraces())
		assert.Equal(t, "base appBase0000000009 is not shared with the token or does not exist", out.ErrorTraces()[0].Message)
		assert.Equal(t, synclet.FailureType("config_error"), out.ErrorTraces()[0].FailureType)
	})

	t.Run("a base whose tables are forbidden", func(t *testing.T) {
		srv, _ := fixtureServer(t, map[string]fixture{"meta/bases/" + base1 + "/tables": {status: http.StatusForbidden, file: "errors/forbidden.json"}})
		out := discover(t, testSource(srv), Config{PersonalAccessToken: testToken, BaseIDs: []string{base1}})
		require.Error(t, out.Err)
		assert.Contains(t, out.ErrorTraces()[0].Message, "base appBase0000000001 is not shared with the token or does not exist")
	})
}

func TestDiscoverFetchesTablesConcurrently(t *testing.T) {
	const n = 12
	var inFlight, peak atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p := strings.TrimPrefix(r.URL.Path, "/v0/")
		if p == "meta/bases" {
			var bases []string
			for i := range n {
				bases = append(bases, fmt.Sprintf(`{"id":"appConc%010d","name":"Base %02d"}`, i, i))
			}
			_, _ = fmt.Fprintf(w, `{"bases":[%s]}`, strings.Join(bases, ","))
			return
		}
		cur := inFlight.Add(1)
		defer inFlight.Add(-1)
		for {
			old := peak.Load()
			if cur <= old || peak.CompareAndSwap(old, cur) {
				break
			}
		}
		var i int
		_, err := fmt.Sscanf(p, "meta/bases/appConc%010d/tables", &i)
		require.NoError(t, err)
		time.Sleep(time.Duration(n-i) * 5 * time.Millisecond)
		_, _ = fmt.Fprintf(w, `{"tables":[{"id":"tbl%013d","name":"Table","fields":[]}]}`, i)
	}))
	t.Cleanup(srv.Close)

	out := discover(t, testSource(srv), testCfg)
	require.NoError(t, out.Err)
	var names []string
	for _, s := range out.Catalog().Streams {
		names = append(names, s.Name)
	}
	var want []string
	for i := range n {
		want = append(want, fmt.Sprintf("Base %02d/Table", i))
	}
	assert.Equal(t, want, names, "streams follow the order of the bases")
	assert.Greater(t, peak.Load(), int32(1), "tables of several bases are fetched at once")
	assert.LessOrEqual(t, peak.Load(), int32(discoverConcurrency))
}
