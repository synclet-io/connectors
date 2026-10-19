package engine

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/fstest"
	"time"

	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
	"github.com/synclet-io/synclet-go-sdk/testkit"

	"source-declarative/internal/manifest"
)

const itemSchema = `{type: object, properties: {id: {type: string}, updated: {type: [integer, string]}, name: {type: [string, "null"]}, parent_id: {type: string}, base: {type: string}, value: {}, account: {type: string}, day: {type: string}}, additionalProperties: true}`

// manifestYAML is a manifest with a fixed head (config spec, check of
// items, bearer auth unless top sets auth) and the given streams.
func manifestYAML(top, streams string) string {
	if !strings.Contains(top, "auth:") {
		top += "auth: {type: bearer, token: \"{{ config.api_key }}\"}\n"
	}
	return `version: 1
connector: {name: source-test}
base_url: https://api.invalid/v1/
spec:
  required: [api_key]
  properties:
    api_key: {type: string, secret: true}
    user: {type: string}
    start_date: {type: string}
    accounts: {type: array, items: {type: string}}
    region: {type: string}
    client_id: {type: string}
    client_secret: {type: string, secret: true}
    refresh_token: {type: string, secret: true}
check: {stream: items}
` + top + "streams:\n" + streams
}

func load(t *testing.T, yaml string) *manifest.Manifest {
	t.Helper()
	m, err := manifest.LoadFS(fstest.MapFS{manifest.FileName: {Data: []byte(yaml)}})
	require.NoError(t, err)
	return m
}

// reply is one fixture response.
type reply struct {
	status  int
	body    string
	headers map[string]string
}

// fakeAPI answers requests by "path?sorted query" (or the path alone) with
// the replies registered for it in order, repeating the last; it records
// every request.
type fakeAPI struct {
	t       *testing.T
	mu      sync.Mutex
	replies map[string][]reply
	served  map[string]int
	reqs    []*http.Request
	bodies  []string
	srv     *httptest.Server
}

func newAPI(t *testing.T) *fakeAPI {
	t.Helper()
	a := &fakeAPI{t: t, replies: map[string][]reply{}, served: map[string]int{}}
	a.srv = httptest.NewServer(http.HandlerFunc(a.serve))
	t.Cleanup(a.srv.Close)
	return a
}

func (a *fakeAPI) on(key string, rs ...reply) *fakeAPI {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.replies[key] = append(a.replies[key], rs...)
	return a
}

func (a *fakeAPI) json(key, body string) *fakeAPI {
	return a.on(key, reply{status: 200, body: body})
}

func requestKey(r *http.Request) string {
	if q := r.URL.Query(); len(q) > 0 {
		return r.URL.EscapedPath() + "?" + q.Encode()
	}
	return r.URL.EscapedPath()
}

func (a *fakeAPI) serve(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	a.mu.Lock()
	a.reqs = append(a.reqs, r.Clone(context.Background()))
	a.bodies = append(a.bodies, string(body))
	key := requestKey(r)
	rs, ok := a.replies[key]
	if !ok {
		key = r.URL.EscapedPath()
		rs, ok = a.replies[key]
	}
	n := a.served[key]
	a.served[key]++
	a.mu.Unlock()
	if !ok {
		a.t.Errorf("no fixture for %s", requestKey(r))
		http.NotFound(w, r)
		return
	}
	rep := rs[min(n, len(rs)-1)]
	for k, v := range rep.headers {
		w.Header().Set(k, v)
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(rep.status)
	_, _ = w.Write([]byte(rep.body))
}

// keys returns the requests made, as "path?sorted query".
func (a *fakeAPI) keys() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make([]string, len(a.reqs))
	for i, r := range a.reqs {
		out[i] = requestKey(r)
	}
	return out
}

func (a *fakeAPI) requests() []*http.Request {
	a.mu.Lock()
	defer a.mu.Unlock()
	return slices.Clone(a.reqs)
}

// clock is a fake clock whose sleeps advance it and are recorded.
type clock struct {
	mu     sync.Mutex
	now    time.Time
	sleeps []time.Duration
}

func newClock() *clock { return &clock{now: time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC)} }

func (c *clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *clock) Sleep(ctx context.Context, d time.Duration) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.sleeps = append(c.sleeps, d)
	c.now = c.now.Add(d)
	return ctx.Err()
}

func (c *clock) waits() []time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(c.sleeps)
}

func (a *fakeAPI) options(c *clock) Options {
	return Options{BaseURL: a.srv.URL + "/v1/", Client: a.srv.Client(), Now: c.Now, Sleep: c.Sleep}
}

func newSource(t *testing.T, yaml string, a *fakeAPI, c *clock) synclet.Source[json.RawMessage] {
	t.Helper()
	return New(load(t, yaml), a.options(c))
}

const testConfig = `{"api_key":"k1","start_date":"2026-10-01T00:00:00Z"}`

func runCmd(t *testing.T, src synclet.Source[json.RawMessage], req synclet.Request) *testkit.Output {
	t.Helper()
	out, err := testkit.RunSource(t.Context(), src, req)
	require.NoError(t, err)
	return out
}

func checkStatus(t *testing.T, src synclet.Source[json.RawMessage], config string) *synclet.ConnectionStatus {
	t.Helper()
	out := runCmd(t, src, synclet.Request{Command: synclet.CommandCheck, Config: json.RawMessage(config)})
	st := out.ConnectionStatus()
	require.NotNil(t, st, "no CONNECTION_STATUS: %+v", out.Messages)
	return st
}

// catalog configures the named streams of src's discover in mode.
func catalog(t *testing.T, src synclet.Source[json.RawMessage], config string, mode synclet.SyncMode, names ...string) *synclet.ConfiguredCatalog {
	t.Helper()
	out := runCmd(t, src, synclet.Request{Command: synclet.CommandDiscover, Config: json.RawMessage(config)})
	require.NoError(t, out.Err)
	cat := out.Catalog()
	require.NotNil(t, cat)
	cc := &synclet.ConfiguredCatalog{}
	for _, n := range names {
		var found bool
		for _, s := range cat.Streams {
			if s.Name == n {
				cc.Streams = append(cc.Streams, synclet.ConfiguredStream{Stream: s, SyncMode: mode, DestinationSyncMode: synclet.DestinationSyncModeAppend})
				found = true
			}
		}
		require.True(t, found, "stream %s not discovered", n)
	}
	return cc
}

func read(t *testing.T, src synclet.Source[json.RawMessage], config string, cat *synclet.ConfiguredCatalog, state string) *testkit.Output {
	t.Helper()
	raw, err := json.Marshal(cat)
	require.NoError(t, err)
	req := synclet.Request{Command: synclet.CommandRead, Config: json.RawMessage(config), Catalog: raw}
	if state != "" {
		req.State = json.RawMessage(state)
	}
	return runCmd(t, src, req)
}

// values returns field of every record of stream, as JSON text.
func values(out *testkit.Output, stream, field string) []string {
	var vs []string
	for _, r := range out.Records(synclet.StreamDescriptor{Name: stream}) {
		var m map[string]json.RawMessage
		_ = json.Unmarshal(r.Data, &m)
		vs = append(vs, string(m[field]))
	}
	return vs
}

// states returns the stream_state of every STATE of stream.
func states(out *testkit.Output, stream string) []string {
	var ss []string
	for _, s := range out.States() {
		if s.Stream != nil && s.Stream.StreamDescriptor.Name == stream {
			ss = append(ss, string(s.Stream.StreamState))
		}
	}
	return ss
}

func traceMessage(out *testkit.Output) (string, synclet.FailureType) {
	for _, e := range out.ErrorTraces() {
		return e.Message, e.FailureType
	}
	return "", ""
}
