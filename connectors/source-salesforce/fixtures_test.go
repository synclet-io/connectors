package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
	"github.com/synclet-io/synclet-go-sdk/testkit"
)

// Synthetic credentials of the fixtures; none belongs to a real org.
const (
	testClientID            = "test-client-id"
	testClientSecret        = "test-client-secret"
	testRefreshToken        = "test-refresh-token"
	testSandboxRefreshToken = "test-sandbox-refresh-token"
	testRotatedRefreshToken = "test-rotated-refresh-token"
	accessTokenPrefix       = "test-access-token-"
)

// secrets are the credentials no request URL may carry.
var secrets = []string{testClientSecret, testRefreshToken, testSandboxRefreshToken, testRotatedRefreshToken, accessTokenPrefix}

// fixture is a response the fixture server sends instead of its default:
// status with the body of testdata/<file>.json. An empty file keeps the
// default body.
type fixture struct {
	status int
	file   string
}

// apiRequest is one request the fixture server received. path is under
// /services/data/<apiVersion>/ for an API request and "login/token" or
// "test/token" for a token request; q is the SOQL query, if any.
type apiRequest struct {
	method string
	path   string
	q      string
}

// fakeOrg is a fixture server that plays one Salesforce org and both OAuth
// hosts.
type fakeOrg struct {
	t         *testing.T
	srv       *httptest.Server
	overrides map[string][]fixture

	mu        sync.Mutex
	reqs      []apiRequest
	served    map[string]int
	issued    int
	expired   map[string]bool
	expiring  int
	limitInfo string
}

// fixtureServer serves the Salesforce REST API and the OAuth token endpoint
// from testdata over TLS:
//
//   - POST /login/services/oauth2/token accepts testRefreshToken, POST
//     /test/services/oauth2/token testSandboxRefreshToken; both answer
//     testdata/token.json with a new access token, or
//     errors/invalid_client_id, errors/invalid_client or errors/invalid_grant;
//   - GET /services/data/<apiVersion>/<p> answers testdata/<p>.json, and a
//     query or queryAll request answers queryFile(q).
//
// {{instance_url}} in a body becomes the server's URL. overrides["METHOD
// file"] replaces the default fixture of that request with its fixtures in
// order, repeating the last. An API request without an access token issued
// by the server, or with an expired one, gets errors/INVALID_SESSION_ID.
// Every API response carries the Sforce-Limit-Info header. A request that
// carries a credential in its URL fails the test.
func fixtureServer(t *testing.T, overrides map[string][]fixture) *fakeOrg {
	t.Helper()
	o := &fakeOrg{t: t, overrides: overrides, served: map[string]int{}, expired: map[string]bool{}, limitInfo: "api-usage=120/15000"}
	o.srv = httptest.NewTLSServer(http.HandlerFunc(o.serve))
	t.Cleanup(o.srv.Close)
	return o
}

func (o *fakeOrg) serve(w http.ResponseWriter, r *http.Request) {
	for _, s := range secrets {
		if strings.Contains(r.URL.String(), s) {
			o.t.Errorf("request %s carries a credential in its URL", r.URL.Path)
		}
	}
	var f fixture
	var token string
	switch r.URL.Path {
	case "/login/services/oauth2/token", "/test/services/oauth2/token":
		f = o.tokenFixture(r)
	default:
		p, ok := strings.CutPrefix(r.URL.Path, "/services/data/"+apiVersion+"/")
		if !ok || r.Method != http.MethodGet {
			http.NotFound(w, r)
			return
		}
		q := r.URL.Query().Get("q")
		o.add(apiRequest{method: r.Method, path: p, q: q})
		f.file = p
		if p == "query" || p == "queryAll" {
			f.file = queryFile(q)
		}
		if !o.authorized(r.Header.Get("Authorization")) {
			f = fixture{status: http.StatusUnauthorized, file: "errors/INVALID_SESSION_ID"}
		}
	}
	key := r.Method + " " + f.file
	o.mu.Lock()
	if seq, ok := o.overrides[key]; ok {
		ov := seq[min(o.served[key], len(seq)-1)]
		o.served[key]++
		f.status = ov.status
		if ov.file != "" {
			f.file = ov.file
		}
	}
	if f.status == 0 {
		f.status = http.StatusOK
	}
	if f.status == http.StatusOK && strings.HasSuffix(r.URL.Path, "/oauth2/token") {
		o.issued++
		token = accessTokenPrefix + strconv.Itoa(o.issued)
		if o.expiring > 0 {
			o.expired[token] = true
			o.expiring--
		}
	}
	limit := o.limitInfo
	o.mu.Unlock()
	body, err := os.ReadFile(filepath.Join("testdata", filepath.FromSlash(f.file)+".json"))
	if err != nil {
		o.t.Errorf("no fixture for %s %s (q=%s): %v", r.Method, r.URL.Path, r.URL.Query().Get("q"), err)
		http.NotFound(w, r)
		return
	}
	body = []byte(strings.NewReplacer("{{instance_url}}", o.srv.URL, "{{access_token}}", token).Replace(string(body)))
	w.Header().Set("Content-Type", "application/json")
	if !strings.HasSuffix(r.URL.Path, "/oauth2/token") {
		w.Header().Set("Sforce-Limit-Info", limit)
	}
	w.WriteHeader(f.status)
	_, _ = w.Write(body)
}

func (o *fakeOrg) tokenFixture(r *http.Request) fixture {
	if err := r.ParseForm(); err != nil {
		o.t.Errorf("parse token form: %v", err)
	}
	host, _, _ := strings.Cut(strings.TrimPrefix(r.URL.Path, "/"), "/")
	o.add(apiRequest{method: r.Method, path: host + "/token"})
	valid := testRefreshToken
	if host == "test" {
		valid = testSandboxRefreshToken
	}
	switch {
	case r.Method != http.MethodPost || r.PostForm.Get("grant_type") != "refresh_token":
		return fixture{status: http.StatusBadRequest, file: "errors/unsupported_grant_type"}
	case r.PostForm.Get("client_id") != testClientID:
		return fixture{status: http.StatusBadRequest, file: "errors/invalid_client_id"}
	case r.PostForm.Get("client_secret") != testClientSecret:
		return fixture{status: http.StatusBadRequest, file: "errors/invalid_client"}
	case r.PostForm.Get("refresh_token") != valid && r.PostForm.Get("refresh_token") != testRotatedRefreshToken:
		return fixture{status: http.StatusBadRequest, file: "errors/invalid_grant"}
	}
	return fixture{file: "token"}
}

// authorized reports whether header carries an access token the server
// issued and has not expired.
func (o *fakeOrg) authorized(header string) bool {
	token, ok := strings.CutPrefix(header, "Bearer ")
	if !ok {
		return false
	}
	n, err := strconv.Atoi(strings.TrimPrefix(token, accessTokenPrefix))
	o.mu.Lock()
	defer o.mu.Unlock()
	return strings.HasPrefix(token, accessTokenPrefix) && err == nil && n >= 1 && n <= o.issued && !o.expired[token]
}

func (o *fakeOrg) add(r apiRequest) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.reqs = append(o.reqs, r)
}

// expireNext makes the API reject the next n access tokens the server
// issues with INVALID_SESSION_ID, as if each had expired at once.
func (o *fakeOrg) expireNext(n int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.expiring = n
}

// override replaces the default fixture of key from now on, as the
// overrides of fixtureServer do.
func (o *fakeOrg) override(key string, fs ...fixture) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.overrides == nil {
		o.overrides = map[string][]fixture{}
	}
	o.overrides[key] = fs
	o.served[key] = 0
}

// setLimit sets the Sforce-Limit-Info header of later API responses.
func (o *fakeOrg) setLimit(v string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.limitInfo = v
}

func (o *fakeOrg) list() []apiRequest {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]apiRequest(nil), o.reqs...)
}

// keys returns "METHOD path" of every request.
func (o *fakeOrg) keys() []string {
	var out []string
	for _, r := range o.list() {
		out = append(out, r.method+" "+r.path)
	}
	return out
}

// queries returns "<path> <q>" of every query and queryAll request.
func (o *fakeOrg) queries() []string {
	var out []string
	for _, r := range o.list() {
		if r.q != "" {
			out = append(out, r.path+" "+r.q)
		}
	}
	return out
}

var soqlPattern = regexp.MustCompile(`^SELECT (.+?) FROM (\w+)`)

// queryFile returns the fixture of a SOQL query: query/<Object>, with -ids
// for a SELECT of Id and at most its cursor, -in-<first id>-<first field
// after Id> for a query by a list of ids, and -since for a query that
// continues after a saved cursor.
func queryFile(q string) string {
	m := soqlPattern.FindStringSubmatch(q)
	if m == nil {
		return "query/unparsed"
	}
	fields := strings.Split(m[1], ", ")
	file := "query/" + m[2]
	switch {
	case strings.Contains(q, "Id IN ('"):
		id, _, _ := strings.Cut(q[strings.Index(q, "Id IN ('")+len("Id IN ('"):], "'")
		return file + "-in-" + id + "-" + fields[1]
	case len(fields) <= 2 && (len(fields) == 1 || fields[1] == "SystemModstamp" || fields[1] == "LastModifiedDate"):
		file += "-ids"
	}
	if strings.Contains(q, "Id > '") {
		file += "-since"
	}
	return file
}

// testSource returns a Source that talks to the fixture server and records
// its waits instead of sleeping.
func testSource(o *fakeOrg) (Source, *waits) {
	w := &waits{}
	return Source{
		loginURL:   o.srv.URL + "/login",
		sandboxURL: o.srv.URL + "/test",
		client:     o.srv.Client(),
		retryDelay: time.Second,
		wait:       w.wait,
	}, w
}

// waits records the waits a Source asked for instead of sleeping.
type waits struct {
	mu sync.Mutex
	ds []time.Duration
}

func (w *waits) wait(ctx context.Context, d time.Duration) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.ds = append(w.ds, d)
	return ctx.Err()
}

func (w *waits) list() []time.Duration {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]time.Duration(nil), w.ds...)
}

// testCfg returns the configuration of the synthetic production org.
func testCfg(o *fakeOrg) Config {
	return Config{
		ClientID:     testClientID,
		ClientSecret: testClientSecret,
		RefreshToken: testRefreshToken,
		InstanceURL:  o.srv.URL,
		StartDate:    "2026-01-01",
	}
}

func rawConfig(t *testing.T, cfg Config) json.RawMessage {
	t.Helper()
	raw, err := json.Marshal(cfg)
	require.NoError(t, err)
	return raw
}

func run(t *testing.T, src Source, req synclet.Request) *testkit.Output {
	t.Helper()
	out, err := testkit.RunSource(t.Context(), src, req)
	require.NoError(t, err)
	return out
}

// discoverStreams returns the discovered streams by name.
func discoverStreams(t *testing.T, src Source, cfg Config) map[string]synclet.Stream {
	t.Helper()
	out := run(t, src, synclet.Request{Command: synclet.CommandDiscover, Config: rawConfig(t, cfg)})
	require.NoError(t, out.Err)
	require.NotNil(t, out.Catalog())
	streams := map[string]synclet.Stream{}
	for _, s := range out.Catalog().Streams {
		streams[s.Name] = s
	}
	return streams
}

func catalogOf(t *testing.T, src Source, cfg Config, mode synclet.SyncMode, names ...string) *synclet.ConfiguredCatalog {
	t.Helper()
	streams := discoverStreams(t, src, cfg)
	cat := &synclet.ConfiguredCatalog{}
	for _, n := range names {
		s, ok := streams[n]
		require.True(t, ok, "stream %s not discovered", n)
		cat.Streams = append(cat.Streams, synclet.ConfiguredStream{
			Stream: s, SyncMode: mode, DestinationSyncMode: synclet.DestinationSyncModeAppendDedup,
		})
	}
	return cat
}

func streamState(name, state string) json.RawMessage {
	return json.RawMessage(`[{"type":"STREAM","stream":{"stream_descriptor":{"name":"` + name + `"},"stream_state":` + state + `}}]`)
}

// runRead reads catalog with cfg from state and returns the output, whose
// Err is the read's error.
func runRead(t *testing.T, src Source, cfg Config, catalog *synclet.ConfiguredCatalog, state json.RawMessage) *testkit.Output {
	t.Helper()
	raw, err := json.Marshal(catalog)
	require.NoError(t, err)
	return run(t, src, synclet.Request{Command: synclet.CommandRead, Config: rawConfig(t, cfg), Catalog: raw, State: state})
}

// records returns the records of stream decoded as maps.
func records(t *testing.T, out *testkit.Output, stream string) []map[string]any {
	t.Helper()
	var recs []map[string]any
	for _, r := range out.Records(synclet.StreamDescriptor{Name: stream}) {
		dec := json.NewDecoder(strings.NewReader(string(r.Data)))
		dec.UseNumber()
		var m map[string]any
		require.NoError(t, dec.Decode(&m))
		recs = append(recs, m)
	}
	return recs
}

func recordIDs(t *testing.T, out *testkit.Output, stream string) []string {
	t.Helper()
	var ids []string
	for _, r := range records(t, out, stream) {
		ids = append(ids, r["Id"].(string))
	}
	return ids
}

// states returns the stream_state of every STATE message, in order.
func states(out *testkit.Output) []string {
	var got []string
	for _, m := range out.Messages {
		if m.Type == synclet.MessageTypeState && m.State.Stream != nil {
			got = append(got, string(m.State.Stream.StreamState))
		}
	}
	return got
}
