package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
	"github.com/synclet-io/synclet-go-sdk/testkit"
)

const (
	testPAT          = "dapi-test"
	testClientID     = "sp-client"
	testClientSecret = "sp-secret"
	testOAuthToken   = "oauth-token"
	testHTTPPath     = "/sql/1.0/warehouses/abc123"
)

// fakeWorkspace is a Databricks workspace over HTTPS: the OAuth token
// endpoint, the Files API and, for any other path, the SQL warehouse's
// Thrift endpoint answering with sqlStatus.
type fakeWorkspace struct {
	srv *httptest.Server

	mu        sync.Mutex
	files     map[string][]byte
	ops       []string
	sqlStatus int
	// filesStatus, when set, answers a Files API request with an error
	// status instead of serving it.
	filesStatus func(method, path string) int
}

func newWorkspace(t *testing.T) *fakeWorkspace {
	t.Helper()
	ws := &fakeWorkspace{files: map[string][]byte{}, sqlStatus: http.StatusUnauthorized}
	ws.srv = httptest.NewTLSServer(http.HandlerFunc(ws.serve))
	t.Cleanup(ws.srv.Close)
	return ws
}

func (ws *fakeWorkspace) serve(w http.ResponseWriter, r *http.Request) {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	if r.URL.Path == "/oidc/v1/token" {
		id, secret, _ := r.BasicAuth()
		_ = r.ParseForm()
		if id != testClientID || secret != testClientSecret || r.Form.Get("grant_type") != "client_credentials" || r.Form.Get("scope") != "all-apis" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = io.WriteString(w, `{"error":"invalid_client","error_description":"Client authentication failed"}`)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"access_token":%q,"token_type":"Bearer","expires_in":3600}`, testOAuthToken)
		return
	}
	files, isFiles := strings.CutPrefix(r.URL.Path, "/api/2.0/fs/files")
	dirs, isDirs := strings.CutPrefix(r.URL.Path, "/api/2.0/fs/directories")
	if !isFiles && !isDirs {
		w.WriteHeader(ws.sqlStatus)
		return
	}
	path := files
	if isDirs {
		path = dirs
	}
	ws.ops = append(ws.ops, r.Method+" "+path)
	if auth := r.Header.Get("Authorization"); auth != "Bearer "+testPAT && auth != "Bearer "+testOAuthToken {
		apiErr(w, http.StatusUnauthorized, "UNAUTHENTICATED", "Invalid access token.")
		return
	}
	if ws.filesStatus != nil {
		if code := ws.filesStatus(r.Method, path); code != 0 {
			apiErr(w, code, "PERMISSION_DENIED", "User does not have WRITE VOLUME on Volume.")
			return
		}
	}
	switch {
	case isFiles && r.Method == http.MethodPut:
		body, _ := io.ReadAll(r.Body)
		ws.files[path] = body
		w.WriteHeader(http.StatusNoContent)
	case isFiles && r.Method == http.MethodDelete:
		delete(ws.files, path)
	case isDirs && r.Method == http.MethodDelete:
		for f := range ws.files {
			if strings.HasPrefix(f, path+"/") {
				apiErr(w, http.StatusConflict, "DIRECTORY_NOT_EMPTY", "directory is not empty")
				return
			}
		}
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func apiErr(w http.ResponseWriter, code int, errCode, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_, _ = fmt.Fprintf(w, `{"error_code":%q,"message":%q}`, errCode, msg)
}

func (ws *fakeWorkspace) file(path string) []byte {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	return ws.files[path]
}

func (ws *fakeWorkspace) fileCount() int {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	return len(ws.files)
}

func (ws *fakeWorkspace) opsList() []string {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	return append([]string(nil), ws.ops...)
}

func (ws *fakeWorkspace) config(t *testing.T) Config {
	t.Helper()
	u, err := url.Parse(ws.srv.URL)
	require.NoError(t, err)
	port, err := strconv.Atoi(u.Port())
	require.NoError(t, err)
	return Config{
		Hostname: u.Hostname(), Port: port, HTTPPath: testHTTPPath,
		Database: "main", Schema: "raw", Volume: "stg",
		Authentication: Auth{AuthType: authPAT, PersonalAccessToken: testPAT},
	}
}

// destination returns the connector talking to ws, with sql as its SQL
// warehouse; a nil sql connects the real driver to ws.
func (ws *fakeWorkspace) destination(sql *fakeSQL) Destination {
	d := NewDestination()
	d.httpClient = ws.srv.Client()
	d.retries = 0
	if sql != nil {
		d.openSQL = func(context.Context, Config, *http.Client, *bearer, int) (sqlRunner, error) { return sql, nil }
	}
	return d
}

// fakeSQL records the statements it runs. information_schema finds the
// catalog, schema and volume unless missing names them and the columns of
// the tables in columns; COPY INTO reports the lines of the staged file it
// reads, and INSERT the rows last copied into the stage table it reads.
type fakeSQL struct {
	ws *fakeWorkspace

	mu      sync.Mutex
	stmts   []string
	log     []string
	missing map[string]bool
	columns map[string][]map[string]string
	staged  map[string]int
	// fail, when set, fails a statement with the error it returns.
	fail func(stmt string) error
	// copied, when set, overrides the row count COPY INTO reports.
	copied func(stmt string, lines int) int
	closed bool
}

func newFakeSQL(ws *fakeWorkspace) *fakeSQL {
	return &fakeSQL{ws: ws, missing: map[string]bool{}, columns: map[string][]map[string]string{}, staged: map[string]int{}}
}

func (f *fakeSQL) exec(ctx context.Context, q string) error {
	_, err := f.query(ctx, q)
	return err
}

var (
	copyInto   = regexp.MustCompile(`^COPY INTO (\S+) FROM .* FROM '([^']+)'\)`)
	insertFrom = regexp.MustCompile(`^INSERT INTO .* FROM (\S+)$`)
	colsQuery  = regexp.MustCompile(`information_schema\.columns WHERE table_schema = '([^']+)' AND table_name = '([^']+)'`)
)

func (f *fakeSQL) query(_ context.Context, q string) ([]map[string]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.stmts = append(f.stmts, q)
	f.log = append(f.log, q)
	if f.fail != nil {
		if err := f.fail(q); err != nil {
			return nil, err
		}
	}
	one := []map[string]string{{"1": "1"}}
	switch {
	case strings.Contains(q, "information_schema.catalogs"):
		if f.missing["catalog"] {
			return nil, nil
		}
		return one, nil
	case strings.Contains(q, "information_schema.schemata"):
		if f.missing["schema"] {
			return nil, nil
		}
		return one, nil
	case strings.Contains(q, "information_schema.volumes"):
		if f.missing["volume"] {
			return nil, nil
		}
		return one, nil
	}
	if m := colsQuery.FindStringSubmatch(q); m != nil {
		return f.columns[m[2]], nil
	}
	if m := copyInto.FindStringSubmatch(q); m != nil {
		lines := bytes.Count(f.ws.file(m[2]), []byte("\n"))
		if f.copied != nil {
			lines = f.copied(q, lines)
		}
		f.staged[m[1]] = lines
		return []map[string]string{{"num_affected_rows": strconv.Itoa(lines), "num_inserted_rows": strconv.Itoa(lines)}}, nil
	}
	if m := insertFrom.FindStringSubmatch(q); m != nil {
		n := strconv.Itoa(f.staged[m[1]])
		return []map[string]string{{"num_affected_rows": n, "num_inserted_rows": n}}, nil
	}
	return nil, nil
}

func (f *fakeSQL) close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = true
	return nil
}

func (f *fakeSQL) statements() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.stmts...)
}

// matching returns the statements that start with prefix.
func (f *fakeSQL) matching(prefix string) []string {
	var out []string
	for _, s := range f.statements() {
		if strings.HasPrefix(s, prefix) {
			out = append(out, s)
		}
	}
	return out
}

// errDenied is how Databricks refuses a statement for a missing privilege.
var errDenied = errors.New("[INSUFFICIENT_PERMISSIONS] Insufficient privileges: User does not have CREATE TABLE on Schema 'main.raw'. SQLSTATE: 42501")

// closedPort returns a local port nothing listens on.
func closedPort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := l.Addr().(*net.TCPAddr).Port
	require.NoError(t, l.Close())
	return port
}

// event is one statement the warehouse ran or one STATE the connector
// echoed, in the order they happened.
func (f *fakeSQL) events() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.log...)
}

// stateLog is the connector's output stream; it adds every STATE it carries
// to the warehouse's event log.
type stateLog struct {
	f   *fakeSQL
	out bytes.Buffer
}

func (s *stateLog) Write(p []byte) (int, error) {
	s.out.Write(p)
	msgs, err := testkit.ReadMessages(bytes.NewReader(p))
	if err != nil {
		return 0, err
	}
	s.f.mu.Lock()
	defer s.f.mu.Unlock()
	for _, m := range msgs {
		if m.Type == synclet.MessageTypeState {
			s.f.log = append(s.f.log, "STATE "+stateText(*m.State))
		}
	}
	return len(p), nil
}

// stateText is a STREAM state as "<stream> <state>".
func stateText(st synclet.StateMessage) string {
	if st.Type != synclet.StateTypeStream || st.Stream == nil {
		return string(st.Type)
	}
	return st.Stream.StreamDescriptor.Name + " " + string(st.Stream.StreamState)
}

// runWrite runs the write command with the catalog and messages of testdata
// files or literal JSON, and returns the states echoed and Write's error.
func runWrite(t *testing.T, d Destination, cfg Config, sql *fakeSQL, catalog, messages string) ([]string, error) {
	t.Helper()
	raw, err := json.Marshal(cfg)
	require.NoError(t, err)
	log := &stateLog{f: sql}
	werr := synclet.ServeDestination(t.Context(), d, synclet.Request{
		Command: synclet.CommandWrite, Config: raw, Catalog: json.RawMessage(catalog), Input: strings.NewReader(messages),
	}, log)
	out, err := testkit.ReadMessages(&log.out)
	require.NoError(t, err)
	var states []string
	for _, m := range out {
		if m.Type == synclet.MessageTypeState {
			states = append(states, stateText(*m.State))
		}
	}
	return states, werr
}

func catalogJSON(streams ...string) string {
	return `{"streams":[` + strings.Join(streams, ",") + `]}`
}

func streamJSON(name, mode, schema string, pk, cursor string) string {
	s := `{"stream":{"name":"` + name + `","json_schema":` + schema + `,"supported_sync_modes":["full_refresh","incremental"]},"sync_mode":"incremental","destination_sync_mode":"` + mode + `"`
	if pk != "" {
		s += `,"primary_key":[["` + pk + `"]]`
	}
	if cursor != "" {
		s += `,"cursor_field":["` + cursor + `"]`
	}
	return s + "}"
}

func record(stream, data string) string {
	return `{"type":"RECORD","record":{"stream":"` + stream + `","data":` + data + `,"emitted_at":1704164645123}}` + "\n"
}

func state(stream, data string) string {
	return `{"type":"STATE","state":{"type":"STREAM","stream":{"stream_descriptor":{"name":"` + stream + `"},"stream_state":` + data + `}}}` + "\n"
}
