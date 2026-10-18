package main

import (
	"bytes"
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// Synthetic identifiers and credentials of the fixtures; none belongs to a
// real account.
const (
	testClientEmail  = "synclet-test@synthetic-project.iam.gserviceaccount.com"
	testClientID     = "test-client-id"
	testClientSecret = "test-client-secret"
	testRefreshToken = "test-refresh-token"
	testAccessToken  = "test-access-token"
	testSheetA       = "1SyntheticSpreadsheetA00000000000000000000000"
	testSheetB       = "1SyntheticSpreadsheetB00000000000000000000000"
	jwtGrant         = "urn:ietf:params:oauth:grant-type:jwt-bearer"
)

// secrets are the credentials no request URL may carry.
var secrets = []string{testClientSecret, testRefreshToken, testAccessToken}

// testKey is the synthetic RSA key of the test service account; otherKey
// signs as a revoked key of the same account.
var (
	testKey  = sync.OnceValue(func() *rsa.PrivateKey { return newKey() })
	otherKey = sync.OnceValue(func() *rsa.PrivateKey { return newKey() })
)

func newKey() *rsa.PrivateKey {
	k, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		panic(err)
	}
	return k
}

// serviceAccountKey returns a service account JSON key holding k.
func serviceAccountKey(t *testing.T, k *rsa.PrivateKey) string {
	t.Helper()
	der, err := x509.MarshalPKCS8PrivateKey(k)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(map[string]string{
		"type":           "service_account",
		"project_id":     "synthetic-project",
		"private_key_id": "0123456789abcdef",
		"private_key":    string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})),
		"client_email":   testClientEmail,
		"client_id":      "100000000000000000001",
		"token_uri":      "https://oauth2.googleapis.com/token",
	})
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

// fixture is a response the fixture server sends instead of its default:
// status with the body of testdata/<file>.json, or body itself. An empty
// file and body keep the default body. header is added to the response.
type fixture struct {
	status int
	file   string
	body   []byte
	header map[string]string
}

// apiRequest is one request the fixture server received.
type apiRequest struct {
	method string
	// path is the escaped path, without the query.
	path  string
	query url.Values
	grant string
}

// hits records the requests the fixture server received, in order.
type hits struct {
	mu   sync.Mutex
	reqs []apiRequest
}

func (h *hits) add(r apiRequest) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.reqs = append(h.reqs, r)
}

func (h *hits) list() []apiRequest {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]apiRequest(nil), h.reqs...)
}

// keys returns "METHOD path" of every request.
func (h *hits) keys() []string {
	var out []string
	for _, r := range h.list() {
		out = append(out, r.method+" "+r.path)
	}
	return out
}

// ranges returns the ranges of every values:batchGet request of
// spreadsheet id, one string per request with its ranges joined by ",".
func (h *hits) ranges(id string) []string {
	var out []string
	for _, r := range h.list() {
		if r.path == batchGetPath(id) {
			out = append(out, strings.Join(r.query["ranges"], ","))
		}
	}
	return out
}

func spreadsheetKey(id string) string { return "GET /v4/spreadsheets/" + id }
func batchGetPath(id string) string   { return "/v4/spreadsheets/" + id + "/values:batchGet" }
func batchGetKey(id string) string    { return "GET " + batchGetPath(id) }
func driveKey(id string) string       { return "GET /drive/v3/files/" + id }

// sheetFile is the values fixture of a sheet: testdata/sheets/<id>/values/<title>.json,
// or <title>.unformatted.json for UNFORMATTED_VALUE when it exists.
func sheetFile(id, title string, unformatted bool) string {
	base := filepath.Join("testdata", "sheets", id, "values", strings.ReplaceAll(title, "/", "_"))
	if unformatted {
		if _, err := os.Stat(base + ".unformatted.json"); err == nil {
			return base + ".unformatted.json"
		}
	}
	return base + ".json"
}

// parseRange reads 'Title'!first:last.
func parseRange(r string) (title string, first, last int, ok bool) {
	quoted, rows, found := strings.Cut(r, "'!")
	if !found || !strings.HasPrefix(quoted, "'") {
		return "", 0, 0, false
	}
	title = strings.ReplaceAll(quoted[1:], "''", "'")
	a, b, found := strings.Cut(rows, ":")
	if !found {
		return "", 0, 0, false
	}
	first, err1 := strconv.Atoi(a)
	last, err2 := strconv.Atoi(b)
	return title, first, last, err1 == nil && err2 == nil && first >= 1 && first <= last
}

func emptyRow(row []any) bool {
	for _, v := range row {
		if v != nil && v != "" {
			return false
		}
	}
	return true
}

// batchGetBody answers a values:batchGet of spreadsheet id: each range's
// rows sliced from the sheet's values fixture, trailing empty rows left
// out as the API does. A range past the sheet's rowCount is a 400.
func batchGetBody(t *testing.T, id string, q url.Values) (int, []byte) {
	t.Helper()
	if q.Get("majorDimension") != "ROWS" {
		t.Errorf("batchGet majorDimension %q, want ROWS", q.Get("majorDimension"))
	}
	unformatted := q.Get("valueRenderOption") == "UNFORMATTED_VALUE"
	var meta struct {
		Sheets []struct {
			Properties sheetProperties `json:"properties"`
		} `json:"sheets"`
	}
	readJSON(t, filepath.Join("testdata", "sheets", id, "spreadsheet.json"), &meta)
	rowCounts := map[string]int{}
	for _, s := range meta.Sheets {
		rowCounts[s.Properties.Title] = s.Properties.rowCount()
	}
	var ranges []map[string]any
	for _, r := range q["ranges"] {
		title, first, last, ok := parseRange(r)
		if !ok {
			t.Errorf("batchGet range %q is not 'Title'!first:last", r)
			return http.StatusBadRequest, nil
		}
		if last > rowCounts[title] {
			return http.StatusBadRequest, readFile(t, "errors/bad_range")
		}
		var file struct {
			Values [][]any `json:"values"`
		}
		readJSON(t, sheetFile(id, title, unformatted), &file)
		rows := file.Values[min(first-1, len(file.Values)):min(last, len(file.Values))]
		for len(rows) > 0 && emptyRow(rows[len(rows)-1]) {
			rows = rows[:len(rows)-1]
		}
		vr := map[string]any{"range": r, "majorDimension": "ROWS"}
		if len(rows) > 0 {
			vr["values"] = rows
		}
		ranges = append(ranges, vr)
	}
	raw, err := json.Marshal(map[string]any{"spreadsheetId": id, "valueRanges": ranges})
	if err != nil {
		t.Fatal(err)
	}
	return http.StatusOK, raw
}

func readJSON(t *testing.T, path string, out any) {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	if err := dec.Decode(out); err != nil {
		t.Fatalf("decode %s: %v", path, err)
	}
}

func readFile(t *testing.T, file string) []byte {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("testdata", filepath.FromSlash(file)+".json"))
	if err != nil {
		t.Fatalf("read fixture %s: %v", file, err)
	}
	return raw
}

// fixtureServer serves the Sheets API, the Drive API and the OAuth token
// endpoint from testdata:
//
//   - POST /token answers token.json for a JWT bearer assertion signed by
//     testKey, or for the test OAuth client and refresh token;
//     errors/invalid_grant_jwt, errors/invalid_client or errors/invalid_grant
//     otherwise;
//   - GET /v4/spreadsheets/<id> answers sheets/<id>/spreadsheet.json, and
//     GET .../values:batchGet the rows of its ranges (see batchGetBody); a
//     spreadsheet without fixtures gets errors/not_found;
//   - GET /drive/v3/files/<id> answers drive/<id>.json, or
//     errors/drive_not_found.
//
// overrides["METHOD path"] ("POST token" for the token endpoint) replaces
// the default with its fixtures in order, repeating the last. An API
// request without the test access token gets errors/unauthenticated. A
// request that carries a credential in its URL fails the test.
func fixtureServer(t *testing.T, overrides map[string][]fixture) (*httptest.Server, *hits) {
	t.Helper()
	h := &hits{}
	var mu sync.Mutex
	served := map[string]int{}
	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		for _, s := range secrets {
			if strings.Contains(r.URL.String(), s) {
				t.Errorf("request %s carries a credential in its URL", r.URL.Path)
			}
		}
		f := fixture{status: http.StatusOK}
		var body []byte
		key := r.Method + " " + r.URL.EscapedPath()
		switch {
		case r.URL.Path == "/token":
			if err := r.ParseForm(); err != nil {
				t.Errorf("parse form: %v", err)
			}
			key = "POST token"
			grant := r.PostForm.Get("grant_type")
			h.add(apiRequest{method: r.Method, path: "token", grant: grant})
			switch {
			case grant == jwtGrant && verifyAssertion(t, r.PostForm.Get("assertion"), srv.URL+"/token"):
				f.file = "token"
			case grant == jwtGrant:
				f = fixture{status: http.StatusBadRequest, file: "errors/invalid_grant_jwt"}
			case grant != "refresh_token":
				f = fixture{status: http.StatusBadRequest, file: "errors/unsupported_grant_type"}
			case r.PostForm.Get("client_id") != testClientID || r.PostForm.Get("client_secret") != testClientSecret:
				f = fixture{status: http.StatusUnauthorized, file: "errors/invalid_client"}
			case r.PostForm.Get("refresh_token") != testRefreshToken:
				f = fixture{status: http.StatusBadRequest, file: "errors/invalid_grant"}
			default:
				f.file = "token"
			}
		case r.Header.Get("Authorization") != "Bearer "+testAccessToken:
			h.add(apiRequest{method: r.Method, path: r.URL.EscapedPath(), query: r.URL.Query()})
			f = fixture{status: http.StatusUnauthorized, file: "errors/unauthenticated"}
		case strings.HasPrefix(r.URL.Path, "/v4/spreadsheets/"):
			h.add(apiRequest{method: r.Method, path: r.URL.EscapedPath(), query: r.URL.Query()})
			id, rest, _ := strings.Cut(strings.TrimPrefix(r.URL.Path, "/v4/spreadsheets/"), "/")
			_, statErr := os.Stat(filepath.Join("testdata", "sheets", id))
			switch {
			case statErr != nil:
				f = fixture{status: http.StatusNotFound, file: "errors/not_found"}
			case rest == "":
				if got := r.URL.Query().Get("fields"); got != "sheets.properties" {
					t.Errorf("spreadsheets.get fields %q, want sheets.properties", got)
				}
				f.file = "sheets/" + id + "/spreadsheet"
			case rest == "values:batchGet":
				f.status, body = batchGetBody(t, id, r.URL.Query())
			default:
				http.NotFound(w, r)
				return
			}
		case strings.HasPrefix(r.URL.Path, "/drive/v3/files/"):
			h.add(apiRequest{method: r.Method, path: r.URL.EscapedPath(), query: r.URL.Query()})
			id := strings.TrimPrefix(r.URL.Path, "/drive/v3/files/")
			if q := r.URL.Query(); q.Get("fields") != "modifiedTime" || q.Get("supportsAllDrives") != "true" {
				t.Errorf("drive files.get query %s", r.URL.RawQuery)
			}
			if _, err := os.Stat(filepath.Join("testdata", "drive", id+".json")); err != nil {
				f = fixture{status: http.StatusNotFound, file: "errors/drive_not_found"}
			} else {
				f.file = "drive/" + id
			}
		default:
			http.NotFound(w, r)
			return
		}
		mu.Lock()
		if seq, ok := overrides[key]; ok {
			o := seq[min(served[key], len(seq)-1)]
			served[key]++
			f.status, f.header = o.status, o.header
			switch {
			case o.body != nil:
				body = o.body
			case o.file != "":
				f.file, body = o.file, nil
			}
		}
		mu.Unlock()
		if f.status == 0 {
			f.status = http.StatusOK
		}
		if body == nil {
			body = readFile(t, f.file)
		}
		for k, v := range f.header {
			w.Header().Set(k, v)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(f.status)
		_, _ = w.Write(body)
	}))
	t.Cleanup(srv.Close)
	return srv, h
}

// verifyAssertion reports whether a JWT bearer assertion is signed by
// testKey for the test service account, audience tokenURL and the Sheets
// and Drive metadata read scopes.
func verifyAssertion(t *testing.T, assertion, tokenURL string) bool {
	t.Helper()
	parts := strings.Split(assertion, ".")
	if len(parts) != 3 {
		t.Errorf("assertion has %d parts", len(parts))
		return false
	}
	sig, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		t.Errorf("assertion signature: %v", err)
		return false
	}
	sum := sha256.Sum256([]byte(parts[0] + "." + parts[1]))
	if rsa.VerifyPKCS1v15(&testKey().PublicKey, crypto.SHA256, sum[:], sig) != nil {
		return false
	}
	raw, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		t.Errorf("assertion claims: %v", err)
		return false
	}
	var claims struct {
		Iss   string `json:"iss"`
		Aud   string `json:"aud"`
		Scope string `json:"scope"`
	}
	if err := json.Unmarshal(raw, &claims); err != nil {
		t.Errorf("assertion claims: %v", err)
		return false
	}
	if claims.Iss != testClientEmail || claims.Aud != tokenURL || claims.Scope != sheetsScope+" "+driveScope {
		t.Errorf("assertion claims %+v", claims)
		return false
	}
	return true
}

// testNow is the fixed clock of every test.
var testNow = time.Date(2026, 10, 19, 12, 0, 0, 0, time.UTC)

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

// testSource returns a Source that talks to srv with the test clock, adds
// one millisecond of jitter to every wait and records its retry waits
// instead of sleeping; pacing waits are dropped (see TestPacer).
func testSource(srv *httptest.Server) (Source, *waits) {
	w := &waits{}
	return Source{
		paceWait:   func(ctx context.Context, _ time.Duration) error { return ctx.Err() },
		sheetsURL:  srv.URL,
		driveURL:   srv.URL,
		tokenURL:   srv.URL + "/token",
		now:        func() time.Time { return testNow },
		client:     srv.Client(),
		retryDelay: time.Second,
		jitter:     func(d time.Duration) time.Duration { return d + time.Millisecond },
		wait:       w.wait,
	}, w
}

// testConfig reads spreadsheet A, given as its URL, with the test service
// account.
func testConfig(t *testing.T, edit func(*Config)) json.RawMessage {
	t.Helper()
	cfg := Config{
		SpreadsheetID: "https://docs.google.com/spreadsheets/d/" + testSheetA + "/edit#gid=0",
		Credentials:   CredentialsConfig{AuthType: authService, ServiceAccountInfo: serviceAccountKey(t, testKey())},
	}
	if edit != nil {
		edit(&cfg)
	}
	raw, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func withOAuth(c *Config) {
	c.Credentials = CredentialsConfig{AuthType: authClient, ClientID: testClientID, ClientSecret: testClientSecret, RefreshToken: testRefreshToken}
}

func withIncremental(c *Config) { c.Incremental = true }

func withStable(c *Config) { c.ColumnNaming = columnNamingStable }
