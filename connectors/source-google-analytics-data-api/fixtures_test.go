package main

import (
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
	"os"
	"path/filepath"
	"slices"
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
	testPropA        = "123456789"
	testPropB        = "987654321"
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
// status with the body of testdata/<file>.json, sent as it is. An empty file
// keeps the default body.
type fixture struct {
	status int
	file   string
}

// apiRequest is one request the fixture server received.
type apiRequest struct {
	method string
	path   string
	grant  string
	report reportRequest
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

// reports returns the runReport requests of prop whose metrics are metrics.
func (h *hits) reports(prop string, metrics ...string) []reportRequest {
	var out []reportRequest
	for _, r := range h.list() {
		if r.path != "properties/"+prop+":runReport" {
			continue
		}
		var ms []string
		for _, m := range r.report.Metrics {
			ms = append(ms, m.Name)
		}
		if slices.Equal(ms, metrics) {
			out = append(out, r.report)
		}
	}
	return out
}

// reportFixture is a hand-written report: its headers, every row of every
// day, and the metadata of each response.
type reportFixture struct {
	DimensionHeaders []nameObj        `json:"dimensionHeaders"`
	MetricHeaders    []map[string]any `json:"metricHeaders"`
	Rows             []struct {
		DimensionValues []value `json:"dimensionValues"`
		MetricValues    []value `json:"metricValues"`
	} `json:"rows"`
	Metadata map[string]any `json:"metadata,omitempty"`
}

// findReport returns the fixture of testdata/properties/<prop> whose headers
// are the requested dimensions and metrics.
func findReport(prop string, req reportRequest) (reportFixture, bool) {
	files, _ := filepath.Glob(filepath.Join("testdata", "properties", prop, "*.json"))
	for _, f := range files {
		if filepath.Base(f) == "metadata.json" {
			continue
		}
		raw, err := os.ReadFile(f)
		if err != nil {
			continue
		}
		var rf reportFixture
		if json.Unmarshal(raw, &rf) != nil {
			continue
		}
		if !slices.Equal(rf.DimensionHeaders, req.Dimensions) || len(rf.MetricHeaders) != len(req.Metrics) {
			continue
		}
		match := true
		for i, m := range rf.MetricHeaders {
			if m["name"] != req.Metrics[i].Name {
				match = false
			}
		}
		if match {
			return rf, true
		}
	}
	return reportFixture{}, false
}

var defaultQuota = map[string]any{
	"tokensPerDay":       map[string]int{"consumed": 12, "remaining": 24100},
	"tokensPerHour":      map[string]int{"consumed": 12, "remaining": 4900},
	"concurrentRequests": map[string]int{"consumed": 0, "remaining": 10},
}

// runReportResponse answers req from rf: the rows whose date is the
// requested day, rowCount of them, sliced by offset and limit.
func runReportResponse(t *testing.T, rf reportFixture, req reportRequest) []byte {
	t.Helper()
	if len(req.DateRanges) != 1 || req.DateRanges[0].StartDate != req.DateRanges[0].EndDate {
		t.Errorf("runReport date ranges %+v, want one day", req.DateRanges)
	}
	if !req.ReturnPropertyQuota {
		t.Error("runReport without returnPropertyQuota")
	}
	day := strings.ReplaceAll(req.DateRanges[0].StartDate, "-", "")
	var rows []any
	for _, r := range rf.Rows {
		if r.DimensionValues[0].Value == day {
			rows = append(rows, r)
		}
	}
	resp := map[string]any{
		"dimensionHeaders": rf.DimensionHeaders,
		"metricHeaders":    rf.MetricHeaders,
		"propertyQuota":    defaultQuota,
		"kind":             "analyticsData#runReport",
	}
	if len(rows) > 0 {
		resp["rowCount"] = len(rows)
	}
	if rf.Metadata != nil {
		resp["metadata"] = rf.Metadata
	}
	if lo := min(req.Offset, len(rows)); lo < len(rows) {
		resp["rows"] = rows[lo:min(lo+req.Limit, len(rows))]
	}
	raw, err := json.Marshal(resp)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

// verifyAssertion reports whether a JWT bearer assertion is signed by
// testKey for the test service account, audience tokenURL and the
// analytics.readonly scope.
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
	if claims.Iss != testClientEmail || claims.Aud != tokenURL || claims.Scope != scope {
		t.Errorf("assertion claims %+v", claims)
		return false
	}
	return true
}

// fixtureServer serves the Google Analytics Data API and the OAuth token
// endpoint from testdata:
//
//   - POST /token answers testdata/token.json for a JWT bearer assertion
//     signed by testKey, or for the test OAuth client and refresh token;
//     errors/invalid_grant_jwt, errors/invalid_client or errors/invalid_grant
//     otherwise;
//   - GET /v1beta/properties/<id>/metadata answers
//     properties/<id>/metadata.json, errors/permission_denied for a property
//     without fixtures;
//   - POST /v1beta/properties/<id>:runReport answers from the fixture of
//     properties/<id> with the requested headers (see runReportResponse).
//
// overrides["METHOD path"] (path without /v1beta/, "POST token" for the
// token endpoint) replaces the default with its fixtures in order,
// repeating the last. An API request without the test access token gets
// errors/unauthenticated. A request that carries a credential in its URL
// fails the test.
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
		var f fixture
		var body []byte
		var key string
		if r.URL.Path == "/token" {
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
		} else {
			p, ok := strings.CutPrefix(r.URL.Path, "/"+apiVersion+"/")
			if !ok {
				http.NotFound(w, r)
				return
			}
			key = r.Method + " " + p
			req := apiRequest{method: r.Method, path: p}
			if r.Method == http.MethodPost {
				if err := json.NewDecoder(r.Body).Decode(&req.report); err != nil {
					t.Errorf("decode body of %s: %v", p, err)
				}
			}
			h.add(req)
			prop, _, _ := strings.Cut(strings.TrimPrefix(p, "properties/"), "/")
			prop, _, _ = strings.Cut(prop, ":")
			_, statErr := os.Stat(filepath.Join("testdata", "properties", prop))
			switch {
			case r.Header.Get("Authorization") != "Bearer "+testAccessToken:
				f = fixture{status: http.StatusUnauthorized, file: "errors/unauthenticated"}
			case statErr != nil:
				f = fixture{status: http.StatusForbidden, file: "errors/permission_denied"}
			case r.Method == http.MethodGet && strings.HasSuffix(p, "/metadata"):
				f.file = "properties/" + prop + "/metadata"
			case r.Method == http.MethodPost && strings.HasSuffix(p, ":runReport"):
				rf, ok := findReport(prop, req.report)
				if !ok {
					t.Errorf("no report fixture of property %s for %+v", prop, req.report)
					http.NotFound(w, r)
					return
				}
				body = runReportResponse(t, rf, req.report)
			default:
				http.NotFound(w, r)
				return
			}
		}
		mu.Lock()
		if seq, ok := overrides[key]; ok {
			o := seq[min(served[key], len(seq)-1)]
			served[key]++
			f.status = o.status
			if o.file != "" {
				f.file, body = o.file, nil
			}
		}
		mu.Unlock()
		if f.status == 0 {
			f.status = http.StatusOK
		}
		if body == nil {
			var err error
			body, err = os.ReadFile(filepath.Join("testdata", filepath.FromSlash(f.file)+".json"))
			if err != nil {
				t.Errorf("no fixture for %s %s: %v", r.Method, r.URL.Path, err)
				http.NotFound(w, r)
				return
			}
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(f.status)
		_, _ = w.Write(body)
	}))
	t.Cleanup(srv.Close)
	return srv, h
}

// testNow is the fixed clock of every test: today is 2026-10-16, half past
// noon.
var testNow = time.Date(2026, 10, 16, 12, 30, 0, 0, time.UTC)

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
// one millisecond of jitter to every wait and records its waits instead of
// sleeping.
func testSource(srv *httptest.Server) (Source, *waits) {
	w := &waits{}
	return Source{
		baseURL:    srv.URL,
		tokenURL:   srv.URL + "/token",
		now:        func() time.Time { return testNow },
		client:     srv.Client(),
		retryDelay: time.Second,
		jitter:     func(d time.Duration) time.Duration { return d + time.Millisecond },
		wait:       w.wait,
	}, w
}

// testConfig reads both synthetic properties from 2026-10-05 with the test
// service account.
func testConfig(t *testing.T, edit func(*Config)) json.RawMessage {
	t.Helper()
	cfg := Config{
		Credentials: Credentials{AuthType: authServiceAccount, ServiceAccountKey: serviceAccountKey(t, testKey())},
		PropertyIDs: []string{testPropA, "properties/" + testPropB},
		StartDate:   "2026-10-05",
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
	c.Credentials = Credentials{AuthType: authOAuth, ClientID: testClientID, ClientSecret: testClientSecret, RefreshToken: testRefreshToken}
}

// myReport is a custom report over a custom dimension and a currency
// metric, with date left for the connector to add.
var myReport = CustomReport{Name: "my_report", Dimensions: []string{"customEvent:plan_tier", "country"}, Metrics: []string{"totalRevenue", "sessions"}}

func withMyReport(c *Config) { c.CustomReports = append(c.CustomReports, myReport) }
