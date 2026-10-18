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
	testSiteA        = "https://www.example.com/"
	testSiteB        = "sc-domain:example.org"
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
// status with the body of testdata/<file>.json, or body itself, sent as it
// is. An empty file and body keep the default body.
type fixture struct {
	status int
	file   string
	body   []byte
}

// apiRequest is one request the fixture server received.
type apiRequest struct {
	method string
	path   string
	site   string
	grant  string
	query  analyticsRequest
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

// keys returns "METHOD path" of every request, the path as it was escaped.
func (h *hits) keys() []string {
	var out []string
	for _, r := range h.list() {
		out = append(out, r.method+" "+r.path)
	}
	return out
}

// queries returns the searchAnalytics.query requests of site.
func (h *hits) queries(site string) []analyticsRequest {
	var out []analyticsRequest
	for _, r := range h.list() {
		if r.site == site && strings.HasSuffix(r.path, "/searchAnalytics/query") {
			out = append(out, r.query)
		}
	}
	return out
}

// queryKey is the override key of the searchAnalytics.query of site.
func queryKey(site string) string { return "POST " + sitePath(site) + "/searchAnalytics/query" }

// siteDir is the testdata directory of a site: https://www.example.com/ is
// sites/www.example.com, sc-domain:example.org is sites/sc-domain_example.org.
func siteDir(site string) string {
	k := strings.TrimSuffix(strings.TrimPrefix(site, "https://"), "/")
	return "sites/" + strings.NewReplacer(":", "_", "/", "_").Replace(k)
}

// analyticsBody answers a searchAnalytics.query of site from
// <dir>/analytics/<type>/<dimensions joined by _>/<day>_<startRow>.json as
// it is when that file exists, else from the rows of <day>.json sliced by
// startRow and rowLimit; a day without a file has no rows.
func analyticsBody(t *testing.T, dir string, q analyticsRequest) []byte {
	t.Helper()
	if q.StartDate != q.EndDate {
		t.Errorf("query from %s to %s, want one day", q.StartDate, q.EndDate)
	}
	base := filepath.Join("testdata", filepath.FromSlash(dir), "analytics", q.Type, strings.Join(q.Dimensions, "_"), q.StartDate)
	if raw, err := os.ReadFile(base + "_" + strconv.Itoa(q.StartRow) + ".json"); err == nil {
		return raw
	}
	var day struct {
		Rows        []json.RawMessage `json:"rows"`
		Aggregation string            `json:"responseAggregationType"`
	}
	if raw, err := os.ReadFile(base + ".json"); err == nil {
		if err := json.Unmarshal(raw, &day); err != nil {
			t.Fatalf("%s: %v", base, err)
		}
	}
	resp := map[string]any{"responseAggregationType": "byProperty"}
	if day.Aggregation != "" {
		resp["responseAggregationType"] = day.Aggregation
	}
	if lo := min(q.StartRow, len(day.Rows)); lo < len(day.Rows) {
		resp["rows"] = day.Rows[lo:min(lo+q.RowLimit, len(day.Rows))]
	}
	raw, err := json.Marshal(resp)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

// fixtureServer serves the Search Console API and the OAuth token endpoint
// from testdata:
//
//   - POST /token answers testdata/token.json for a JWT bearer assertion
//     signed by testKey, or for the test OAuth client and refresh token;
//     errors/invalid_grant_jwt, errors/invalid_client or errors/invalid_grant
//     otherwise;
//   - GET /webmasters/v3/sites/<site> answers <dir>/site.json and
//     GET .../sites/<site>/sitemaps <dir>/sitemaps.json, with dir the
//     siteDir of the site; a site without fixtures gets errors/forbidden_site;
//   - POST .../sites/<site>/searchAnalytics/query answers from the analytics
//     fixtures of the site (see analyticsBody).
//
// <site> must be escaped as one path segment. overrides["METHOD path"] (the
// escaped path without /webmasters/v3/, "POST token" for the token
// endpoint) replaces the default with its fixtures in order, repeating the
// last. An API request without the test access token gets
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
			p, ok := strings.CutPrefix(r.URL.EscapedPath(), "/"+apiVersion+"/")
			if !ok {
				http.NotFound(w, r)
				return
			}
			key = r.Method + " " + p
			escaped, rest, _ := strings.Cut(strings.TrimPrefix(p, "sites/"), "/")
			site, err := url.PathUnescape(escaped)
			if err != nil || !strings.HasPrefix(p, "sites/") || sitePath(site) != "sites/"+escaped {
				t.Errorf("request path %s does not escape its site as one segment", p)
			}
			req := apiRequest{method: r.Method, path: p, site: site}
			if r.Method == http.MethodPost {
				if err := json.NewDecoder(r.Body).Decode(&req.query); err != nil {
					t.Errorf("decode body of %s: %v", p, err)
				}
			}
			h.add(req)
			dir := siteDir(site)
			_, statErr := os.Stat(filepath.Join("testdata", filepath.FromSlash(dir)))
			switch {
			case r.Header.Get("Authorization") != "Bearer "+testAccessToken:
				f = fixture{status: http.StatusUnauthorized, file: "errors/unauthenticated"}
			case statErr != nil:
				f = fixture{status: http.StatusForbidden, file: "errors/forbidden_site"}
			case r.Method == http.MethodGet && rest == "":
				f.file = dir + "/site"
			case r.Method == http.MethodGet && rest == "sitemaps":
				f.file = dir + "/sitemaps"
			case r.Method == http.MethodPost && rest == "searchAnalytics/query":
				body = analyticsBody(t, dir, req.query)
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

// verifyAssertion reports whether a JWT bearer assertion is signed by
// testKey for the test service account, audience tokenURL and the
// webmasters.readonly scope.
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
// one millisecond of jitter to every wait and records its retry waits
// instead of sleeping; pacing waits are dropped (see TestPacer).
func testSource(srv *httptest.Server) (Source, *waits) {
	w := &waits{}
	return Source{
		paceWait:   func(ctx context.Context, _ time.Duration) error { return ctx.Err() },
		baseURL:    srv.URL,
		tokenURL:   srv.URL + "/token",
		now:        func() time.Time { return testNow },
		client:     srv.Client(),
		retryDelay: time.Second,
		jitter:     func(d time.Duration) time.Duration { return d + time.Millisecond },
		wait:       w.wait,
	}, w
}

// testConfig reads both synthetic sites from 2026-10-14 with the test
// service account.
func testConfig(t *testing.T, edit func(*Config)) json.RawMessage {
	t.Helper()
	cfg := Config{
		Credentials: Credentials{AuthType: authServiceAccount, ServiceAccountKey: serviceAccountKey(t, testKey())},
		SiteURLs:    []string{testSiteA, testSiteB},
		StartDate:   "2026-10-14",
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

// discoverPages is a custom report of the discover search type by page,
// with date left for the connector to add.
var discoverPages = CustomReport{Name: "discover_pages", Dimensions: []string{"page"}, SearchType: "discover"}

func withDiscoverPages(c *Config) { c.CustomReports = append(c.CustomReports, discoverPages) }
