package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// Synthetic identifiers and credentials of the fixtures; none belongs to a
// real account.
const (
	testDeveloperToken = "test-developer-token"
	testClientID       = "test-client-id"
	testClientSecret   = "test-client-secret"
	testRefreshToken   = "test-refresh-token"
	testAccessToken    = "test-access-token"
	testManager        = "1111111111"
	testClientA        = "1234567890"
	testClientB        = "2345678901"
)

// secrets are the credentials no request URL may carry.
var secrets = []string{testDeveloperToken, testClientSecret, testRefreshToken, testAccessToken}

// fixture is a response the fixture server sends instead of its default:
// status with the body of testdata/<file>.json. An empty file keeps the
// default body.
type fixture struct {
	status int
	file   string
}

// apiRequest is one request the fixture server received.
type apiRequest struct {
	method    string
	path      string
	query     string
	pageToken string
	login     string
	devToken  string
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

// searches returns the googleAds:search requests whose query reads resource.
func (h *hits) searches(resource string) []apiRequest {
	var out []apiRequest
	for _, r := range h.list() {
		if !strings.HasSuffix(r.path, "/googleAds:search") {
			continue
		}
		if q, err := parseQuery(r.query); err == nil && q.resource == resource {
			out = append(out, r)
		}
	}
	return out
}

// searchFile returns the fixture of a googleAds:search request:
// customers/<cid>/<resource>, with _daily when the query selects
// segments.date and _<pageToken> for a later page.
func searchFile(cid, query, pageToken string) string {
	file := "customers/" + cid + "/"
	if q, err := parseQuery(query); err == nil {
		file += q.resource
		if q.selects(dateField) {
			file += "_daily"
		}
	}
	if pageToken != "" {
		file += "_" + pageToken
	}
	return file
}

// fixtureServer serves the Google Ads REST API and the OAuth token endpoint
// from testdata:
//
//   - POST /token answers an access token for the test client and refresh
//     token, errors/invalid_client or errors/invalid_grant otherwise;
//   - POST /<apiVersion>/customers/<cid>/googleAds:search answers
//     searchFile(cid, query, pageToken);
//   - POST /<apiVersion>/googleAdsFields:search answers testdata/fields.json.
//
// overrides["METHOD path"] (path without /<apiVersion>/) replaces the
// default with its fixtures in order, repeating the last. An API request
// without the test access token gets errors/unauthenticated, one with
// another developer token errors/developer_token_not_approved. A request
// that carries a credential in its URL fails the test.
func fixtureServer(t *testing.T, overrides map[string][]fixture) (*httptest.Server, *hits) {
	t.Helper()
	h := &hits{}
	var mu sync.Mutex
	served := map[string]int{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		for _, s := range secrets {
			if strings.Contains(r.URL.String(), s) {
				t.Errorf("request %s carries a credential in its URL", r.URL.Path)
			}
		}
		var f fixture
		var key string
		if r.URL.Path == "/token" {
			if err := r.ParseForm(); err != nil {
				t.Errorf("parse form: %v", err)
			}
			key = "POST token"
			h.add(apiRequest{method: r.Method, path: "token"})
			switch {
			case r.PostForm.Get("grant_type") != "refresh_token":
				f = fixture{status: http.StatusBadRequest, file: "errors/unsupported_grant_type"}
			case r.PostForm.Get("client_id") != testClientID || r.PostForm.Get("client_secret") != testClientSecret:
				f = fixture{status: http.StatusUnauthorized, file: "errors/invalid_client"}
			case r.PostForm.Get("refresh_token") != testRefreshToken:
				f = fixture{status: http.StatusBadRequest, file: "errors/invalid_grant"}
			default:
				f = fixture{file: "token"}
			}
		} else {
			p, ok := strings.CutPrefix(r.URL.Path, "/"+apiVersion+"/")
			if !ok || r.Method != http.MethodPost {
				http.NotFound(w, r)
				return
			}
			var body struct {
				Query     string `json:"query"`
				PageToken string `json:"pageToken"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Errorf("decode body of %s: %v", p, err)
			}
			key = "POST " + p
			h.add(apiRequest{
				method: r.Method, path: p, query: body.Query, pageToken: body.PageToken,
				login: r.Header.Get("login-customer-id"), devToken: r.Header.Get("developer-token"),
			})
			switch {
			case r.Header.Get("Authorization") != "Bearer "+testAccessToken:
				f = fixture{status: http.StatusUnauthorized, file: "errors/unauthenticated"}
			case r.Header.Get("developer-token") != testDeveloperToken:
				f = fixture{status: http.StatusForbidden, file: "errors/developer_token_not_approved"}
			case p == "googleAdsFields:search":
				f = fixture{file: "fields"}
			default:
				cid, ok := strings.CutSuffix(strings.TrimPrefix(p, "customers/"), "/googleAds:search")
				if !ok {
					http.NotFound(w, r)
					return
				}
				f = fixture{file: searchFile(cid, body.Query, body.PageToken)}
			}
		}
		mu.Lock()
		if seq, ok := overrides[key]; ok {
			o := seq[min(served[key], len(seq)-1)]
			served[key]++
			f.status = o.status
			if o.file != "" {
				f.file = o.file
			}
		}
		mu.Unlock()
		if f.status == 0 {
			f.status = http.StatusOK
		}
		body, err := os.ReadFile(filepath.Join("testdata", filepath.FromSlash(f.file)+".json"))
		if err != nil {
			t.Errorf("no fixture for %s %s: %v", r.Method, r.URL.Path, err)
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(f.status)
		_, _ = w.Write(body)
	}))
	t.Cleanup(srv.Close)
	return srv, h
}

// testNow is the fixed clock of every test: today is 2026-10-18.
var testNow = time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC)

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

// testConfig reads the synthetic manager account with both its clients.
func testConfig(t *testing.T, edit func(*Config)) json.RawMessage {
	t.Helper()
	cfg := Config{
		DeveloperToken: testDeveloperToken,
		ClientID:       testClientID,
		ClientSecret:   testClientSecret,
		RefreshToken:   testRefreshToken,
		CustomerIDs:    []string{"111-111-1111"},
		StartDate:      "2026-10-01",
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

// keywordsQuery is a custom daily query whose keyword fields are not in
// knownFields, so their types come from testdata/fields.json.
const keywordsQuery = "SELECT customer.id, ad_group.id, ad_group_criterion.keyword.text, ad_group_criterion.keyword.match_type, metrics.clicks, metrics.cost_micros, segments.date FROM keyword_view WHERE ad_group_criterion.status = 'ENABLED' ORDER BY metrics.clicks DESC"

func withKeywords(c *Config) {
	c.CustomQueries = []CustomQuery{{Name: "my_keywords", Query: keywordsQuery}}
}
