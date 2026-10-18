package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"
)

const (
	// testToken is the only access token the fixture server accepts; the
	// client credentials grant of testClientID and testClientSecret
	// returns it.
	testToken        = "test-shopify-admin-token"
	testClientID     = "test-client-id"
	testClientSecret = "test-client-secret"
	// testShop is the only shop the fixture server knows.
	testShop    = "example"
	graphqlPath = "/admin/api/" + apiVersion + "/graphql.json"
)

// hit is one request the fixture server received.
type hit struct {
	method string
	path   string
	// key is the fixture key of a GraphQL request (see graphqlKey).
	key   string
	op    string
	doc   string
	vars  map[string]any
	token string
}

func (h hit) String() string {
	if h.key != "" {
		return h.method + " " + h.path + " " + h.key
	}
	return h.method + " " + h.path
}

// hits records the requests the fixture server was asked for, in order.
type hits struct {
	mu   sync.Mutex
	reqs []hit
}

func (h *hits) add(r hit) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.reqs = append(h.reqs, r)
}

func (h *hits) list() []hit {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]hit(nil), h.reqs...)
}

// keys returns the fixture key of every GraphQL request.
func (h *hits) keys() []string {
	var out []string
	for _, r := range h.list() {
		if r.key != "" {
			out = append(out, r.key)
		}
	}
	return out
}

// ops returns the hits of GraphQL operation op.
func (h *hits) ops(op string) []hit {
	var out []hit
	for _, r := range h.list() {
		if r.op == op {
			out = append(out, r)
		}
	}
	return out
}

type gqlBody struct {
	Query         string         `json:"query"`
	Variables     map[string]any `json:"variables"`
	OperationName string         `json:"operationName"`
}

var (
	windowPattern = regexp.MustCompile(`updated_at:>='([^']+)'(?: AND updated_at:<'([^']+)')?`)
	keyLayout     = "20060102T150405Z"
)

// bulkStream returns the stream whose bulk query is q, with its window
// start in keyLayout when the query has one.
func bulkStream(t *testing.T, q string) string {
	var w *window
	if m := windowPattern.FindStringSubmatch(q); m != nil {
		w = &window{}
		var err error
		if w.from, err = time.Parse(filterLayout, m[1]); err != nil {
			t.Errorf("bulk query window %q", m[0])
			return ""
		}
		if m[2] != "" {
			if w.to, err = time.Parse(filterLayout, m[2]); err != nil {
				t.Errorf("bulk query window %q", m[0])
				return ""
			}
		}
	}
	for _, st := range streams {
		if st.bulk && st.bulkQuery(w) == q {
			if w == nil {
				return st.name
			}
			return st.name + "-" + w.from.Format(keyLayout)
		}
	}
	t.Errorf("bulk query of no stream: %s", q)
	return ""
}

// graphqlKey maps a GraphQL request to its fixture key: BulkRun-<stream>
// with -<window start> for a windowed one, BulkPoll-<id> and
// BulkCancel-<id> by the last segment of the operation's GID, <op>-<after>
// for a later page and <op> otherwise. testdata/graphql/<key>.json answers
// it.
func graphqlKey(t *testing.T, b gqlBody) string {
	switch b.OperationName {
	case "BulkRun":
		q, _ := b.Variables["query"].(string)
		return "BulkRun-" + bulkStream(t, q)
	case "BulkPoll", "BulkCancel":
		id, _ := b.Variables["id"].(string)
		return b.OperationName + "-" + id[strings.LastIndex(id, "/")+1:]
	}
	if after, ok := b.Variables["after"].(string); ok && after != "" {
		return b.OperationName + "-" + after
	}
	return b.OperationName
}

// respond answers with status and the body of testdata/<file>, with
// {{base_url}} replaced by the server's URL.
func respond(t *testing.T, status int, file string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		body, err := os.ReadFile(filepath.Join("testdata", filepath.FromSlash(file)))
		if err != nil {
			t.Errorf("no fixture %s for %s", file, r.URL)
			http.NotFound(w, r)
			return
		}
		body = bytes.ReplaceAll(body, []byte("{{base_url}}"), []byte("http://"+r.Host))
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write(body)
	}
}

// fixtureServer serves the Admin API of the shop testShop under
// /example, the client credentials grant and bulk outputs from testdata,
// or from the handler overrides holds for a request's key: the GraphQL
// fixture key, "oauth" for the grant or "bulk/<file>" for an output. Any
// other shop answers 404. A GraphQL request without testToken in
// X-Shopify-Access-Token gets 401. A request carrying a credential in its
// URL, or an output download carrying the token, fails the test.
func fixtureServer(t *testing.T, overrides map[string]http.HandlerFunc) (*httptest.Server, *hits) {
	t.Helper()
	h := &hits{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		for _, secret := range []string{testToken, testClientSecret} {
			if strings.Contains(r.URL.String(), secret) {
				t.Errorf("request %s carries a credential in its URL", r.URL.Path)
			}
		}
		raw, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read body of %s: %v", r.URL, err)
		}
		r.Body = io.NopCloser(bytes.NewReader(raw))
		token := r.Header.Get(tokenHeader)
		serve := func(key, file string) {
			if o, ok := overrides[key]; ok {
				o(w, r)
				return
			}
			respond(t, http.StatusOK, file)(w, r)
		}

		if p, ok := strings.CutPrefix(r.URL.Path, "/bulk/"); ok {
			h.add(hit{method: r.Method, path: r.URL.Path, token: token})
			if token != "" || r.Header.Get("Authorization") != "" {
				t.Errorf("bulk output download %s carries a Shopify token", r.URL.Path)
			}
			serve("bulk/"+p, "bulk/"+p)
			return
		}
		shop, rest, _ := strings.Cut(strings.TrimPrefix(r.URL.Path, "/"), "/")
		rest = "/" + rest
		if shop != testShop {
			h.add(hit{method: r.Method, path: r.URL.Path, token: token})
			respond(t, http.StatusNotFound, "errors/not_found.json")(w, r)
			return
		}
		switch rest {
		case "/admin/oauth/access_token":
			h.add(hit{method: r.Method, path: rest, token: token})
			if err := r.ParseForm(); err != nil {
				t.Errorf("grant form: %v", err)
			}
			if r.PostForm.Get("grant_type") != "client_credentials" || r.PostForm.Get("client_id") != testClientID || r.PostForm.Get("client_secret") != testClientSecret {
				if o, ok := overrides["oauth"]; ok {
					o(w, r)
					return
				}
				respond(t, http.StatusBadRequest, "errors/invalid_client.json")(w, r)
				return
			}
			serve("oauth", "oauth/access_token.json")
		case graphqlPath:
			var b gqlBody
			if err := json.Unmarshal(raw, &b); err != nil {
				t.Errorf("GraphQL body: %v", err)
			}
			key := graphqlKey(t, b)
			h.add(hit{method: r.Method, path: rest, key: key, op: b.OperationName, doc: b.Query, vars: b.Variables, token: token})
			if token != testToken {
				respond(t, http.StatusUnauthorized, "errors/invalid_token.json")(w, r)
				return
			}
			serve(key, "graphql/"+key+".json")
		default:
			h.add(hit{method: r.Method, path: rest, token: token})
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
			respond(t, http.StatusNotFound, "errors/not_found.json")(w, r)
		}
	}))
	t.Cleanup(srv.Close)
	return srv, h
}

// testNow is the fixed clock of every test.
var testNow = time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC)

// testSource returns a Source that talks to srv with the test clock and
// waits that return at once.
func testSource(srv *httptest.Server) Source {
	return Source{
		baseURL:      func(shop string) string { return srv.URL + "/" + shop },
		now:          func() time.Time { return testNow },
		client:       srv.Client(),
		retryDelay:   time.Millisecond,
		wait:         func(ctx context.Context, _ time.Duration) error { return ctx.Err() },
		pollInterval: time.Millisecond,
	}
}

// waits records the durations a Source waited for.
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

// jsonBody answers with status and body.
func jsonBody(status int, body string) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = io.WriteString(w, body)
	}
}

// sequence answers the n-th request with handlers[n], and the last
// handler from then on.
func sequence(handlers ...http.HandlerFunc) http.HandlerFunc {
	var mu sync.Mutex
	n := 0
	return func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		hf := handlers[min(n, len(handlers)-1)]
		n++
		mu.Unlock()
		hf(w, r)
	}
}
