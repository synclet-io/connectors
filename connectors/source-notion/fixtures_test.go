package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// testToken is the only integration token the fixture server accepts.
const testToken = "ntn_test0000000000000000000000000000000000000000"

const (
	dsA     = "da000000-0000-4000-8000-00000000000a"
	dsB     = "da000000-0000-4000-8000-00000000000b"
	dbA     = "db000000-0000-4000-8000-00000000000a"
	pageP1  = "a1000000-0000-4000-8000-000000000001"
	pageP2  = "a1000000-0000-4000-8000-000000000002"
	pagePU  = "a1000000-0000-4000-8000-000000000003"
	pageOld = "a1000000-0000-4000-8000-000000000004"
	rowA1   = "a1000000-0000-4000-8000-000000000005"
	rowA2   = "a1000000-0000-4000-8000-000000000006"
	rowB1   = "a1000000-0000-4000-8000-000000000007"
	blockB1 = "b1000000-0000-4000-8000-000000000001"
	blockB3 = "b1000000-0000-4000-8000-000000000003"
	blockB4 = "b1000000-0000-4000-8000-000000000004"
	block11 = "b1000000-0000-4000-8000-000000000011"
	blockBA = "b1000000-0000-4000-8000-000000000111"
)

// fixture is a response the fixture server sends instead of the testdata
// file of a request: status with the body of testdata/<file>.
type fixture struct {
	status int
	file   string
}

// hit is one request the fixture server received: its method, the path
// under /v1/, the start_cursor of a GET and the decoded body of a POST.
type hit struct {
	method string
	path   string
	cursor string
	body   map[string]any
}

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

// to returns the requests for path.
func (h *hits) to(path string) []hit {
	var out []hit
	for _, r := range h.list() {
		if r.path == path {
			out = append(out, r)
		}
	}
	return out
}

// isPost reports whether API path p takes POST; every other path takes GET.
func isPost(p string) bool {
	return p == "search" || (strings.HasPrefix(p, "data_sources/") && strings.HasSuffix(p, "/query"))
}

// fixtureKey maps a request to its testdata file without ".json": GET
// /v1/<p> is answered by <p>, with start_cursor=X by <p>-X; POST /v1/search
// by search/<filter value> or search/all without a filter, and with
// start_cursor X in the body by search/<filter>-X; other POST paths like
// GET.
func fixtureKey(p, cursor string, body map[string]any) string {
	key := p
	if p == "search" {
		value := "all"
		if f, ok := body["filter"].(map[string]any); ok {
			value, _ = f["value"].(string)
		}
		key = "search/" + value
	}
	if cursor != "" {
		key += "-" + cursor
	}
	return key
}

// fixtureServer serves the Notion API from testdata, or a request's
// override keyed by its fixtureKey. A request without the test token in
// its Authorization header gets Notion's 401. The test fails on a request
// without the pinned Notion-Version, with the wrong method, with the token
// in its URL, or with a query string on a POST.
func fixtureServer(t *testing.T, overrides map[string]fixture) (*httptest.Server, *hits) {
	t.Helper()
	h := &hits{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.RawQuery, testToken) {
			t.Errorf("request %s carries the token in its URL", r.URL.Path)
		}
		if v := r.Header.Get("Notion-Version"); v != notionVersion {
			t.Errorf("request %s has Notion-Version %q", r.URL.Path, v)
		}
		p := strings.TrimPrefix(r.URL.Path, "/v1/")
		want := http.MethodGet
		if isPost(p) {
			want = http.MethodPost
		}
		if r.Method != want {
			t.Errorf("%s %s, want %s", r.Method, p, want)
		}
		rec := hit{method: r.Method, path: p}
		if r.Method == http.MethodPost {
			if r.URL.RawQuery != "" {
				t.Errorf("POST %s carries a query string %q", p, r.URL.RawQuery)
			}
			raw, err := io.ReadAll(r.Body)
			if err != nil || json.Unmarshal(raw, &rec.body) != nil {
				t.Errorf("POST %s: body %q is not a JSON object", p, raw)
			}
			rec.cursor, _ = rec.body["start_cursor"].(string)
		} else {
			rec.cursor = r.URL.Query().Get("start_cursor")
		}
		h.add(rec)
		key := fixtureKey(p, rec.cursor, rec.body)
		f, ok := overrides[key]
		if !ok {
			f = fixture{status: http.StatusOK, file: key + ".json"}
		}
		if r.Header.Get("Authorization") != "Bearer "+testToken {
			f = fixture{status: http.StatusUnauthorized, file: "errors/unauthorized.json"}
		}
		body, err := os.ReadFile(filepath.Join("testdata", filepath.FromSlash(f.file)))
		if err != nil {
			t.Errorf("no fixture %s for %s %s", f.file, r.Method, p)
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

// testNow is the fixed clock of every test.
var testNow = time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC)

// testSource returns a Source that talks to srv with the test clock and
// waits that return at once.
func testSource(srv *httptest.Server) Source {
	return Source{
		baseURL:    srv.URL + "/v1/",
		now:        func() time.Time { return testNow },
		client:     srv.Client(),
		retryDelay: time.Millisecond,
		wait:       func(ctx context.Context, _ time.Duration) error { return ctx.Err() },
	}
}
