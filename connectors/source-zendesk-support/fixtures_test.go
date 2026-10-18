package main

import (
	"context"
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// testEmail and testToken are the only credentials the fixture server
// accepts.
const (
	testEmail = "tester@example.com"
	testToken = "test-api-token"
)

var testAuth = "Basic " + base64.StdEncoding.EncodeToString([]byte(testEmail+"/token:"+testToken))

// fixture is a response the fixture server sends for one API path instead of
// its testdata file: status with the body of testdata/<file>.
type fixture struct {
	status int
	file   string
}

// hits records the requests the fixture server was asked for, in order, as
// the path under /api/v2/ followed by the sorted query, if any.
type hits struct {
	mu   sync.Mutex
	reqs []string
}

func (h *hits) add(p string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.reqs = append(h.reqs, p)
}

func (h *hits) list() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]string(nil), h.reqs...)
}

// fixtureFile maps a request to its testdata file: GET /api/v2/<p>.json is
// answered by testdata/<p>.json; with cursor=X by testdata/<p>/cursor-X.json;
// with page[after]=X by testdata/<p>/after-X.json; ticket_events with
// start_time=T by testdata/incremental/ticket_events/T.json.
func fixtureFile(r *http.Request) string {
	p := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/api/v2/"), ".json")
	q := r.URL.Query()
	switch {
	case q.Has("cursor"):
		return p + "/cursor-" + q.Get("cursor") + ".json"
	case q.Has("page[after]"):
		return p + "/after-" + q.Get("page[after]") + ".json"
	case p == "incremental/ticket_events":
		return p + "/" + q.Get("start_time") + ".json"
	}
	return p + ".json"
}

// fixtureServer serves the Zendesk Support API from testdata, or a path's
// override (keyed by the path under /api/v2/). A request without the test
// credentials in its Authorization header gets 401; one that carries a
// token in its URL fails the test.
func fixtureServer(t *testing.T, overrides map[string]fixture) (*httptest.Server, *hits) {
	t.Helper()
	h := &hits{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		if q.Has("token") || q.Has("api_token") || strings.Contains(r.URL.RawQuery, testToken) {
			t.Errorf("request %s carries a token in its URL", r.URL.Path)
		}
		p := strings.TrimPrefix(r.URL.Path, "/api/v2/")
		if len(q) > 0 {
			h.add(p + "?" + q.Encode())
		} else {
			h.add(p)
		}
		f, ok := overrides[p]
		if !ok {
			f = fixture{status: http.StatusOK, file: fixtureFile(r)}
		}
		if r.Header.Get("Authorization") != testAuth {
			f = fixture{status: http.StatusUnauthorized, file: "errors/unauthorized.json"}
		}
		body, err := os.ReadFile(filepath.Join("testdata", filepath.FromSlash(f.file)))
		if err != nil {
			t.Errorf("no fixture %s for %s", f.file, r.URL)
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
		baseURL:    srv.URL + "/api/v2/",
		now:        func() time.Time { return testNow },
		client:     srv.Client(),
		retryDelay: time.Millisecond,
		wait:       func(ctx context.Context, _ time.Duration) error { return ctx.Err() },
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
