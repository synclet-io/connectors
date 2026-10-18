package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// testKey is the only key the fixture server accepts; testV1Key is
// answered the way the API answers an API v1 key.
const (
	testKey   = "test-secret-key"
	testV1Key = "test-v1-key"
	testProj  = "proj_test1"
)

// fixture is a response the fixture server sends for one API path instead
// of its testdata file: status and headers with the body of testdata/<file>.
// times > 0 sends it for the first times requests of the path only.
type fixture struct {
	status  int
	file    string
	headers map[string]string
	times   int
}

// hits records the requests the fixture server was asked for, in order, as
// the path under /v2/ followed by the sorted query, if any.
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

// fixtureFile maps a request to its testdata file: GET /v2/<p> is answered
// by testdata/<p>.json, with starting_after=X by testdata/<p>/after-X.json.
func fixtureFile(r *http.Request) string {
	p := strings.TrimPrefix(r.URL.Path, "/v2/")
	if q := r.URL.Query(); q.Has("starting_after") {
		return p + "/after-" + q.Get("starting_after") + ".json"
	}
	return p + ".json"
}

// fixtureServer serves the RevenueCat API v2 from testdata, or a path's
// override (keyed by the path under /v2/). A request without the test key
// as its Bearer token gets 401; a path under another project than
// proj_test1 gets 404; a request that carries the key in its URL fails the
// test.
func fixtureServer(t *testing.T, overrides map[string]fixture) (*httptest.Server, *hits) {
	t.Helper()
	h := &hits{}
	var mu sync.Mutex
	served := map[string]int{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.RawQuery, testKey) || strings.Contains(r.URL.RawQuery, testV1Key) {
			t.Errorf("request %s carries a key in its URL", r.URL.Path)
		}
		p := strings.TrimPrefix(r.URL.Path, "/v2/")
		if q := r.URL.Query(); len(q) > 0 {
			h.add(p + "?" + q.Encode())
		} else {
			h.add(p)
		}
		f := fixture{status: http.StatusOK, file: fixtureFile(r)}
		if o, ok := overrides[p]; ok {
			mu.Lock()
			served[p]++
			n := served[p]
			mu.Unlock()
			if o.times == 0 || n <= o.times {
				f = o
			}
		}
		if rest, ok := strings.CutPrefix(p, "projects/"); ok && !strings.HasPrefix(rest, testProj+"/") && strings.Contains(rest, "/") {
			f = fixture{status: http.StatusNotFound, file: "errors/resource_missing.json"}
		}
		switch r.Header.Get("Authorization") {
		case "Bearer " + testKey:
		case "Bearer " + testV1Key:
			f = fixture{status: http.StatusUnauthorized, file: "errors/v1_key.json"}
		default:
			f = fixture{status: http.StatusUnauthorized, file: "errors/unauthorized.json"}
		}
		body, err := os.ReadFile(filepath.Join("testdata", filepath.FromSlash(f.file)))
		if err != nil {
			t.Errorf("no fixture %s for %s", f.file, r.URL)
			http.NotFound(w, r)
			return
		}
		for k, v := range f.headers {
			w.Header().Set(k, v)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(f.status)
		_, _ = w.Write(body)
	}))
	t.Cleanup(srv.Close)
	return srv, h
}

// clock is a fake clock: wait records each duration and moves the clock
// forward by it instead of sleeping.
type clock struct {
	mu sync.Mutex
	t  time.Time
	ds []time.Duration
}

func newClock() *clock { return &clock{t: time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC)} }

func (c *clock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *clock) wait(ctx context.Context, d time.Duration) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ds = append(c.ds, d)
	c.t = c.t.Add(d)
	return ctx.Err()
}

func (c *clock) waits() []time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]time.Duration(nil), c.ds...)
}

// testSource returns a Source that talks to srv on the fake clock clk.
func testSource(srv *httptest.Server, clk *clock) Source {
	return Source{
		baseURL:    srv.URL + "/v2/",
		client:     srv.Client(),
		now:        clk.now,
		retryDelay: time.Millisecond,
		wait:       clk.wait,
	}
}

// httptestServer answers every request with the test key with body, and
// any other with the API's 401.
func httptestServer(t *testing.T, body string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Header.Get("Authorization") != "Bearer "+testKey {
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"type":"authentication_error","message":"Invalid API key."}`))
			return
		}
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	return srv
}
