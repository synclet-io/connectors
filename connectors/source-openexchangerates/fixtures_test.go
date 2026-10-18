package main

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// testAppID is the only app_id the fixture server accepts.
const testAppID = "test-app-id"

// fixture is a response the fixture server sends for one API path instead of
// testdata/<path>: status with the body of testdata/<file>.
type fixture struct {
	status int
	file   string
}

// hits records the API paths the fixture server was asked for, in order.
type hits struct {
	mu    sync.Mutex
	paths []string
}

func (h *hits) add(p string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.paths = append(h.paths, p)
}

func (h *hits) list() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]string(nil), h.paths...)
}

// fixtureServer serves the Open Exchange Rates API from testdata: a request
// for /api/<path> gets testdata/<path>, or its override. A request without
// the test app_id in its Authorization header gets 401; one that carries
// app_id in its URL fails the test.
func fixtureServer(t *testing.T, overrides map[string]fixture) (*httptest.Server, *hits) {
	t.Helper()
	h := &hits{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Has("app_id") {
			t.Errorf("request %s carries app_id in its URL", r.URL.Path)
		}
		p := strings.TrimPrefix(r.URL.Path, "/api/")
		h.add(p)
		f, ok := overrides[p]
		if !ok {
			f = fixture{status: http.StatusOK, file: p}
		}
		if r.Header.Get("Authorization") != "Token "+testAppID {
			f = fixture{status: http.StatusUnauthorized, file: "errors/invalid_app_id.json"}
		}
		body, err := os.ReadFile(filepath.Join("testdata", filepath.FromSlash(f.file)))
		if err != nil {
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

// testNow is the fixed clock of every test: the last full day is 2026-10-17.
var testNow = time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC)

// testSource returns a Source that talks to srv with the test clock.
func testSource(srv *httptest.Server) Source {
	return Source{
		baseURL:    srv.URL + "/api/",
		now:        func() time.Time { return testNow },
		client:     srv.Client(),
		retryDelay: time.Millisecond,
	}
}
