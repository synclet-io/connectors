package main

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// testReportKey is the only report key the fixture server accepts.
const testReportKey = "test-report-key"

// fixture is a response the fixture server sends instead of a testdata file:
// status with the body of testdata/<file>.
type fixture struct {
	status int
	file   string
}

// hit is one request the fixture server received: the fixture path it maps
// to and its query.
type hit struct {
	path  string
	query url.Values
}

// hits records the requests the fixture server received, in order.
type hits struct {
	mu   sync.Mutex
	list []hit
}

func (h *hits) add(x hit) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.list = append(h.list, x)
}

func (h *hits) all() []hit {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]hit(nil), h.list...)
}

// paths returns the fixture paths requested, in order.
func (h *hits) paths() []string {
	var ps []string
	for _, x := range h.all() {
		ps = append(ps, x.path)
	}
	return ps
}

// fixturePath maps a report request to its testdata file:
// /report?report_type=<type>&start=<day> to report/<type>/<day>.json and
// /maxReport?start=<day> to maxReport/<day>.json.
func fixturePath(r *http.Request) string {
	q := r.URL.Query()
	p := strings.TrimPrefix(r.URL.Path, "/")
	if t := q.Get("report_type"); t != "" {
		p += "/" + t
	}
	return p + "/" + q.Get("start") + ".json"
}

// fixtureServer serves the AppLovin reporting API from testdata. A request is
// answered with its fixturePath file, or with the override of that path or
// of its directory (report/advertiser, maxReport). A request without the
// test report key as api_key gets 401. AppLovin takes the key only as a
// query parameter, so the fixture server checks it there.
func fixtureServer(t *testing.T, overrides map[string]fixture) (*httptest.Server, *hits) {
	t.Helper()
	h := &hits{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p := fixturePath(r)
		h.add(hit{path: p, query: r.URL.Query()})
		f, ok := overrides[p]
		if !ok {
			f, ok = overrides[filepath.ToSlash(filepath.Dir(p))]
		}
		if !ok {
			f = fixture{status: http.StatusOK, file: p}
		}
		if r.URL.Query().Get("api_key") != testReportKey {
			f = fixture{status: http.StatusUnauthorized, file: "errors/invalid_key.json"}
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

// testNow is the fixed clock of every test: today is 2026-10-18 (UTC).
var testNow = time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC)

// testSource returns a Source that talks to srv with the test clock.
func testSource(srv *httptest.Server) Source {
	return Source{
		baseURL:    srv.URL + "/",
		now:        func() time.Time { return testNow },
		client:     srv.Client(),
		retryDelay: time.Millisecond,
	}
}
