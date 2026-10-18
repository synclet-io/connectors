package main

import (
	"context"
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

// testToken is the only personal access token the fixture server accepts.
const testToken = "patTester.0123456789abcdef"

const (
	base1 = "appBase0000000001"
	base2 = "appBase0000000002"
	base3 = "appBase0000000003"
)

// fixture is a response the fixture server sends for one API path instead of
// its testdata file: status with the body of testdata/<file>.
type fixture struct {
	status int
	file   string
}

// hits records the requests the fixture server was asked for, in order, as
// the path under /v0/ followed by the sorted query, if any.
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

var sinceTimestamp = regexp.MustCompile(`DATETIME_PARSE\('([^']+)'\)`)

// fixtureFile maps a request to its testdata file: GET /v0/<p> is answered
// by testdata/<p>.json; with offset=X by testdata/<p>/offset-X.json; with a
// filterByFormula from timestamp T by testdata/<p>/since-T.json. Slashes in
// X and colons in T become hyphens.
func fixtureFile(r *http.Request) string {
	p := strings.TrimPrefix(r.URL.Path, "/v0/")
	q := r.URL.Query()
	name := ""
	if f := q.Get("filterByFormula"); f != "" {
		m := sinceTimestamp.FindStringSubmatch(f)
		if m == nil {
			return p + "/unparsed-formula.json"
		}
		name = "since-" + strings.ReplaceAll(m[1], ":", "-")
	}
	if o := q.Get("offset"); o != "" {
		if name != "" {
			name += "-"
		}
		name += "offset-" + strings.ReplaceAll(o, "/", "-")
	}
	if name == "" {
		return p + ".json"
	}
	return p + "/" + name + ".json"
}

// fixtureServer serves the Airtable API from testdata, or a path's override
// (keyed by the path under /v0/). A request without the test token in its
// Authorization header gets 401; one that carries the token in its URL
// fails the test.
func fixtureServer(t *testing.T, overrides map[string]fixture) (*httptest.Server, *hits) {
	t.Helper()
	h := &hits{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.RawQuery, testToken) {
			t.Errorf("request %s carries the token in its URL", r.URL.Path)
		}
		p := strings.TrimPrefix(r.URL.Path, "/v0/")
		if r.URL.RawQuery != "" {
			h.add(p + "?" + r.URL.Query().Encode())
		} else {
			h.add(p)
		}
		f, ok := overrides[p]
		if !ok {
			f = fixture{status: http.StatusOK, file: fixtureFile(r)}
		}
		if r.Header.Get("Authorization") != "Bearer "+testToken {
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
		baseURL:    srv.URL + "/v0/",
		now:        func() time.Time { return testNow },
		client:     srv.Client(),
		retryDelay: time.Millisecond,
		wait:       func(ctx context.Context, _ time.Duration) error { return ctx.Err() },
	}
}
