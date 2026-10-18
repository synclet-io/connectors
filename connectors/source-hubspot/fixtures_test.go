package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
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

// testToken is the only private-app token the fixture server accepts.
const testToken = "test-private-app-token"

// hit is one request the fixture server received: the path without its
// leading slash, the query and the body.
type hit struct {
	method string
	path   string
	query  url.Values
	body   []byte
}

func (h hit) String() string {
	s := h.method + " " + h.path
	if len(h.query) > 0 {
		s += "?" + h.query.Encode()
	}
	return s
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

// lines returns every request as "METHOD path?query".
func (h *hits) lines() []string {
	var out []string
	for _, r := range h.list() {
		out = append(out, r.String())
	}
	return out
}

// searchBody is the part of a CRM search request body the fixtures key on.
type searchBody struct {
	FilterGroups []struct {
		Filters []struct {
			PropertyName string `json:"propertyName"`
			Operator     string `json:"operator"`
			Value        string `json:"value"`
		} `json:"filters"`
	} `json:"filterGroups"`
	Sorts      []map[string]string `json:"sorts"`
	Properties []string            `json:"properties"`
	Limit      int                 `json:"limit"`
	After      string              `json:"after"`
}

// filter returns the value of the filter on property with operator op.
func (b searchBody) filter(property, op string) string {
	for _, g := range b.FilterGroups {
		for _, f := range g.Filters {
			if f.PropertyName == property && f.Operator == op {
				return f.Value
			}
		}
	}
	return ""
}

// window returns the GTE and LT values of the search's modification
// filter, whichever property it is on.
func (b searchBody) window() (string, string) {
	for _, g := range b.FilterGroups {
		for _, f := range g.Filters {
			if f.Operator == "GTE" {
				return f.Value, b.filter(f.PropertyName, "LT")
			}
		}
	}
	return "", ""
}

// batchBody is a batch read request body.
type batchBody struct {
	Properties []string `json:"properties"`
	Inputs     []struct {
		ID string `json:"id"`
	} `json:"inputs"`
}

// fixtureFile maps a request to its testdata file: GET /<p> is answered
// by testdata/<p>.json, with after=X by testdata/<p>/after-X.json; a POST
// to <p>/search by testdata/<p>/search/<GTE value>-<hs_object_id GT
// value>.json; a POST to <p>/batch/read by testdata/<p>/batch/read/<first
// input id>.json.
func fixtureFile(method, p string, q url.Values, body []byte) string {
	if method == http.MethodGet {
		if q.Has("after") {
			return p + "/after-" + q.Get("after") + ".json"
		}
		return p + ".json"
	}
	switch {
	case strings.HasSuffix(p, "/search"):
		var b searchBody
		_ = json.Unmarshal(body, &b)
		from, _ := b.window()
		return p + "/" + from + "-" + b.filter("hs_object_id", "GT") + ".json"
	case strings.HasSuffix(p, "/batch/read"):
		var b batchBody
		_ = json.Unmarshal(body, &b)
		if len(b.Inputs) == 0 {
			return p + "/empty.json"
		}
		return p + "/" + b.Inputs[0].ID + ".json"
	}
	return p + ".json"
}

// respond answers with status and the body of testdata/<file>.
func respond(t *testing.T, status int, file string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		body, err := os.ReadFile(filepath.Join("testdata", filepath.FromSlash(file)))
		if err != nil {
			t.Errorf("no fixture %s for %s", file, r.URL)
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write(body)
	}
}

// fixtureServer serves the HubSpot API from testdata, or from the handler
// overrides holds for a request's fixture file (see fixtureFile). The v4
// labels of a pair without a fixture answer 404, as HubSpot does for
// objects that cannot be associated. A request without the test token in
// its Authorization header gets 401; one that carries the token in its URL
// fails the test.
func fixtureServer(t *testing.T, overrides map[string]http.HandlerFunc) (*httptest.Server, *hits) {
	t.Helper()
	h := &hits{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.RawQuery, testToken) || r.URL.Query().Has("hapikey") {
			t.Errorf("request %s carries a token in its URL", r.URL.Path)
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read body of %s: %v", r.URL, err)
		}
		r.Body = io.NopCloser(bytes.NewReader(body))
		p := strings.TrimPrefix(r.URL.Path, "/")
		h.add(hit{method: r.Method, path: p, query: r.URL.Query(), body: body})
		if r.Header.Get("Authorization") != "Bearer "+testToken {
			respond(t, http.StatusUnauthorized, "errors/invalid_token.json")(w, r)
			return
		}
		file := fixtureFile(r.Method, p, r.URL.Query(), body)
		if o, ok := overrides[file]; ok {
			o(w, r)
			return
		}
		if strings.HasSuffix(p, "/labels") {
			if _, err := os.Stat(filepath.Join("testdata", filepath.FromSlash(file))); errors.Is(err, fs.ErrNotExist) {
				respond(t, http.StatusNotFound, "errors/not_found.json")(w, r)
				return
			}
		}
		respond(t, http.StatusOK, file)(w, r)
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
		baseURL:    srv.URL + "/",
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
