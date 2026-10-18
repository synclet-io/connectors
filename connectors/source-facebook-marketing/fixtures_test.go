package main

import (
	"context"
	"encoding/json"
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

// Synthetic identifiers of the fixtures; none belongs to a real account.
const (
	testToken    = "test-access-token"
	testAccountA = "1111111111"
	testAccountB = "2222222222"
	testBusiness = "3333333333"
)

// fixture is a response the fixture server sends instead of its default:
// status with the body of testdata/<file>.json and the given headers.
type fixture struct {
	status int
	file   string
	header map[string]string
}

// apiRequest is one request the fixture server received.
type apiRequest struct {
	method string
	path   string
	query  url.Values
	form   url.Values
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

// runID is the report_run_id the fixture server gives an insights job.
func runID(acct string, form url.Values) string {
	parts := []string{"run", acct, form.Get("level")}
	if b := form.Get("breakdowns"); b != "" {
		parts = append(parts, strings.Split(b, ",")...)
	}
	return strings.Join(parts, "-")
}

// fixtureServer serves the Graph API from testdata:
//
//   - POST act_<id>/insights answers {"report_run_id": runID(...)};
//   - GET run-... answers testdata/async/completed.json;
//   - GET run-.../insights answers testdata/report_runs/run-.../insights.json;
//   - any other GET <path> answers testdata/<path>.json;
//   - a request with after=<c> gets the file with "_<c>" appended to its name.
//
// overrides["METHOD path"] replaces the default with its fixtures in order,
// repeating the last. A request without the test token in its Authorization
// header gets error 190; one that carries access_token in its URL or form
// fails the test.
func fixtureServer(t *testing.T, overrides map[string][]fixture) (*httptest.Server, *hits) {
	t.Helper()
	h := &hits{}
	var mu sync.Mutex
	served := map[string]int{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			t.Errorf("parse form: %v", err)
		}
		if r.URL.Query().Has("access_token") || r.PostForm.Has("access_token") {
			t.Errorf("request %s carries access_token in its URL or form", r.URL.Path)
		}
		p, ok := strings.CutPrefix(r.URL.Path, "/"+apiVersion+"/")
		if !ok {
			http.NotFound(w, r)
			return
		}
		h.add(apiRequest{method: r.Method, path: p, query: r.URL.Query(), form: r.PostForm})
		key := r.Method + " " + p
		var f fixture
		mu.Lock()
		seq, overridden := overrides[key]
		if overridden {
			f = seq[min(served[key], len(seq)-1)]
			served[key]++
		}
		mu.Unlock()
		if r.Header.Get("Authorization") != "Bearer "+testToken {
			f, overridden = fixture{status: http.StatusBadRequest, file: "errors/invalid_token"}, true
		}
		if !overridden {
			switch {
			case r.Method == http.MethodPost && strings.HasSuffix(p, "/insights"):
				acct := strings.TrimSuffix(strings.TrimPrefix(p, "act_"), "/insights")
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(map[string]string{"report_run_id": runID(acct, r.PostForm)})
				return
			case strings.HasPrefix(p, "run-") && !strings.Contains(p, "/"):
				f = fixture{file: "async/completed"}
			case strings.HasPrefix(p, "run-"):
				f = fixture{file: "report_runs/" + p}
			default:
				f = fixture{file: p}
			}
			if after := r.URL.Query().Get("after"); after != "" {
				f.file += "_" + after
			}
		}
		if f.status == 0 {
			f.status = http.StatusOK
		}
		body, err := os.ReadFile(filepath.Join("testdata", filepath.FromSlash(f.file)+".json"))
		if err != nil {
			t.Errorf("no fixture for %s %s: %v", r.Method, p, err)
			http.NotFound(w, r)
			return
		}
		for k, v := range f.header {
			w.Header().Set(k, v)
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

// testSource returns a Source that talks to srv with the test clock and
// records its waits instead of sleeping.
func testSource(srv *httptest.Server) (Source, *waits) {
	w := &waits{}
	return Source{
		baseURL:      srv.URL,
		now:          func() time.Time { return testNow },
		client:       srv.Client(),
		retryDelay:   time.Second,
		pollInterval: 2 * time.Second,
		wait:         w.wait,
	}, w
}

// testConfig reads both synthetic accounts and the synthetic business.
func testConfig(t *testing.T, edit func(*Config)) json.RawMessage {
	t.Helper()
	cfg := Config{
		AccessToken: testToken,
		AccountIDs:  []string{"act_" + testAccountA, testAccountB},
		BusinessIDs: []string{testBusiness},
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
