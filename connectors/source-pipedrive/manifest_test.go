package sourcepipedrive

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	synclet "github.com/synclet-io/synclet-go-sdk"
	"github.com/synclet-io/synclet-go-sdk/testkit"

	"source-declarative/declarativetest"
)

var (
	config = json.RawMessage(`{"api_token":"test-api-token","start_date":"2026-01-01T00:00:00Z"}`)
	broken = json.RawMessage(`{"api_token":"wrong-api-token","start_date":"2026-01-01T00:00:00Z"}`)
	now    = time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC)
)

// requests records the query of every request by path.
type requests struct {
	mu      sync.Mutex
	queries map[string][]url.Values
}

func newRequests() *requests { return &requests{queries: map[string][]url.Values{}} }

func (r *requests) take(path string) []url.Values {
	r.mu.Lock()
	defer r.mu.Unlock()
	got := r.queries[path]
	delete(r.queries, path)
	return got
}

// route answers testdata/<path>.json, testdata/<path>/cursor=<c>.json for
// later v2 pages and testdata/<path>/start=<n>.json for later v1 pages.
func (r *requests) route(path string, q url.Values) string {
	r.mu.Lock()
	r.queries[path] = append(r.queries[path], q)
	r.mu.Unlock()
	if c := q.Get("cursor"); c != "" {
		return path + "/cursor=" + url.QueryEscape(c) + ".json"
	}
	if s := q.Get("start"); s != "" && s != "0" {
		return path + "/start=" + s + ".json"
	}
	return path + ".json"
}

func suite(r *requests) declarativetest.Suite {
	return declarativetest.Suite{
		Config:        config,
		BrokenConfig:  broken,
		Auth:          declarativetest.Query("api_token", "test-api-token"),
		Now:           now,
		MinPages:      2,
		RateLimitOnce: true,
		Route:         r.route,
	}
}

func TestManifest(t *testing.T) {
	declarativetest.Run(t, suite(newRequests()))
}

func TestCheckForbidden(t *testing.T) {
	s := suite(newRequests())
	s.Overrides = map[string]declarativetest.Fixture{"v1/users": {Status: 403, File: "errors/403.json"}}
	h := declarativetest.New(t, s)
	out, err := testkit.RunSource(context.Background(), h.Source, synclet.Request{Command: synclet.CommandCheck, Config: config})
	if err != nil {
		t.Fatal(err)
	}
	if st := out.ConnectionStatus(); st == nil || st.Status != synclet.ConnectionStatusFailed || !strings.Contains(st.Message, "You do not have permissions") {
		t.Fatalf("check on 403: %+v", st)
	}
}

// A company without notes or files gets "data": null from v1.
func TestEmptyV1Lists(t *testing.T) {
	r := newRequests()
	s := suite(r)
	s.Overrides = map[string]declarativetest.Fixture{
		"v1/notes": {Status: 200, File: "v1/empty.json"},
		"v1/files": {Status: 200, File: "v1/empty.json"},
	}
	h := declarativetest.New(t, s)
	out := h.Read(t, h.Catalog(t, synclet.SyncModeFullRefresh, "notes", "files"), nil)
	if out.Err != nil {
		t.Fatalf("read: %v", out.Err)
	}
	for _, name := range []string{"notes", "files"} {
		if n := len(out.Records(synclet.StreamDescriptor{Name: name})); n != 0 {
			t.Errorf("%s: %d records, want none", name, n)
		}
		if got := r.take("v1/" + name); len(got) != 1 {
			t.Errorf("%s: %d requests, want 1", name, len(got))
		}
	}
}

func TestIncrementalUpdatedSince(t *testing.T) {
	for _, tc := range []struct{ stream, resumed string }{
		{"deals", "2026-03-07T09:00:00Z"},
		{"persons", "2026-03-05T09:00:00Z"},
		{"organizations", "2026-03-04T09:00:00Z"},
		{"activities", "2026-03-03T09:00:00Z"},
	} {
		t.Run(tc.stream, func(t *testing.T) {
			r := newRequests()
			h := declarativetest.New(t, suite(r))
			cat := h.Catalog(t, synclet.SyncModeIncremental, tc.stream)
			path := "v2/" + tc.stream
			first := h.Read(t, cat, nil)
			if first.Err != nil {
				t.Fatal(first.Err)
			}
			got := r.take(path)
			if len(got) != 2 {
				t.Fatalf("first read: %d requests, want 2", len(got))
			}
			for _, q := range got {
				if q.Get("updated_since") != "2026-01-01T00:00:00Z" || q.Get("sort_by") != "update_time" || q.Get("sort_direction") != "asc" || q.Get("limit") != "500" {
					t.Errorf("first read query %v", q)
				}
			}
			second := h.Read(t, cat, first.NextState())
			if second.Err != nil {
				t.Fatal(second.Err)
			}
			if got := r.take(path); len(got) == 0 || got[0].Get("updated_since") != tc.resumed {
				t.Errorf("resumed read: %v, want updated_since %q", got, tc.resumed)
			}
		})
	}
}

func TestTokenStaysInQuery(t *testing.T) {
	var mu sync.Mutex
	var bad []string
	s := suite(newRequests())
	s.Inspect = func(r *http.Request) {
		if r.Header.Get("Authorization") != "" || r.URL.Query().Get("api_token") == "" {
			mu.Lock()
			bad = append(bad, r.URL.Path)
			mu.Unlock()
		}
	}
	h := declarativetest.New(t, s)
	if out := h.Read(t, h.Catalog(t, synclet.SyncModeFullRefresh), nil); out.Err != nil {
		t.Fatal(out.Err)
	}
	if len(bad) > 0 {
		t.Errorf("requests without the api_token parameter or with an Authorization header: %v", bad)
	}
}

func TestSchemasAreOpen(t *testing.T) {
	files, err := filepath.Glob("schemas/*.json")
	if err != nil || len(files) == 0 {
		t.Fatalf("schemas: %v", err)
	}
	for _, f := range files {
		raw, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		var s map[string]any
		if err := json.Unmarshal(raw, &s); err != nil {
			t.Fatalf("%s: %v", f, err)
		}
		open(t, f, s)
	}
}

// open fails on every object schema of s that does not allow unknown
// properties.
func open(t *testing.T, at string, s map[string]any) {
	t.Helper()
	typ := s["type"]
	isObject := typ == "object"
	if list, ok := typ.([]any); ok {
		isObject = slices.Contains(list, any("object"))
	}
	if isObject && s["additionalProperties"] != true {
		t.Errorf("%s: additionalProperties is not true", at)
	}
	props, _ := s["properties"].(map[string]any)
	for k, v := range props {
		if sub, ok := v.(map[string]any); ok {
			open(t, at+"."+k, sub)
		}
	}
	if items, ok := s["items"].(map[string]any); ok {
		open(t, at+"[]", items)
	}
}

func TestUnknownFieldIsKept(t *testing.T) {
	h := declarativetest.New(t, suite(newRequests()))
	out := h.Read(t, h.Catalog(t, synclet.SyncModeFullRefresh, "deals"), nil)
	if out.Err != nil {
		t.Fatal(out.Err)
	}
	for _, rec := range out.Records(synclet.StreamDescriptor{Name: "deals"}) {
		if strings.Contains(string(rec.Data), `"x_unlisted_field":"kept"`) {
			return
		}
	}
	t.Error("no deal record carries the field the schema does not list")
}

func TestTokenNeverInOutput(t *testing.T) {
	s := suite(newRequests())
	s.Overrides = map[string]declarativetest.Fixture{"v2/deals": {Status: 400, File: "errors/400.json"}}
	h := declarativetest.New(t, s)
	out := h.Read(t, h.Catalog(t, synclet.SyncModeFullRefresh, "deals"), nil)
	if out.Err == nil {
		t.Fatal("read on 400: no error")
	}
	raw, err := json.Marshal(out.Messages)
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range []string{out.Err.Error(), string(raw)} {
		if strings.Contains(s, "test-api-token") {
			t.Errorf("api token in the output: %s", s)
		}
	}
	if !strings.Contains(out.Err.Error(), "Bad request") {
		t.Errorf("error %q does not carry the API's text", out.Err)
	}
}
