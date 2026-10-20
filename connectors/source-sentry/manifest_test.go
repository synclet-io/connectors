package sourcesentry

import (
	"context"
	"encoding/json"
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
	config = json.RawMessage(`{"auth_token":"test-token","hostname":"sentry.io","organization":"example-org","project":"example-project","start_date":"2026-01-01T00:00:00Z"}`)
	broken = json.RawMessage(`{"auth_token":"wrong-token","hostname":"sentry.io","organization":"example-org","project":"example-project","start_date":"2026-01-01T00:00:00Z"}`)
	now    = time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC)
	auth   = declarativetest.Header("Authorization", "Bearer test-token")
)

const project = "projects/example-org/example-project/"

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

// route answers testdata/<path without its trailing slash>.json and
// testdata/<path>/cursor=<cursor>.json for later pages.
func (r *requests) route(path string, q url.Values) string {
	r.mu.Lock()
	r.queries[path] = append(r.queries[path], q)
	r.mu.Unlock()
	base := strings.TrimSuffix(path, "/")
	if c := q.Get("cursor"); c != "" {
		return base + "/cursor=" + url.QueryEscape(c) + ".json"
	}
	return base + ".json"
}

func suite(r *requests) declarativetest.Suite {
	return declarativetest.Suite{
		Config:        config,
		BrokenConfig:  broken,
		Auth:          auth,
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
	s.Overrides = map[string]declarativetest.Fixture{project: {Status: 403, File: "errors/403.json"}}
	h := declarativetest.New(t, s)
	out, err := testkit.RunSource(context.Background(), h.Source, synclet.Request{Command: synclet.CommandCheck, Config: config})
	if err != nil {
		t.Fatal(err)
	}
	if st := out.ConnectionStatus(); st == nil || st.Status != synclet.ConnectionStatusFailed || !strings.Contains(st.Message, "You do not have permission") {
		t.Fatalf("check on 403: %+v", st)
	}
}

func TestIncrementalFilters(t *testing.T) {
	for _, tc := range []struct {
		stream, path, param, first, resumed string
	}{
		{"issues", project + "issues/", "query", "lastSeen:>=2026-01-01T00:00:00", "lastSeen:>=2026-03-07T09:00:00"},
		{"events", project + "events/", "start", "2026-01-01T00:00:00", "2026-03-07T09:00:00"},
	} {
		t.Run(tc.stream, func(t *testing.T) {
			r := newRequests()
			h := declarativetest.New(t, suite(r))
			cat := h.Catalog(t, synclet.SyncModeIncremental, tc.stream)
			first := h.Read(t, cat, nil)
			if first.Err != nil {
				t.Fatal(first.Err)
			}
			got := r.take(tc.path)
			if len(got) != 3 {
				t.Fatalf("first read: %d requests, want 3", len(got))
			}
			for _, q := range got {
				if q.Get(tc.param) != tc.first {
					t.Errorf("first read %s %q, want %q", tc.param, q.Get(tc.param), tc.first)
				}
				if tc.stream == "events" && (q.Get("end") != "2026-10-18T12:00:00" || q.Get("full") != "true") {
					t.Errorf("events end %q full %q", q.Get("end"), q.Get("full"))
				}
			}
			second := h.Read(t, cat, first.NextState())
			if second.Err != nil {
				t.Fatal(second.Err)
			}
			if got := r.take(tc.path); len(got) == 0 || got[0].Get(tc.param) != tc.resumed {
				t.Errorf("resumed read %s: %v, want %q", tc.param, got, tc.resumed)
			}
		})
	}
}

func TestReleasesKeepNewerOnly(t *testing.T) {
	h := declarativetest.New(t, suite(newRequests()))
	cat := h.Catalog(t, synclet.SyncModeIncremental, "releases")
	first := h.Read(t, cat, nil)
	if first.Err != nil {
		t.Fatal(first.Err)
	}
	if n := len(first.Records(synclet.StreamDescriptor{Name: "releases"})); n != 3 {
		t.Fatalf("first read: %d releases, want 3", n)
	}
	second := h.Read(t, cat, first.NextState())
	if second.Err != nil {
		t.Fatal(second.Err)
	}
	recs := second.Records(synclet.StreamDescriptor{Name: "releases"})
	if len(recs) != 1 || !strings.Contains(string(recs[0].Data), `"version":"1.0.2"`) {
		t.Errorf("resumed read: %d releases, want only the newest", len(recs))
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
	out := h.Read(t, h.Catalog(t, synclet.SyncModeFullRefresh, "projects"), nil)
	if out.Err != nil {
		t.Fatal(out.Err)
	}
	for _, rec := range out.Records(synclet.StreamDescriptor{Name: "projects"}) {
		if strings.Contains(string(rec.Data), `"x_unlisted_field":"kept"`) {
			return
		}
	}
	t.Error("no project record carries the field the schema does not list")
}
