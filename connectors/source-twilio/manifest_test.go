package sourcetwilio

import (
	"context"
	"encoding/base64"
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

const account = "AC00000000000000000000000000000001"

var (
	config = json.RawMessage(`{"account_sid":"` + account + `","auth_token":"test-auth-token","start_date":"2026-01-01T00:00:00Z"}`)
	broken = json.RawMessage(`{"account_sid":"` + account + `","auth_token":"wrong-auth-token","start_date":"2026-01-01T00:00:00Z"}`)
	now    = time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC)
	auth   = declarativetest.Header("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte(account+":test-auth-token")))
)

// dataSpan is the time span the message and usage fixtures cover; a window
// outside it is answered by the empty page.
var dataSpan = [2]time.Time{
	time.Date(2026, 3, 5, 0, 0, 0, 0, time.UTC),
	time.Date(2026, 3, 8, 23, 59, 59, 0, time.UTC),
}

// requests records the query of every request by path.
type requests struct {
	mu      sync.Mutex
	queries map[string][]url.Values
}

func (r *requests) take(path string) []url.Values {
	r.mu.Lock()
	defer r.mu.Unlock()
	got := r.queries[path]
	delete(r.queries, path)
	return got
}

// route answers testdata/<path without .json>.json, PageToken=<t>.json for
// later pages and empty.json for a message or usage window outside dataSpan.
func (r *requests) route(t *testing.T) func(string, url.Values) string {
	return func(path string, q url.Values) string {
		r.mu.Lock()
		r.queries[path] = append(r.queries[path], q)
		r.mu.Unlock()
		base := strings.TrimSuffix(path, ".json")
		var from, to string
		layout := time.RFC3339
		switch {
		case strings.HasSuffix(base, "/Messages"):
			from, to = q.Get("DateSent>"), q.Get("DateSent<")
		case strings.HasSuffix(base, "/Usage/Records/Daily"):
			from, to, layout = q.Get("StartDate"), q.Get("EndDate"), time.DateOnly
		}
		if from != "" || to != "" {
			since, err1 := time.Parse(layout, from)
			until, err2 := time.Parse(layout, to)
			if err1 != nil || err2 != nil {
				t.Errorf("%s: window %q..%q", path, from, to)
			}
			if since.After(dataSpan[1]) || !until.After(dataSpan[0]) {
				return base + "/empty.json"
			}
		}
		if tok := q.Get("PageToken"); tok != "" {
			return base + "/PageToken=" + tok + ".json"
		}
		return base + ".json"
	}
}

func suite(t *testing.T, r *requests) declarativetest.Suite {
	return declarativetest.Suite{
		Config:        config,
		BrokenConfig:  broken,
		Auth:          auth,
		Now:           now,
		MinPages:      2,
		RateLimitOnce: true,
		BasePath:      "/2010-04-01/",
		Route:         r.route(t),
	}
}

func newRequests() *requests { return &requests{queries: map[string][]url.Values{}} }

func TestManifest(t *testing.T) {
	declarativetest.Run(t, suite(t, newRequests()))
}

func TestCheckForbidden(t *testing.T) {
	s := suite(t, newRequests())
	s.Overrides = map[string]declarativetest.Fixture{"Accounts.json": {Status: 403, File: "errors/403.json"}}
	h := declarativetest.New(t, s)
	out, err := testkit.RunSource(context.Background(), h.Source, synclet.Request{Command: synclet.CommandCheck, Config: config})
	if err != nil {
		t.Fatal(err)
	}
	if st := out.ConnectionStatus(); st == nil || st.Status != synclet.ConnectionStatusFailed || !strings.Contains(st.Message, "Permission Denied") {
		t.Fatalf("check on 403: %+v", st)
	}
}

func TestIncrementalFilters(t *testing.T) {
	for _, tc := range []struct {
		stream, path, param, first, resumed string
	}{
		{"calls", "Calls.json", "EndTime>", "2026-01-01T00:00:00Z", "2026-03-07T08:00:00Z"},
		{"recordings", "Recordings.json", "DateCreated>", "2026-01-01T00:00:00Z", "2026-03-07T08:00:00Z"},
		{"conferences", "Conferences.json", "DateCreated>", "2026-01-01", "2026-03-08"},
	} {
		t.Run(tc.stream, func(t *testing.T) {
			r := newRequests()
			h := declarativetest.New(t, suite(t, r))
			cat := h.Catalog(t, synclet.SyncModeIncremental, tc.stream)
			path := "Accounts/" + account + "/" + tc.path
			first := h.Read(t, cat, nil)
			if first.Err != nil {
				t.Fatal(first.Err)
			}
			if got := r.take(path); len(got) != 2 || got[0].Get(tc.param) != tc.first || got[1].Get(tc.param) != tc.first {
				t.Errorf("first read %s: %v, want %q on both pages", tc.param, got, tc.first)
			}
			second := h.Read(t, cat, first.NextState())
			if second.Err != nil {
				t.Fatal(second.Err)
			}
			if got := r.take(path); len(got) == 0 || got[0].Get(tc.param) != tc.resumed {
				t.Errorf("resumed read %s: %v, want %q", tc.param, got, tc.resumed)
			}
		})
	}
}

func TestWindows(t *testing.T) {
	for _, tc := range []struct {
		stream, path, from, to, layout string
		step                           time.Duration
	}{
		{"messages", "Messages.json", "DateSent>", "DateSent<", time.RFC3339, 30 * 24 * time.Hour},
		{"usage_records", "Usage/Records/Daily.json", "StartDate", "EndDate", time.DateOnly, 30 * 24 * time.Hour},
	} {
		t.Run(tc.stream, func(t *testing.T) {
			r := newRequests()
			h := declarativetest.New(t, suite(t, r))
			out := h.Read(t, h.Catalog(t, synclet.SyncModeIncremental, tc.stream), nil)
			if out.Err != nil {
				t.Fatal(out.Err)
			}
			var windows [][2]string
			for _, q := range r.take("Accounts/" + account + "/" + tc.path) {
				if q.Get("PageToken") == "" {
					windows = append(windows, [2]string{q.Get(tc.from), q.Get(tc.to)})
				}
			}
			start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
			var want [][2]string
			for s := start; s.Before(now); s = s.Add(tc.step) {
				e := s.Add(tc.step)
				if e.After(now) {
					e = now
				}
				want = append(want, [2]string{s.Format(tc.layout), e.Format(tc.layout)})
			}
			if !slices.Equal(windows, want) {
				t.Errorf("windows %v, want %v", windows, want)
			}
			var states []synclet.StateMessage
			for _, m := range out.Messages {
				if m.State != nil {
					states = append(states, *m.State)
				}
			}
			if len(states) != len(want) {
				t.Errorf("%d STATE messages, want one per window (%d)", len(states), len(want))
			}
		})
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
	h := declarativetest.New(t, suite(t, newRequests()))
	out := h.Read(t, h.Catalog(t, synclet.SyncModeFullRefresh, "accounts"), nil)
	if out.Err != nil {
		t.Fatal(out.Err)
	}
	for _, rec := range out.Records(synclet.StreamDescriptor{Name: "accounts"}) {
		if strings.Contains(string(rec.Data), `"x_unlisted_field":"kept"`) {
			return
		}
	}
	t.Error("no account record carries the field the schema does not list")
}
