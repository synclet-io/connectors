package sourcemailchimp

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"maps"
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
	config = json.RawMessage(`{"api_key":"0123456789abcdef-us0","data_center":"us0","start_date":"2026-01-01T00:00:00Z"}`)
	broken = json.RawMessage(`{"api_key":"fedcba9876543210-us0","data_center":"us0","start_date":"2026-01-01T00:00:00Z"}`)
	now    = time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC)
	auth   = declarativetest.Header("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte("synclet:0123456789abcdef-us0")))
)

// unsent is the campaign without a report: its email activity answers 404.
const unsent = "reports/cmp0000003/email-activity"

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

// route answers testdata/<path>.json for offset 0 and
// testdata/<path>/offset=<n>.json for later pages.
func (r *requests) route(path string, q url.Values) string {
	r.mu.Lock()
	r.queries[path] = append(r.queries[path], q)
	r.mu.Unlock()
	if o := q.Get("offset"); o != "" && o != "0" {
		return path + "/offset=" + o + ".json"
	}
	return path + ".json"
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
		Overrides:     map[string]declarativetest.Fixture{unsent: {Status: 404, File: unsent + ".json"}},
	}
}

func TestManifest(t *testing.T) {
	declarativetest.Run(t, suite(newRequests()))
}

func TestCheckForbidden(t *testing.T) {
	s := suite(newRequests())
	s.Overrides = map[string]declarativetest.Fixture{"lists": {Status: 403, File: "errors/403.json"}}
	h := declarativetest.New(t, s)
	out, err := testkit.RunSource(context.Background(), h.Source, synclet.Request{Command: synclet.CommandCheck, Config: config})
	if err != nil {
		t.Fatal(err)
	}
	if st := out.ConnectionStatus(); st == nil || st.Status != synclet.ConnectionStatusFailed || !strings.Contains(st.Message, "linked to a different datacenter") {
		t.Fatalf("check on 403: %+v", st)
	}
}

func TestIncrementalFilters(t *testing.T) {
	for _, tc := range []struct{ stream, param, resumed string }{
		{"campaigns", "since_create_time", "2026-03-03T10:00:00Z"},
		{"reports", "since_send_time", "2026-03-04T10:00:00Z"},
		{"automations", "since_create_time", "2026-02-03T10:00:00Z"},
	} {
		t.Run(tc.stream, func(t *testing.T) {
			r := newRequests()
			h := declarativetest.New(t, suite(r))
			cat := h.Catalog(t, synclet.SyncModeIncremental, tc.stream)
			r.take(tc.stream)
			first := h.Read(t, cat, nil)
			if first.Err != nil {
				t.Fatal(first.Err)
			}
			if got := r.take(tc.stream); len(got) != 2 || got[0].Get(tc.param) != "2026-01-01T00:00:00Z" || got[1].Get(tc.param) != "2026-01-01T00:00:00Z" {
				t.Errorf("first read %s: %v, want start_date on both pages", tc.param, got)
			}
			second := h.Read(t, cat, first.NextState())
			if second.Err != nil {
				t.Fatal(second.Err)
			}
			if got := r.take(tc.stream); len(got) == 0 || got[0].Get(tc.param) != tc.resumed {
				t.Errorf("resumed read %s: %v, want %q", tc.param, got, tc.resumed)
			}
		})
	}
}

func TestListChildren(t *testing.T) {
	h := declarativetest.New(t, suite(newRequests()))
	out := h.Read(t, h.Catalog(t, synclet.SyncModeFullRefresh, "list_members", "segments", "tags", "interest_categories"), nil)
	if out.Err != nil {
		t.Fatal(out.Err)
	}
	want := map[string]int{"a1b2c3d4e5": 3, "f6a7b8c9d0": 1, "0a1b2c3d4e": 1}
	for stream, n := range map[string]map[string]int{"list_members": want, "segments": want, "interest_categories": want,
		"tags": {"a1b2c3d4e5": 2, "f6a7b8c9d0": 2, "0a1b2c3d4e": 2}} {
		got := map[string]int{}
		for _, rec := range out.Records(synclet.StreamDescriptor{Name: stream}) {
			var v struct {
				ListID string `json:"list_id"`
			}
			if err := json.Unmarshal(rec.Data, &v); err != nil {
				t.Fatal(err)
			}
			got[v.ListID]++
		}
		if !maps.Equal(got, n) {
			t.Errorf("%s per list %v, want %v", stream, got, n)
		}
	}
}

func TestEmailActivitySkipsUnsentCampaign(t *testing.T) {
	h := declarativetest.New(t, suite(newRequests()))
	out := h.Read(t, h.Catalog(t, synclet.SyncModeFullRefresh, "email_activity"), nil)
	if out.Err != nil {
		t.Fatal(out.Err)
	}
	got := map[string]int{}
	for _, rec := range out.Records(synclet.StreamDescriptor{Name: "email_activity"}) {
		var v struct {
			CampaignID string `json:"campaign_id"`
		}
		if err := json.Unmarshal(rec.Data, &v); err != nil {
			t.Fatal(err)
		}
		got[v.CampaignID]++
	}
	if want := map[string]int{"cmp0000001": 3, "cmp0000002": 1}; !maps.Equal(got, want) {
		t.Errorf("email activity per campaign %v, want %v", got, want)
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
	out := h.Read(t, h.Catalog(t, synclet.SyncModeFullRefresh, "lists"), nil)
	if out.Err != nil {
		t.Fatal(out.Err)
	}
	for _, rec := range out.Records(synclet.StreamDescriptor{Name: "lists"}) {
		if strings.Contains(string(rec.Data), `"x_unlisted_field":"kept"`) {
			return
		}
	}
	t.Error("no list record carries the field the schema does not list")
}
