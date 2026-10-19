package sourceklaviyo

import (
	"context"
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
	config       = json.RawMessage(`{"api_key":"pk_test_key","start_date":"2026-01-01T00:00:00Z"}`)
	brokenConfig = json.RawMessage(`{"api_key":"pk_wrong_key","start_date":"2026-01-01T00:00:00Z"}`)
)

// requests records the filter parameter of every request by path.
type requests struct {
	mu      sync.Mutex
	filters map[string][]string
}

func (r *requests) take(path string) []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	got := r.filters[path]
	delete(r.filters, path)
	return got
}

// route answers testdata/<path>.json, testdata/<path>/page[cursor]=<c>.json
// for later pages, and splits campaigns by the channel of their filter.
func (r *requests) route(t *testing.T) func(string, url.Values) string {
	return func(path string, q url.Values) string {
		r.mu.Lock()
		r.filters[path] = append(r.filters[path], q.Get("filter"))
		r.mu.Unlock()
		if path == "profiles" {
			if got, want := q.Get("additional-fields[profile]"), "subscriptions,predictive_analytics"; got != want {
				t.Errorf("profiles additional-fields[profile] %q, want %q", got, want)
			}
			if got := q.Get("page[size]"); got != "100" {
				t.Errorf("profiles page[size] %q, want 100", got)
			}
		}
		dir := path
		if path == "campaigns" {
			channel, ok := strings.CutPrefix(q.Get("filter"), "equals(messages.channel,'")
			channel, ok2 := strings.CutSuffix(channel, "')")
			if !ok || !ok2 {
				t.Errorf("campaigns filter %q", q.Get("filter"))
			}
			dir += "/" + channel
		}
		if c := q.Get("page[cursor]"); c != "" {
			return dir + "/page[cursor]=" + url.QueryEscape(c) + ".json"
		}
		return dir + ".json"
	}
}

func suite(t *testing.T, r *requests) declarativetest.Suite {
	return declarativetest.Suite{
		Config:        config,
		BrokenConfig:  brokenConfig,
		Auth:          declarativetest.Header("Authorization", "Klaviyo-API-Key pk_test_key"),
		Headers:       map[string]string{"revision": "2025-07-15"},
		Now:           time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC),
		MinPages:      2,
		RateLimitOnce: true,
		Route:         r.route(t),
	}
}

func TestManifest(t *testing.T) {
	declarativetest.Run(t, suite(t, &requests{filters: map[string][]string{}}))
}

func TestCheckForbidden(t *testing.T) {
	s := suite(t, &requests{filters: map[string][]string{}})
	s.Overrides = map[string]declarativetest.Fixture{"metrics": {Status: 403, File: "errors/403.json"}}
	h := declarativetest.New(t, s)
	out, err := testkit.RunSource(context.Background(), h.Source, synclet.Request{Command: synclet.CommandCheck, Config: config})
	if err != nil {
		t.Fatal(err)
	}
	st := out.ConnectionStatus()
	if st == nil || st.Status != synclet.ConnectionStatusFailed {
		t.Fatalf("check on 403: %+v", st)
	}
	if want := "This API key is missing required scopes: metrics:read."; !strings.Contains(st.Message, want) {
		t.Errorf("check on 403: message %q does not carry %q", st.Message, want)
	}
}

func TestCampaignChannels(t *testing.T) {
	h := declarativetest.New(t, suite(t, &requests{filters: map[string][]string{}}))
	out := h.Read(t, h.Catalog(t, synclet.SyncModeFullRefresh, "campaigns"), nil)
	if out.Err != nil {
		t.Fatal(out.Err)
	}
	want := map[string]string{"TESTC1": "email", "TESTC2": "email", "TESTC3": "sms", "TESTC4": "mobile_push"}
	got := map[string]string{}
	for _, rec := range out.Records(synclet.StreamDescriptor{Name: "campaigns"}) {
		var c struct{ ID, Channel string }
		if err := json.Unmarshal(rec.Data, &c); err != nil {
			t.Fatal(err)
		}
		got[c.ID] = c.Channel
	}
	if !maps.Equal(got, want) {
		t.Errorf("campaign channels %v, want %v", got, want)
	}
}

func TestIncrementalFilter(t *testing.T) {
	for _, tc := range []struct{ stream, op, field, top string }{
		{"profiles", "greater-than", "updated", "2026-03-05T09:15:00+00:00"},
		{"events", "greater-or-equal", "datetime", "2026-03-05T10:00:00+00:00"},
	} {
		t.Run(tc.stream, func(t *testing.T) {
			r := &requests{filters: map[string][]string{}}
			h := declarativetest.New(t, suite(t, r))
			cat := h.Catalog(t, synclet.SyncModeIncremental, tc.stream)
			r.take(tc.stream)

			first := h.Read(t, cat, nil)
			if first.Err != nil {
				t.Fatal(first.Err)
			}
			want := tc.op + "(" + tc.field + ",2026-01-01T00:00:00Z)"
			if got := r.take(tc.stream); len(got) != 2 || got[0] != want || got[1] != want {
				t.Errorf("first read filters %q, want %q on both pages", got, want)
			}
			var msgs []synclet.StateMessage
			if err := json.Unmarshal(first.NextState(), &msgs); err != nil {
				t.Fatal(err)
			}
			if len(msgs) != 1 || !strings.Contains(string(msgs[0].Stream.StreamState), `"`+tc.field+`":"`+tc.top+`"`) {
				t.Fatalf("state after the first read: %s", first.NextState())
			}

			second := h.Read(t, cat, first.NextState())
			if second.Err != nil {
				t.Fatal(second.Err)
			}
			top, _ := time.Parse(time.RFC3339, tc.top)
			want = tc.op + "(" + tc.field + "," + top.Add(-time.Hour).UTC().Format(time.RFC3339) + ")"
			if got := r.take(tc.stream); len(got) == 0 || got[0] != want {
				t.Errorf("resumed read filters %q, want %q", got, want)
			}
		})
	}
}

// documented holds the attributes Klaviyo documents for each stream at the
// pinned revision; a schema must keep every one of them.
var documented = map[string][]string{
	"profiles": {"email", "phone_number", "external_id", "first_name", "last_name", "organization", "locale", "title", "image",
		"created", "updated", "last_event_date", "location", "properties", "subscriptions", "predictive_analytics"},
	"events":    {"timestamp", "event_properties", "datetime", "uuid"},
	"campaigns": {"name", "status", "archived", "audiences", "send_options", "tracking_options", "send_strategy", "created_at", "scheduled_at", "updated_at", "send_time"},
	"flows":     {"name", "status", "archived", "created", "updated", "trigger_type"},
	"lists":     {"name", "created", "updated", "opt_in_process"},
	"metrics":   {"name", "created", "updated", "integration"},
	"segments":  {"name", "definition", "created", "updated", "is_active", "is_processing", "is_starred"},
}

func TestSchemasKeepEveryAttribute(t *testing.T) {
	for stream, attrs := range documented {
		raw, err := os.ReadFile(filepath.Join("schemas", stream+".json"))
		if err != nil {
			t.Fatal(err)
		}
		var s map[string]any
		if err := json.Unmarshal(raw, &s); err != nil {
			t.Fatalf("%s: %v", stream, err)
		}
		props, _ := s["properties"].(map[string]any)
		for _, k := range []string{"type", "id", "attributes", "relationships", "links"} {
			if _, ok := props[k]; !ok {
				t.Errorf("%s: no %q", stream, k)
			}
		}
		attributes, _ := props["attributes"].(map[string]any)
		declared, _ := attributes["properties"].(map[string]any)
		for _, a := range attrs {
			if _, ok := declared[a]; !ok {
				t.Errorf("%s: attribute %q is missing", stream, a)
			}
		}
		open(t, stream, s)
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

func TestUnknownAttributeIsKept(t *testing.T) {
	h := declarativetest.New(t, suite(t, &requests{filters: map[string][]string{}}))
	out := h.Read(t, h.Catalog(t, synclet.SyncModeFullRefresh, "profiles"), nil)
	if out.Err != nil {
		t.Fatal(out.Err)
	}
	for _, rec := range out.Records(synclet.StreamDescriptor{Name: "profiles"}) {
		if strings.Contains(string(rec.Data), `"x_unlisted_attribute":"kept"`) {
			return
		}
	}
	t.Error("no profile record carries the attribute the schema does not list")
}
