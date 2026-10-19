package sourcechargebee

import (
	"context"
	"encoding/json"
	"maps"
	"net/url"
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
	config       = json.RawMessage(`{"site":"synthetic-test","site_api_key":"test_synthetic_key","start_date":"2026-01-01T00:00:00Z"}`)
	brokenConfig = json.RawMessage(`{"site":"synthetic-test","site_api_key":"wrong_key","start_date":"2026-01-01T00:00:00Z"}`)
	auth         = declarativetest.Header("Authorization", "Basic dGVzdF9zeW50aGV0aWNfa2V5Og==")
	now          = time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC)
)

func TestManifest(t *testing.T) {
	declarativetest.Run(t, declarativetest.Suite{
		Config:        config,
		BrokenConfig:  brokenConfig,
		Auth:          auth,
		Now:           now,
		MinPages:      2,
		RateLimitOnce: true,
	})
}

func TestCheckErrors(t *testing.T) {
	for _, tc := range []struct {
		name      string
		config    json.RawMessage
		overrides map[string]declarativetest.Fixture
		want      string
	}{
		{"401 authentication", brokenConfig, nil, "Sorry, authentication failed. Invalid api key"},
		{"403 permission", config, map[string]declarativetest.Fixture{"customers": {Status: 403, File: "errors/403.json"}}, "The API key does not have the permission to access this resource"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := declarativetest.New(t, declarativetest.Suite{Config: config, Auth: auth, Now: now, Overrides: tc.overrides})
			st := check(t, h, tc.config)
			if st.Status != synclet.ConnectionStatusFailed {
				t.Fatalf("check: %s", st.Status)
			}
			if !strings.Contains(st.Message, tc.want) {
				t.Errorf("check: message %q does not carry the API's %q", st.Message, tc.want)
			}
		})
	}
}

// requests records the query of every request and routes it by the fixture
// convention: a request with offset reads testdata/<path>/offset=<v>.json.
type requests struct {
	mu   sync.Mutex
	seen map[string][]url.Values
}

func (r *requests) route(path string, q url.Values) string {
	r.mu.Lock()
	r.seen[path] = append(r.seen[path], q)
	r.mu.Unlock()
	if v := q.Get("offset"); v != "" {
		return path + "/offset=" + url.QueryEscape(v) + ".json"
	}
	return path + ".json"
}

func TestPagesFollowNextOffset(t *testing.T) {
	reqs := &requests{seen: map[string][]url.Values{}}
	h := declarativetest.New(t, declarativetest.Suite{Config: config, Auth: auth, Now: now, Route: reqs.route})
	out := h.Read(t, h.Catalog(t, synclet.SyncModeFullRefresh), nil)
	if out.Err != nil {
		t.Fatalf("read: %v", out.Err)
	}
	for _, st := range h.Manifest.Streams {
		qs := reqs.seen[st.Name]
		if len(qs) != 2 {
			t.Errorf("%s: %d requests, want 2", st.Name, len(qs))
			continue
		}
		if qs[0].Has("offset") {
			t.Errorf("%s: first request carries offset %q", st.Name, qs[0].Get("offset"))
		}
		if got, want := qs[1].Get("offset"), `["1767400000000","2002"]`; got != want {
			t.Errorf("%s: second request offset %q, want the first page's next_offset %q", st.Name, got, want)
		}
		if got := qs[1].Get("limit"); got != "100" {
			t.Errorf("%s: second request limit %q, want 100", st.Name, got)
		}
		if got := ids(t, out.Records(synclet.StreamDescriptor{Name: st.Name})); len(got) != 3 {
			t.Errorf("%s: records %v, want both pages' 3", st.Name, got)
		}
	}
}

func TestIncrementalResume(t *testing.T) {
	reqs := &requests{seen: map[string][]url.Values{}}
	h := declarativetest.New(t, declarativetest.Suite{Config: config, Auth: auth, Now: now, Route: reqs.route})
	var state []synclet.StateMessage
	for _, st := range h.Manifest.Streams {
		state = append(state, synclet.StateMessage{
			Type:   synclet.StateTypeStream,
			Stream: &synclet.StreamState{StreamDescriptor: synclet.StreamDescriptor{Name: st.Name}, StreamState: json.RawMessage(`{"updated_at":1767400000}`)},
		})
	}
	raw, err := json.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	out := h.Read(t, h.Catalog(t, synclet.SyncModeIncremental), raw)
	if out.Err != nil {
		t.Fatalf("read: %v", out.Err)
	}
	for _, st := range h.Manifest.Streams {
		qs := reqs.seen[st.Name]
		if len(qs) == 0 {
			t.Errorf("%s: no request", st.Name)
			continue
		}
		for _, q := range qs {
			if got := q.Get("updated_at[after]"); got != "1767396400" {
				t.Errorf("%s: updated_at[after]=%q, want the saved 1767400000 minus the 1h lookback, 1767396400", st.Name, got)
			}
		}
		recs := out.Records(synclet.StreamDescriptor{Name: st.Name})
		got := ids(t, recs)
		if len(got) != 2 {
			t.Errorf("%s: resumed read emitted %v, want the 2 records updated at or after 1767396400", st.Name, got)
		}
		if want := wantDeleted[st.Name]; want != nil && !slices.ContainsFunc(recs, want(t)) {
			t.Errorf("%s: resumed read emitted no deleted object", st.Name)
		}
	}
}

// wantDeleted says how each stream's deleted objects arrive; items and
// item_prices lists return none.
var wantDeleted = map[string]func(*testing.T) func(synclet.RecordMessage) bool{
	"customers":     deleted,
	"subscriptions": deleted,
	"invoices":      deleted,
	"credit_notes":  deleted,
	"transactions":  deleted,
	"coupons":       statusDeleted,
}

func TestStreamParams(t *testing.T) {
	want := map[string]url.Values{
		"customers":     {"sort_by[asc]": {"updated_at"}, "include_deleted": {"true"}},
		"subscriptions": {"sort_by[asc]": {"updated_at"}, "include_deleted": {"true"}},
		"invoices":      {"sort_by[asc]": {"updated_at"}, "include_deleted": {"true"}},
		"credit_notes":  {"sort_by[asc]": {"date"}, "include_deleted": {"true"}},
		"transactions":  {"sort_by[asc]": {"updated_at"}, "include_deleted": {"true"}},
		"items":         {"sort_by[asc]": {"updated_at"}},
		"item_prices":   {"sort_by[asc]": {"updated_at"}},
		"coupons":       {"sort_by[asc]": {"created_at"}},
	}
	reqs := &requests{seen: map[string][]url.Values{}}
	h := declarativetest.New(t, declarativetest.Suite{Config: config, Auth: auth, Now: now, Route: reqs.route})
	out := h.Read(t, h.Catalog(t, synclet.SyncModeIncremental), nil)
	if out.Err != nil {
		t.Fatalf("read: %v", out.Err)
	}
	if len(want) != len(h.Manifest.Streams) {
		t.Fatalf("%d streams, want params for %d", len(h.Manifest.Streams), len(want))
	}
	for name, w := range want {
		w.Set("limit", "100")
		w.Set("updated_at[after]", "1767225600")
		qs := reqs.seen[name]
		if len(qs) == 0 {
			t.Errorf("%s: no request", name)
			continue
		}
		for _, q := range qs {
			q = maps.Clone(q)
			delete(q, "offset")
			if got := q.Encode(); got != w.Encode() {
				t.Errorf("%s: query %s, want %s", name, got, w.Encode())
			}
		}
	}
}

func check(t *testing.T, h *declarativetest.Harness, cfg json.RawMessage) *synclet.ConnectionStatus {
	t.Helper()
	out, err := testkit.RunSource(context.Background(), h.Source, synclet.Request{Command: synclet.CommandCheck, Config: cfg})
	if err != nil {
		t.Fatal(err)
	}
	st := out.ConnectionStatus()
	if st == nil {
		t.Fatal("check: no CONNECTION_STATUS")
	}
	return st
}

func ids(t *testing.T, recs []synclet.RecordMessage) []string {
	t.Helper()
	var out []string
	for _, r := range recs {
		var v struct {
			ID string `json:"id"`
		}
		if err := json.Unmarshal(r.Data, &v); err != nil {
			t.Fatal(err)
		}
		out = append(out, v.ID)
	}
	return out
}

func deleted(t *testing.T) func(synclet.RecordMessage) bool {
	return func(r synclet.RecordMessage) bool {
		var v struct {
			Deleted bool `json:"deleted"`
		}
		if err := json.Unmarshal(r.Data, &v); err != nil {
			t.Fatal(err)
		}
		return v.Deleted
	}
}

func statusDeleted(t *testing.T) func(synclet.RecordMessage) bool {
	return func(r synclet.RecordMessage) bool {
		var v struct {
			Status string `json:"status"`
		}
		if err := json.Unmarshal(r.Data, &v); err != nil {
			t.Fatal(err)
		}
		return v.Status == "deleted"
	}
}
