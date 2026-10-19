package sourcejira

import (
	"context"
	"encoding/json"
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

const (
	config       = `{"domain":"example.atlassian.net","email":"ada@example.com","api_token":"test-token","start_date":"2026-01-01T00:00:00Z"}`
	brokenConfig = `{"domain":"example.atlassian.net","email":"ada@example.com","api_token":"wrong-token","start_date":"2026-01-01T00:00:00Z"}`
	// base64("ada@example.com:test-token")
	basicAuth = "Basic YWRhQGV4YW1wbGUuY29tOnRlc3QtdG9rZW4="
)

func suite() declarativetest.Suite {
	return declarativetest.Suite{
		Config:        json.RawMessage(config),
		BrokenConfig:  json.RawMessage(brokenConfig),
		Auth:          declarativetest.Header("Authorization", basicAuth),
		Now:           time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC),
		MinPages:      2,
		RateLimitOnce: true,
	}
}

func TestManifest(t *testing.T) {
	declarativetest.Run(t, suite())
}

// recorder routes requests by the fixture convention and keeps every query.
type recorder struct {
	mu      sync.Mutex
	queries map[string][]url.Values
}

func (r *recorder) route(path string, q url.Values) string {
	r.mu.Lock()
	r.queries[path] = append(r.queries[path], q)
	r.mu.Unlock()
	var parts []string
	for _, k := range []string{"nextPageToken", "startAt"} {
		if v := q.Get(k); v != "" {
			parts = append(parts, k+"="+url.QueryEscape(v))
		}
	}
	if len(parts) == 0 {
		return path + ".json"
	}
	return path + "/" + strings.Join(parts, "&") + ".json"
}

func (r *recorder) values(path, key string) []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	var vs []string
	for _, q := range r.queries[path] {
		vs = append(vs, q.Get(key))
	}
	return vs
}

func recorded(t *testing.T, s declarativetest.Suite) (*declarativetest.Harness, *recorder) {
	t.Helper()
	rec := &recorder{queries: map[string][]url.Values{}}
	s.Route = rec.route
	return declarativetest.New(t, s), rec
}

func check(t *testing.T, h *declarativetest.Harness, cfg string) *synclet.ConnectionStatus {
	t.Helper()
	out, err := testkit.RunSource(context.Background(), h.Source, synclet.Request{Command: synclet.CommandCheck, Config: json.RawMessage(cfg)})
	if err != nil {
		t.Fatal(err)
	}
	st := out.ConnectionStatus()
	if st == nil {
		t.Fatal("check: no CONNECTION_STATUS")
	}
	return st
}

func TestCheckErrors(t *testing.T) {
	t.Run("401", func(t *testing.T) {
		h := declarativetest.New(t, suite())
		st := check(t, h, brokenConfig)
		if st.Status != synclet.ConnectionStatusFailed || !strings.Contains(st.Message, "Client must be authenticated to access this resource.") || !strings.Contains(st.Message, "401") || strings.Contains(st.Message, "errorMessages") {
			t.Errorf("check: %s %q", st.Status, st.Message)
		}
	})
	t.Run("403", func(t *testing.T) {
		s := suite()
		s.Overrides = map[string]declarativetest.Fixture{"api/3/project/search": {Status: 403, File: "errors/403.json"}}
		h := declarativetest.New(t, s)
		st := check(t, h, config)
		if st.Status != synclet.ConnectionStatusFailed || !strings.Contains(st.Message, "You do not have permission to browse projects on this site.") || !strings.Contains(st.Message, "403") || strings.Contains(st.Message, "errorMessages") {
			t.Errorf("check: %s %q", st.Status, st.Message)
		}
	})
}

func TestPages(t *testing.T) {
	h, rec := recorded(t, suite())
	out := h.Read(t, h.Catalog(t, synclet.SyncModeFullRefresh), nil)
	if out.Err != nil {
		t.Fatal(out.Err)
	}
	cases := []struct {
		stream, path, param string
		want                []string
	}{
		{"projects", "api/3/project/search", "startAt", []string{"0", "2"}},
		{"users", "api/3/users/search", "startAt", []string{"0", "2", "3"}},
		{"issue_fields", "api/3/field/search", "startAt", []string{"0", "2"}},
		{"boards", "agile/1.0/board", "startAt", []string{"0", "2"}},
		{"issues", "api/3/search/jql", "nextPageToken", []string{"", "tok-page-2"}},
	}
	for _, c := range cases {
		if got := rec.values(c.path, c.param); !slices.Equal(got, c.want) {
			t.Errorf("%s: %s sent %q, want %q", c.stream, c.param, got, c.want)
		}
		if got := len(out.Records(synclet.StreamDescriptor{Name: c.stream})); got != 3 {
			t.Errorf("%s: %d records, want 3 across the pages", c.stream, got)
		}
	}
}

func TestIssuesResume(t *testing.T) {
	h, rec := recorded(t, suite())
	state, err := json.Marshal([]synclet.StateMessage{{
		Type: synclet.StateTypeStream,
		Stream: &synclet.StreamState{
			StreamDescriptor: synclet.StreamDescriptor{Name: "issues"},
			StreamState:      json.RawMessage(`{"updated":"2026-03-02T05:30:00.000+0200"}`),
		},
	}})
	if err != nil {
		t.Fatal(err)
	}
	out := h.Read(t, h.Catalog(t, synclet.SyncModeIncremental, "issues"), state)
	if out.Err != nil {
		t.Fatal(out.Err)
	}
	// saved 03:30Z minus the 1d lookback
	want := `updated >= "2026-03-01 03:30" ORDER BY updated ASC`
	for _, jql := range rec.values("api/3/search/jql", "jql") {
		if jql != want {
			t.Errorf("jql %q, want %q", jql, want)
		}
	}
	var keys []string
	for _, r := range out.Records(synclet.StreamDescriptor{Name: "issues"}) {
		var rec struct {
			Key string `json:"key"`
		}
		if err := json.Unmarshal(r.Data, &rec); err != nil {
			t.Fatal(err)
		}
		keys = append(keys, rec.Key)
	}
	// ALPHA-1 (2026-02-11T08:00Z) is older than the bound; BETA-1 (04:30Z) and ALPHA-2 (08:15:30Z) are not
	if want := []string{"BETA-1", "ALPHA-2"}; !slices.Equal(keys, want) {
		t.Errorf("issues %q, want %q", keys, want)
	}
	next := out.NextState()
	if !strings.Contains(string(next), `"updated":"2026-03-02T10:15:30.000+0200"`) {
		t.Errorf("state %s", next)
	}
}
