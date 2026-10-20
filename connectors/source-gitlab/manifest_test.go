package sourcegitlab

import (
	"context"
	"encoding/json"
	"fmt"
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
	config = json.RawMessage(`{"access_token":"glpat-test-token","api_url":"gitlab.example.com","projects":["101","202"],"groups":["11"],"start_date":"2026-01-01T00:00:00Z"}`)
	broken = json.RawMessage(`{"access_token":"glpat-wrong-token","api_url":"gitlab.example.com","projects":["101","202"],"groups":["11"],"start_date":"2026-01-01T00:00:00Z"}`)
	now    = time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC)
	auth   = declarativetest.Header("Authorization", "Bearer glpat-test-token")
)

func TestManifest(t *testing.T) {
	declarativetest.Run(t, declarativetest.Suite{
		Config:        config,
		BrokenConfig:  broken,
		Auth:          auth,
		Now:           now,
		MinPages:      2,
		RateLimitOnce: true,
	})
}

func TestCheckFailures(t *testing.T) {
	tests := []struct {
		name      string
		config    json.RawMessage
		overrides map[string]declarativetest.Fixture
		want      string
	}{
		{"401 bad token", broken, nil, "401 Unauthorized"},
		{"403 missing scope", config, map[string]declarativetest.Fixture{
			"projects/101": {Status: 403, File: "errors/403.json"},
		}, "403 Forbidden"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := declarativetest.New(t, declarativetest.Suite{Config: config, Auth: auth, Now: now, Overrides: tt.overrides})
			out, err := testkit.RunSource(context.Background(), h.Source, synclet.Request{Command: synclet.CommandCheck, Config: tt.config})
			if err != nil {
				t.Fatal(err)
			}
			st := out.ConnectionStatus()
			if st == nil || st.Status != synclet.ConnectionStatusFailed || !strings.Contains(st.Message, tt.want) {
				t.Fatalf("check: got %+v, want FAILED with %q", st, tt.want)
			}
		})
	}
}

func TestEveryProjectStreamReadsBothProjects(t *testing.T) {
	h := declarativetest.New(t, declarativetest.Suite{Config: config, Auth: auth, Now: now})
	out := h.Read(t, h.Catalog(t, synclet.SyncModeFullRefresh), nil)
	if out.Err != nil {
		t.Fatalf("read: %v", out.Err)
	}
	want := map[string]map[string]int{
		"projects":           {"101": 1, "202": 1},
		"issues":             {"101": 3, "202": 1},
		"merge_requests":     {"101": 3, "202": 1},
		"pipelines":          {"101": 3, "202": 1},
		"commits":            {"101": 3, "202": 1},
		"jobs":               {"101": 3, "202": 1},
		"releases":           {"101": 3},
		"tags":               {"101": 3, "202": 1},
		"branches":           {"101": 3, "202": 1},
		"project_members":    {"101": 3, "202": 1},
		"project_milestones": {"101": 3},
		"project_labels":     {"101": 3, "202": 1},
	}
	for stream, perProject := range want {
		got := map[string]int{}
		for _, r := range out.Records(synclet.StreamDescriptor{Name: stream}) {
			var rec map[string]any
			if err := json.Unmarshal(r.Data, &rec); err != nil {
				t.Fatal(err)
			}
			key := "project_id"
			if stream == "projects" {
				key = "id"
			}
			got[fmt.Sprint(rec[key])]++
		}
		if !maps.Equal(got, perProject) {
			t.Errorf("stream %s: records per project %v, want %v", stream, got, perProject)
		}
	}
	for stream, n := range map[string]int{"groups": 1, "group_members": 3, "group_milestones": 3, "group_labels": 3, "users": 4} {
		if got := len(out.Records(synclet.StreamDescriptor{Name: stream})); got != n {
			t.Errorf("stream %s: %d records, want %d", stream, got, n)
		}
	}
}

func TestIncrementalResumesThroughFilters(t *testing.T) {
	filters := map[string]string{"issues": "updated_after", "merge_requests": "updated_after", "pipelines": "updated_after", "commits": "since"}
	paths := map[string]string{"issues": "issues", "merge_requests": "merge_requests", "pipelines": "pipelines", "commits": "repository/commits"}
	var (
		mu   sync.Mutex
		sent map[string][]string
	)
	route := func(path string, q url.Values) string {
		for stream, suffix := range paths {
			if strings.HasSuffix(path, "/"+suffix) {
				mu.Lock()
				sent[path] = append(sent[path], q.Get(filters[stream]))
				mu.Unlock()
				if stream != "commits" && (q.Get("order_by") != "updated_at" || q.Get("sort") != "asc") {
					t.Errorf("%s: order_by=%q sort=%q", path, q.Get("order_by"), q.Get("sort"))
				}
			}
		}
		if p := q.Get("page"); p != "" {
			return path + "/page=" + p + ".json"
		}
		return path + ".json"
	}
	h := declarativetest.New(t, declarativetest.Suite{Config: config, Auth: auth, Now: now, Route: route})
	cat := h.Catalog(t, synclet.SyncModeIncremental, "issues", "merge_requests", "pipelines", "commits")
	read := func(state json.RawMessage) *testkit.Output {
		sent = map[string][]string{}
		out := h.Read(t, cat, state)
		if out.Err != nil {
			t.Fatalf("read: %v", out.Err)
		}
		return out
	}
	check := func(want map[string]string) {
		t.Helper()
		if got := slices.Sorted(maps.Keys(sent)); !slices.Equal(got, slices.Sorted(maps.Keys(want))) {
			t.Fatalf("requested paths %v", got)
		}
		for path, values := range sent {
			for _, v := range values {
				if v != want[path] {
					t.Errorf("%s: %q, want %q", path, v, want[path])
				}
			}
		}
	}
	first := read(nil)
	start := "2026-01-01T00:00:00Z"
	all := map[string]string{}
	for _, p := range []string{"101", "202"} {
		for _, s := range paths {
			all["projects/"+p+"/"+s] = start
		}
	}
	check(all)

	read(first.NextState())
	check(map[string]string{
		"projects/101/issues":             "2026-09-01T09:00:00Z",
		"projects/202/issues":             "2026-07-01T09:00:00Z",
		"projects/101/merge_requests":     "2026-09-02T09:00:00Z",
		"projects/202/merge_requests":     "2026-07-02T09:00:00Z",
		"projects/101/pipelines":          "2026-09-03T09:00:00Z",
		"projects/202/pipelines":          "2026-07-03T09:00:00Z",
		"projects/101/repository/commits": "2026-09-05T09:00:00Z",
		"projects/202/repository/commits": "2026-07-05T11:00:00Z",
	})
}
