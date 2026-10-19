package sourcegithub

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
	config = json.RawMessage(`{"access_token":"test-token","repositories":["octo-example/api","octo-example/web"],"start_date":"2026-01-01T00:00:00Z"}`)
	broken = json.RawMessage(`{"access_token":"wrong-token","repositories":["octo-example/api","octo-example/web"],"start_date":"2026-01-01T00:00:00Z"}`)
	now    = time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC)
	auth   = declarativetest.Header("Authorization", "Bearer test-token")
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
		{"401 bad credentials", broken, nil, "Bad credentials"},
		{"403 missing permission", config, map[string]declarativetest.Fixture{
			"repos/octo-example/api": {Status: 403, File: "errors/403.json"},
		}, "Resource not accessible by personal access token"},
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

func TestEveryStreamReadsBothRepositories(t *testing.T) {
	h := declarativetest.New(t, declarativetest.Suite{Config: config, Auth: auth, Now: now})
	out := h.Read(t, h.Catalog(t, synclet.SyncModeFullRefresh), nil)
	if out.Err != nil {
		t.Fatalf("read: %v", out.Err)
	}
	for _, st := range h.Manifest.Streams {
		perRepo := map[string]int{}
		for _, r := range out.Records(synclet.StreamDescriptor{Name: st.Name}) {
			var rec struct {
				Repository string `json:"repository"`
			}
			if err := json.Unmarshal(r.Data, &rec); err != nil {
				t.Fatal(err)
			}
			perRepo[rec.Repository]++
		}
		want := map[string]int{"octo-example/api": 3, "octo-example/web": 3}
		if st.Name == "repositories" {
			want = map[string]int{"octo-example/api": 1, "octo-example/web": 1}
		}
		if !maps.Equal(perRepo, want) {
			t.Errorf("stream %s: records per repository %v, want %v", st.Name, perRepo, want)
		}
	}
}

func TestIncrementalResumesThroughSince(t *testing.T) {
	var (
		mu    sync.Mutex
		since map[string][]string
	)
	route := func(path string, q url.Values) string {
		mu.Lock()
		since[path] = append(since[path], q.Get("since"))
		mu.Unlock()
		if p := q.Get("page"); p != "" {
			return path + "/page=" + p + ".json"
		}
		return path + ".json"
	}
	h := declarativetest.New(t, declarativetest.Suite{Config: config, Auth: auth, Now: now, Route: route})
	cat := h.Catalog(t, synclet.SyncModeIncremental, "issues", "issue_comments", "commits")
	read := func(state json.RawMessage) *testkit.Output {
		since = map[string][]string{}
		out := h.Read(t, cat, state)
		if out.Err != nil {
			t.Fatalf("read: %v", out.Err)
		}
		return out
	}
	check := func(want map[string]string) {
		t.Helper()
		if got := slices.Sorted(maps.Keys(since)); !slices.Equal(got, slices.Sorted(maps.Keys(want))) {
			t.Fatalf("requested paths %v", got)
		}
		for path, values := range since {
			for _, v := range values {
				if v != want[path] {
					t.Errorf("%s: since=%q, want %q", path, v, want[path])
				}
			}
		}
	}

	first := read(nil)
	start := "2026-01-01T00:00:00Z"
	check(map[string]string{
		"repos/octo-example/api/issues": start, "repos/octo-example/web/issues": start,
		"repos/octo-example/api/issues/comments": start, "repos/octo-example/web/issues/comments": start,
		"repos/octo-example/api/commits": start, "repos/octo-example/web/commits": start,
	})

	read(first.NextState())
	check(map[string]string{
		"repos/octo-example/api/issues":          "2026-08-12T09:00:00Z",
		"repos/octo-example/web/issues":          "2026-09-12T09:00:00Z",
		"repos/octo-example/api/issues/comments": "2026-08-07T09:00:00Z",
		"repos/octo-example/web/issues/comments": "2026-08-08T09:00:00Z",
		"repos/octo-example/api/commits":         "2026-10-15T08:00:00Z",
		"repos/octo-example/web/commits":         "2026-09-15T08:00:00Z",
	})
}
