package sourcetypeform

import (
	"context"
	"encoding/json"
	"maps"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	synclet "github.com/synclet-io/synclet-go-sdk"
	"github.com/synclet-io/synclet-go-sdk/testkit"

	"source-declarative/declarativetest"
)

var (
	config = json.RawMessage(`{"access_token":"tfp_test_token","start_date":"2026-01-01T00:00:00Z"}`)
	broken = json.RawMessage(`{"access_token":"tfp_wrong_token","start_date":"2026-01-01T00:00:00Z"}`)
	now    = time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC)
	auth   = declarativetest.Header("Authorization", "Bearer tfp_test_token")
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
		{"401 bad token", broken, nil, "Authentication credentials not found"},
		{"403 missing scope", config, map[string]declarativetest.Fixture{
			"workspaces": {Status: 403, File: "errors/403.json"},
		}, "does not have the required scope"},
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

func TestChildrenReadEveryForm(t *testing.T) {
	h := declarativetest.New(t, declarativetest.Suite{Config: config, Auth: auth, Now: now})
	out := h.Read(t, h.Catalog(t, synclet.SyncModeFullRefresh, "forms", "responses", "webhooks"), nil)
	if out.Err != nil {
		t.Fatalf("read: %v", out.Err)
	}
	for _, r := range out.Records(synclet.StreamDescriptor{Name: "forms"}) {
		var f struct {
			ReadAt string `json:"_read_at"`
		}
		if err := json.Unmarshal(r.Data, &f); err != nil {
			t.Fatal(err)
		}
		if f.ReadAt != "2026-10-18T12:00:00Z" {
			t.Errorf("form _read_at %q, want the read's time", f.ReadAt)
		}
	}
	count := func(stream string) map[string]int {
		got := map[string]int{}
		for _, r := range out.Records(synclet.StreamDescriptor{Name: stream}) {
			var rec struct {
				FormID string `json:"form_id"`
			}
			if err := json.Unmarshal(r.Data, &rec); err != nil {
				t.Fatal(err)
			}
			got[rec.FormID]++
		}
		return got
	}
	if got, want := count("responses"), map[string]int{"frmAAAA1": 3, "frmBBBB2": 1}; !maps.Equal(got, want) {
		t.Errorf("responses per form %v, want %v", got, want)
	}
	if got, want := count("webhooks"), map[string]int{"frmAAAA1": 2, "frmBBBB2": 1}; !maps.Equal(got, want) {
		t.Errorf("webhooks per form %v, want %v", got, want)
	}
}

func TestResponsesResumeThroughSince(t *testing.T) {
	var (
		mu    sync.Mutex
		since map[string]string
	)
	route := func(path string, q url.Values) string {
		if strings.HasSuffix(path, "/responses") {
			mu.Lock()
			if _, ok := since[path]; !ok {
				since[path] = q.Get("since")
			}
			mu.Unlock()
		}
		if p := q.Get("page"); p != "" {
			return path + "/page=" + p + ".json"
		}
		if b := q.Get("before"); b != "" {
			return path + "/before=" + b + ".json"
		}
		return path + ".json"
	}
	h := declarativetest.New(t, declarativetest.Suite{Config: config, Auth: auth, Now: now, Route: route})
	cat := h.Catalog(t, synclet.SyncModeIncremental, "forms", "responses")
	read := func(state json.RawMessage) *testkit.Output {
		since = map[string]string{}
		out := h.Read(t, cat, state)
		if out.Err != nil {
			t.Fatalf("read: %v", out.Err)
		}
		return out
	}
	first := read(nil)
	for _, form := range []string{"frmAAAA1", "frmBBBB2", "frmCCCC3"} {
		if got := since["forms/"+form+"/responses"]; got != "2026-01-01T00:00:00Z" {
			t.Errorf("first read of %s: since=%q, want the start date", form, got)
		}
	}
	read(first.NextState())
	// The pass starts at the older of the forms cursor (the first read's
	// time) and the responses cursor (the newest submission), minus 1h.
	for _, form := range []string{"frmAAAA1", "frmBBBB2", "frmCCCC3"} {
		if got := since["forms/"+form+"/responses"]; got != "2026-10-01T08:30:05Z" {
			t.Errorf("resumed read of %s: since=%q, want 2026-10-01T08:30:05Z", form, got)
		}
	}
}
