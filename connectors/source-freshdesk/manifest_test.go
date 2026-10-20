package sourcefreshdesk

import (
	"context"
	"encoding/base64"
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
	config = json.RawMessage(`{"domain":"synthetic.freshdesk.com","api_key":"fd-test-key","start_date":"2026-01-01T00:00:00Z"}`)
	broken = json.RawMessage(`{"domain":"synthetic.freshdesk.com","api_key":"fd-wrong-key","start_date":"2026-01-01T00:00:00Z"}`)
	now    = time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC)
	auth   = declarativetest.Header("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte("fd-test-key:X")))
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
		{"401 bad key", broken, nil, "You have to be logged in"},
		{"403 no access", config, map[string]declarativetest.Fixture{
			"tickets": {Status: 403, File: "errors/403.json"},
		}, "You are not authorized"},
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

func TestConversationsPerTicket(t *testing.T) {
	h := declarativetest.New(t, declarativetest.Suite{Config: config, Auth: auth, Now: now})
	out := h.Read(t, h.Catalog(t, synclet.SyncModeFullRefresh, "conversations"), nil)
	if out.Err != nil {
		t.Fatalf("read: %v", out.Err)
	}
	got := map[int]int{}
	for _, r := range out.Records(synclet.StreamDescriptor{Name: "conversations"}) {
		var c struct {
			TicketID int `json:"ticket_id"`
		}
		if err := json.Unmarshal(r.Data, &c); err != nil {
			t.Fatal(err)
		}
		got[c.TicketID]++
	}
	if want := map[int]int{1001: 3, 1002: 1}; !maps.Equal(got, want) {
		t.Errorf("conversations per ticket %v, want %v", got, want)
	}
}

func TestIncrementalResumesThroughSince(t *testing.T) {
	filters := map[string]string{"tickets": "updated_since", "contacts": "_updated_since", "surveys/satisfaction_ratings": "created_since"}
	var (
		mu   sync.Mutex
		sent map[string][]string
	)
	route := func(path string, q url.Values) string {
		if p, ok := filters[path]; ok {
			mu.Lock()
			sent[path] = append(sent[path], q.Get(p))
			mu.Unlock()
			if path == "tickets" && (q.Get("order_by") != "updated_at" || q.Get("order_type") != "asc") {
				t.Errorf("tickets order %q %q", q.Get("order_by"), q.Get("order_type"))
			}
		}
		if p := q.Get("page"); p != "" {
			return path + "/page=" + p + ".json"
		}
		return path + ".json"
	}
	h := declarativetest.New(t, declarativetest.Suite{Config: config, Auth: auth, Now: now, Route: route})
	cat := h.Catalog(t, synclet.SyncModeIncremental, "tickets", "contacts", "satisfaction_ratings")
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
		for path, v := range want {
			if len(sent[path]) == 0 {
				t.Errorf("%s: no request", path)
			}
			for _, got := range sent[path] {
				if got != v {
					t.Errorf("%s: %s=%q, want %q", path, filters[path], got, v)
				}
			}
		}
	}
	first := read(nil)
	start := "2026-01-01T00:00:00Z"
	check(map[string]string{"tickets": start, "contacts": start, "surveys/satisfaction_ratings": start})
	read(first.NextState())
	check(map[string]string{
		"tickets":                      "2026-10-01T09:00:00Z",
		"contacts":                     "2026-09-19T23:00:00Z",
		"surveys/satisfaction_ratings": "2026-10-02T23:00:00Z",
	})
}
