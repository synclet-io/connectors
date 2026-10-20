package sourceslack

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
	config = json.RawMessage(`{"bot_token":"xoxb-test-token","start_date":"2026-01-01T00:00:00Z","channel_types":"public_channel,private_channel"}`)
	broken = json.RawMessage(`{"bot_token":"xoxb-wrong-token","start_date":"2026-01-01T00:00:00Z"}`)
	now    = time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC)
	auth   = declarativetest.Header("Authorization", "Bearer xoxb-test-token")
)

// route answers testdata/<method>.json, or testdata/<method>/<k=v&k=v>.json
// with the channel and the pagination cursor of the request.
func route(path string, q url.Values) string {
	var parts []string
	for _, k := range []string{"channel", "cursor"} {
		if v := q.Get(k); v != "" {
			parts = append(parts, k+"="+url.QueryEscape(v))
		}
	}
	if len(parts) == 0 {
		return path + ".json"
	}
	return path + "/" + strings.Join(parts, "&") + ".json"
}

func TestManifest(t *testing.T) {
	declarativetest.Run(t, declarativetest.Suite{
		Config:        config,
		BrokenConfig:  broken,
		Auth:          auth,
		Now:           now,
		MinPages:      2,
		RateLimitOnce: true,
		Route:         route,
	})
}

func TestCheckFailures(t *testing.T) {
	tests := []struct {
		name      string
		config    json.RawMessage
		overrides map[string]declarativetest.Fixture
		want      string
	}{
		{"401 invalid auth", broken, nil, "invalid_auth"},
		{"403 missing scope", config, map[string]declarativetest.Fixture{
			"users.list": {Status: 403, File: "errors/missing_scope.json"},
		}, "missing_scope"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := declarativetest.New(t, declarativetest.Suite{Config: config, Auth: auth, Now: now, Route: route, Overrides: tt.overrides})
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

// Slack answers most errors with HTTP 200 and "ok": false; check passes on
// the status alone, the read fails on the missing records.
func TestReadFailsOnOkFalse(t *testing.T) {
	h := declarativetest.New(t, declarativetest.Suite{Config: config, Auth: auth, Now: now, Route: route, Overrides: map[string]declarativetest.Fixture{
		"users.list": {Status: 200, File: "errors/missing_scope.json"},
	}})
	out := h.Read(t, h.Catalog(t, synclet.SyncModeFullRefresh, "users"), nil)
	if out.Err == nil || !strings.Contains(out.Err.Error(), "records path $.members is not in the response") {
		t.Fatalf("read: %v, want the missing records path", out.Err)
	}
}

func TestMessagesPerChannel(t *testing.T) {
	var (
		mu    sync.Mutex
		types []string
	)
	r := func(path string, q url.Values) string {
		if path == "users.conversations" {
			mu.Lock()
			types = append(types, q.Get("types"))
			mu.Unlock()
		}
		return route(path, q)
	}
	h := declarativetest.New(t, declarativetest.Suite{Config: config, Auth: auth, Now: now, Route: r})
	out := h.Read(t, h.Catalog(t, synclet.SyncModeFullRefresh, "channel_messages"), nil)
	if out.Err != nil {
		t.Fatalf("read: %v", out.Err)
	}
	got := map[string][]string{}
	for _, rec := range out.Records(synclet.StreamDescriptor{Name: "channel_messages"}) {
		var m struct {
			ChannelID string `json:"channel_id"`
			TS        string `json:"ts"`
		}
		if err := json.Unmarshal(rec.Data, &m); err != nil {
			t.Fatal(err)
		}
		got[m.ChannelID] = append(got[m.ChannelID], m.TS)
	}
	want := map[string][]string{
		"C0SYN0001": {"1791000000.000200", "1790000000.000100", "1789000000.000100"},
		"C0SYN0002": {"1792000000.000300"},
	}
	if !maps.EqualFunc(got, want, slices.Equal) {
		t.Errorf("messages per channel %v, want %v", got, want)
	}
	if len(out.Records(synclet.StreamDescriptor{Name: "channels"})) != 0 {
		t.Error("channels emitted although not in the catalog")
	}
	for _, v := range types {
		if v != "public_channel,private_channel" {
			t.Errorf("users.conversations types=%q", v)
		}
	}
}

func TestMessagesResumeThroughOldest(t *testing.T) {
	var (
		mu     sync.Mutex
		oldest map[string]string
	)
	r := func(path string, q url.Values) string {
		if path == "conversations.history" {
			mu.Lock()
			if _, ok := oldest[q.Get("channel")]; !ok {
				oldest[q.Get("channel")] = q.Get("oldest")
			}
			mu.Unlock()
			if q.Get("inclusive") != "true" {
				t.Errorf("conversations.history inclusive=%q", q.Get("inclusive"))
			}
		}
		return route(path, q)
	}
	h := declarativetest.New(t, declarativetest.Suite{Config: config, Auth: auth, Now: now, Route: r})
	cat := h.Catalog(t, synclet.SyncModeIncremental, "channels", "channel_messages")
	read := func(state json.RawMessage) *testkit.Output {
		oldest = map[string]string{}
		out := h.Read(t, cat, state)
		if out.Err != nil {
			t.Fatalf("read: %v", out.Err)
		}
		return out
	}
	check := func(want string) {
		t.Helper()
		if got := slices.Sorted(maps.Keys(oldest)); !slices.Equal(got, []string{"C0SYN0001", "C0SYN0002", "C0SYN0003"}) {
			t.Fatalf("channels read %v", got)
		}
		for ch, v := range oldest {
			if v != want {
				t.Errorf("%s: oldest=%q, want %q", ch, v, want)
			}
		}
	}
	first := read(nil)
	check("1767225600")
	// Newest message 1792000000 minus the 7d lookback.
	read(first.NextState())
	check("1791395200")
}
