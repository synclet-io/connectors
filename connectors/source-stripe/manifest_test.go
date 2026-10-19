package sourcestripe

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	synclet "github.com/synclet-io/synclet-go-sdk"
	"github.com/synclet-io/synclet-go-sdk/testkit"

	"source-declarative/declarativetest"
)

const (
	startUnix = 1767225600 // 2026-01-01T00:00:00Z, the start_date of config
	lookback  = 3600       // incremental.lookback of every stream, in seconds
)

var (
	config       = json.RawMessage(`{"api_key":"sk_test_example","start_date":"2026-01-01T00:00:00Z"}`)
	brokenConfig = json.RawMessage(`{"api_key":"wrong-key","start_date":"2026-01-01T00:00:00Z"}`)
)

var streams = []string{
	"customers", "charges", "invoices", "subscriptions", "payment_intents",
	"refunds", "products", "prices", "balance_transactions", "events",
}

func suite() declarativetest.Suite {
	return declarativetest.Suite{
		Config:       config,
		BrokenConfig: brokenConfig,
		Auth:         declarativetest.Header("Authorization", "Bearer sk_test_example"),
		Headers:      map[string]string{"Stripe-Version": "2025-08-27.basil"},
		Now:          time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC),
		MinPages:     2,
	}
}

func TestManifest(t *testing.T) {
	s := suite()
	s.RateLimitOnce = true
	declarativetest.Run(t, s)
}

func TestCheckFailures(t *testing.T) {
	for _, tc := range []struct {
		name    string
		config  json.RawMessage
		over    map[string]declarativetest.Fixture
		status  int
		message string
	}{
		{
			name:    "unauthenticated",
			config:  brokenConfig,
			status:  http.StatusUnauthorized,
			message: "Invalid API Key provided: *****-key",
		},
		{
			name:    "missing permission",
			config:  config,
			over:    map[string]declarativetest.Fixture{"customers": {Status: http.StatusForbidden, File: "errors/403.json"}},
			status:  http.StatusForbidden,
			message: "The provided key 'rk_test_example' does not have the required permissions for this endpoint on account 'acct_test1'.",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := suite()
			s.Overrides = tc.over
			h := declarativetest.New(t, s)
			out, err := testkit.RunSource(context.Background(), h.Source, synclet.Request{Command: synclet.CommandCheck, Config: tc.config})
			if err != nil {
				t.Fatal(err)
			}
			st := out.ConnectionStatus()
			if st == nil || st.Status != synclet.ConnectionStatusFailed {
				t.Fatalf("check: %+v, want FAILED", st)
			}
			want := "(HTTP " + strconv.Itoa(tc.status) + "): " + tc.message
			if !strings.Contains(st.Message, want) {
				t.Errorf("check message %q does not end with %q", st.Message, want)
			}
		})
	}
}

// recorder routes requests to fixtures as declarativetest does and keeps
// every request's query per path.
type recorder struct {
	mu   sync.Mutex
	reqs map[string][]url.Values
}

func (r *recorder) route(path string, q url.Values) string {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.reqs[path] = append(r.reqs[path], q)
	if after := q.Get("starting_after"); after != "" {
		return path + "/starting_after=" + url.QueryEscape(after) + ".json"
	}
	return path + ".json"
}

func (r *recorder) take(path string) []url.Values {
	r.mu.Lock()
	defer r.mu.Unlock()
	qs := r.reqs[path]
	delete(r.reqs, path)
	return qs
}

func newRecorded(t *testing.T) (*declarativetest.Harness, *recorder) {
	t.Helper()
	rec := &recorder{reqs: map[string][]url.Values{}}
	s := suite()
	s.Route = rec.route
	return declarativetest.New(t, s), rec
}

type page struct {
	Data []struct {
		ID      string `json:"id"`
		Created int64  `json:"created"`
	} `json:"data"`
	HasMore bool `json:"has_more"`
}

func readPage(t *testing.T, file string) page {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("testdata", file))
	if err != nil {
		t.Fatal(err)
	}
	var p page
	if err := json.Unmarshal(b, &p); err != nil {
		t.Fatalf("%s: %v", file, err)
	}
	return p
}

func recordIDs(t *testing.T, out *testkit.Output, stream string) []string {
	t.Helper()
	var ids []string
	for _, r := range out.Records(synclet.StreamDescriptor{Name: stream}) {
		var rec struct {
			ID string `json:"id"`
		}
		if err := json.Unmarshal(r.Data, &rec); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, rec.ID)
	}
	return ids
}

func TestStreamsPageByStartingAfter(t *testing.T) {
	h, rec := newRecorded(t)
	if got := len(h.Manifest.Streams); got != len(streams) {
		t.Fatalf("manifest has %d streams, want %d", got, len(streams))
	}
	for _, name := range streams {
		t.Run(name, func(t *testing.T) {
			out := h.Read(t, h.Catalog(t, synclet.SyncModeFullRefresh, name), nil)
			if out.Err != nil {
				t.Fatalf("read: %v", out.Err)
			}
			first := readPage(t, name+".json")
			if !first.HasMore || len(first.Data) == 0 {
				t.Fatalf("%s.json: the first page must have records and has_more", name)
			}
			last := first.Data[len(first.Data)-1].ID
			second := readPage(t, name+"/starting_after="+last+".json")

			qs := rec.take(name)
			if len(qs) != 2 {
				t.Fatalf("%d requests, want 2: %v", len(qs), qs)
			}
			if qs[0].Has("starting_after") {
				t.Errorf("first request carries starting_after: %v", qs[0])
			}
			if got := qs[1].Get("starting_after"); got != last {
				t.Errorf("second request starting_after=%q, want the first page's last id %q", got, last)
			}
			if name == "subscriptions" {
				for _, q := range qs {
					if got := q.Get("status"); got != "all" {
						t.Errorf("subscriptions request status=%q, want all: canceled subscriptions are left out otherwise", got)
					}
				}
			}
			var want []string
			for _, p := range [][]string{ids(first), ids(second)} {
				want = append(want, p...)
			}
			if got := recordIDs(t, out, name); !slices.Equal(got, want) {
				t.Errorf("records %v, want %v", got, want)
			}
		})
	}
}

func ids(p page) []string {
	out := make([]string, len(p.Data))
	for i, d := range p.Data {
		out[i] = d.ID
	}
	return out
}

func TestIncrementalResumesFromCreatedMinusLookback(t *testing.T) {
	h, rec := newRecorded(t)
	for _, name := range streams {
		t.Run(name, func(t *testing.T) {
			cat := h.Catalog(t, synclet.SyncModeIncremental, name)
			first := h.Read(t, cat, nil)
			if first.Err != nil {
				t.Fatalf("first read: %v", first.Err)
			}
			for _, q := range rec.take(name) {
				if got := q.Get("created[gte]"); got != strconv.Itoa(startUnix) {
					t.Errorf("first read: created[gte]=%q, want the start date %d", got, startUnix)
				}
			}
			saved := savedCreated(t, first.NextState(), name)
			second := h.Read(t, cat, first.NextState())
			if second.Err != nil {
				t.Fatalf("second read: %v", second.Err)
			}
			qs := rec.take(name)
			if len(qs) == 0 {
				t.Fatal("second read sent no request")
			}
			want := strconv.FormatInt(saved-lookback, 10)
			for _, q := range qs {
				if got := q.Get("created[gte]"); got != want {
					t.Errorf("second read: created[gte]=%q, want saved created %d minus %ds = %s", got, saved, lookback, want)
				}
			}
			for _, r := range second.Records(synclet.StreamDescriptor{Name: name}) {
				var c struct {
					Created int64 `json:"created"`
				}
				if err := json.Unmarshal(r.Data, &c); err != nil {
					t.Fatal(err)
				}
				if c.Created < saved-lookback {
					t.Errorf("second read emitted a record created %d, before %s", c.Created, want)
				}
			}
		})
	}
}

func savedCreated(t *testing.T, state json.RawMessage, stream string) int64 {
	t.Helper()
	var msgs []synclet.StateMessage
	if err := json.Unmarshal(state, &msgs); err != nil {
		t.Fatalf("state: %v", err)
	}
	for _, m := range msgs {
		if m.Stream == nil || m.Stream.StreamDescriptor.Name != stream {
			continue
		}
		var s struct {
			Created *int64 `json:"created"`
		}
		if err := json.Unmarshal(m.Stream.StreamState, &s); err != nil || s.Created == nil {
			t.Fatalf("state of %s: %s, want {\"created\": <unix seconds>}", stream, m.Stream.StreamState)
		}
		return *s.Created
	}
	t.Fatalf("no state for %s in %s", stream, state)
	return 0
}

func TestEventsCarryUpdates(t *testing.T) {
	h, _ := newRecorded(t)
	var events *synclet.Stream
	for _, st := range h.Catalog(t, synclet.SyncModeIncremental, "events").Streams {
		events = &st.Stream
	}
	if events == nil {
		t.Fatal("no incremental events stream")
	}
	if !slices.Equal(events.DefaultCursorField, []string{"created"}) {
		t.Errorf("events cursor %v, want [created]", events.DefaultCursorField)
	}

	out := h.Read(t, h.Catalog(t, synclet.SyncModeFullRefresh), nil)
	if out.Err != nil {
		t.Fatalf("read: %v", out.Err)
	}
	objects := map[string]string{}
	for _, name := range streams {
		for _, id := range recordIDs(t, out, name) {
			objects[id] = name
		}
	}
	updated := map[string]bool{}
	for _, r := range out.Records(synclet.StreamDescriptor{Name: "events"}) {
		var e struct {
			Type string `json:"type"`
			Data struct {
				Object struct {
					ID string `json:"id"`
				} `json:"object"`
				PreviousAttributes map[string]any `json:"previous_attributes"`
			} `json:"data"`
		}
		if err := json.Unmarshal(r.Data, &e); err != nil {
			t.Fatal(err)
		}
		if !strings.HasSuffix(e.Type, ".updated") {
			continue
		}
		stream, ok := objects[e.Data.Object.ID]
		if !ok || len(e.Data.PreviousAttributes) == 0 {
			t.Errorf("%s event for %q: not an update of a synced object", e.Type, e.Data.Object.ID)
			continue
		}
		updated[stream] = true
	}
	if !updated["customers"] || !updated["invoices"] {
		t.Errorf("events carry updates of %v, want customers and invoices", updated)
	}

	readme, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(readme, []byte("catch new objects only")) {
		t.Error("README does not say that object streams catch new objects only")
	}
}
