// Package declarativetest is the test entry of a manifest connector: it
// loads the connector's manifest, serves its testdata/ fixtures from an
// httptest server and runs the SDK's conformance suite against the engine.
package declarativetest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
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

	"source-declarative/internal/engine"
	"source-declarative/internal/manifest"
)

// Auth is the credential every fixture request must carry.
type Auth struct {
	header, query, value string
}

// Header requires header name to be value.
func Header(name, value string) Auth { return Auth{header: name, value: value} }

// Query requires query parameter name to be value.
func Query(name, value string) Auth { return Auth{query: name, value: value} }

func (a Auth) ok(r *http.Request) bool {
	if a.header != "" {
		return r.Header.Get(a.header) == a.value
	}
	if a.query != "" {
		return r.URL.Query().Get(a.query) == a.value
	}
	return true
}

// Fixture replaces the response to one route: Status with the body of
// File, relative to the fixture directory.
type Fixture struct {
	Status int
	File   string
}

// Suite is what Run needs from a manifest connector's test.
type Suite struct {
	// Dir is the connector directory; default ".".
	Dir string
	// Config passes check and reads every stream; BrokenConfig fails check
	// with the text of the unauthorized fixture.
	Config, BrokenConfig json.RawMessage
	// Auth is the credential a request must carry; without it the request
	// gets the unauthorized fixture with HTTP 401.
	Auth Auth
	// Headers are headers every request must carry with these values.
	Headers map[string]string
	// Now is the engine's clock.
	Now time.Time
	// MinPages is the least number of pages the fixtures of every paginated
	// stream span.
	MinPages int
	// Overrides replaces the response of a route: the request path under
	// the base URL, such as "charges".
	Overrides map[string]Fixture
	// RateLimitOnce answers the first request of one more read with 429 and
	// Retry-After: 0; that read must still succeed.
	RateLimitOnce bool

	// Fixtures is the fixture directory; default <Dir>/testdata.
	Fixtures string
	// BasePath is the path of the base URL on the fixture server; default /.
	BasePath string
	// Route maps a request (its path under BasePath and its query) to a
	// fixture file; default the convention of the README.
	Route func(path string, query url.Values) string
	// Inspect, when set, sees every request the fixture server receives.
	Inspect func(r *http.Request)
	// Unauthorized is the fixture of a request without the credential;
	// default errors/401.json.
	Unauthorized string
}

func (s Suite) withDefaults() Suite {
	if s.Dir == "" {
		s.Dir = "."
	}
	if s.Fixtures == "" {
		s.Fixtures = filepath.Join(s.Dir, "testdata")
	}
	if s.BasePath == "" {
		s.BasePath = "/"
	}
	if s.Unauthorized == "" {
		s.Unauthorized = "errors/401.json"
	}
	if s.Now.IsZero() {
		s.Now = time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC)
	}
	return s
}

// Harness is a loaded manifest connector on a fixture server.
type Harness struct {
	Manifest *manifest.Manifest
	Source   synclet.Source[json.RawMessage]

	suite     Suite
	server    string
	mu        sync.Mutex
	pages     map[string]int
	rateLimit bool
}

// New loads the manifest of s.Dir, checks its name against the directory
// and starts its fixture server.
func New(t testing.TB, s Suite) *Harness {
	t.Helper()
	s = s.withDefaults()
	m, err := manifest.Load(s.Dir)
	if err != nil {
		t.Fatalf("manifest of %s:\n%v", s.Dir, err)
	}
	abs, err := filepath.Abs(s.Dir)
	if err != nil {
		t.Fatal(err)
	}
	if base := filepath.Base(abs); base != m.Connector.Name {
		t.Fatalf("connector.name %q does not match its directory %q", m.Connector.Name, base)
	}
	h := &Harness{Manifest: m, suite: s, pages: map[string]int{}}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { h.serve(t, w, r) }))
	t.Cleanup(srv.Close)
	h.server = srv.URL + s.BasePath
	h.Source = engine.New(m, engine.Options{
		BaseURL: srv.URL + s.BasePath,
		Client:  srv.Client(),
		Now:     func() time.Time { return s.Now },
		Sleep:   func(ctx context.Context, _ time.Duration) error { return ctx.Err() },
		OnRequest: func(stream string, page int) {
			h.mu.Lock()
			defer h.mu.Unlock()
			h.pages[stream] = max(h.pages[stream], page)
		},
	})
	return h
}

func (h *Harness) serve(t testing.TB, w http.ResponseWriter, r *http.Request) {
	s := h.suite
	if s.Inspect != nil {
		s.Inspect(r)
	}
	if s.Auth.header != "" && s.Auth.value != "" && strings.Contains(r.URL.RawQuery, url.QueryEscape(s.Auth.value)) {
		t.Errorf("request %s carries the credential in its URL", r.URL.Path)
	}
	for k, v := range s.Headers {
		if got := r.Header.Get(k); got != v {
			t.Errorf("request %s: header %s=%q, want %q", r.URL.Path, k, got, v)
		}
	}
	h.mu.Lock()
	limited := h.rateLimit
	h.rateLimit = false
	h.mu.Unlock()
	if limited {
		w.Header().Set("Retry-After", "0")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{}`))
		return
	}
	rel := strings.TrimPrefix(r.URL.EscapedPath(), s.BasePath)
	status, file := http.StatusOK, ""
	if s.Route != nil {
		file = s.Route(rel, r.URL.Query())
	} else {
		file = h.route(rel, r.URL.Query())
	}
	if f, ok := s.Overrides[rel]; ok {
		status, file = f.Status, f.File
	}
	if !s.Auth.ok(r) {
		status, file = http.StatusUnauthorized, s.Unauthorized
	}
	body, err := os.ReadFile(filepath.Join(s.Fixtures, filepath.FromSlash(file)))
	if err != nil {
		t.Errorf("no fixture %s for %s", file, r.URL)
		http.NotFound(w, r)
		return
	}
	if hb, err := os.ReadFile(filepath.Join(s.Fixtures, filepath.FromSlash(strings.TrimSuffix(file, ".json")+".headers.json"))); err == nil {
		var hs map[string]string
		if err := json.Unmarshal(hb, &hs); err != nil {
			t.Errorf("%s.headers.json: %v", file, err)
		}
		for k, v := range hs {
			w.Header().Set(k, h.local(v))
		}
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(body)
}

// local points the manifest's base URL in a fixture header value, such as
// an absolute Link URL, at the fixture server. A base URL with a template
// stays as it is.
func (h *Harness) local(v string) string {
	base := h.Manifest.BaseURL.Raw
	if base == "" || strings.Contains(base, "{{") {
		return v
	}
	return strings.ReplaceAll(v, base, h.server)
}

// route is the fixture convention: testdata/<path>.json, or with
// pagination parameters testdata/<path>/<k=v&k=v>.json, keys sorted.
// A query key is a pagination parameter unless it is a static params key,
// a size_param, a since/until parameter or the api_key in the query.
func (h *Harness) route(rel string, q url.Values) string {
	static := map[string]bool{}
	m := h.Manifest
	mark := func(params map[string]manifest.Param) {
		for k := range params {
			static[k] = true
		}
	}
	mark(m.Params)
	for _, st := range m.Streams {
		mark(st.Params)
		if p := st.Pagination; p != nil && p.SizeParam != "" {
			static[p.SizeParam] = true
		}
	}
	if m.Auth.Type == manifest.AuthAPIKey && m.Auth.In == "query" {
		static[m.Auth.Name] = true
	}
	var parts []string
	for k, vs := range q {
		if static[k] {
			continue
		}
		for _, v := range vs {
			parts = append(parts, k+"="+url.QueryEscape(v))
		}
	}
	if len(parts) == 0 {
		return rel + ".json"
	}
	slices.Sort(parts)
	return rel + "/" + strings.Join(parts, "&") + ".json"
}

// Read runs one read of the catalog from state.
func (h *Harness) Read(t testing.TB, cat *synclet.ConfiguredCatalog, state json.RawMessage) *testkit.Output {
	t.Helper()
	raw, err := json.Marshal(cat)
	if err != nil {
		t.Fatal(err)
	}
	out, err := testkit.RunSource(context.Background(), h.Source, synclet.Request{Command: synclet.CommandRead, Config: h.suite.Config, Catalog: raw, State: state})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// Catalog configures streams of the discovered catalog (every stream when
// none is named) in mode.
func (h *Harness) Catalog(t testing.TB, mode synclet.SyncMode, streams ...string) *synclet.ConfiguredCatalog {
	t.Helper()
	out, err := testkit.RunSource(context.Background(), h.Source, synclet.Request{Command: synclet.CommandDiscover, Config: h.suite.Config})
	if err != nil || out.Err != nil || out.Catalog() == nil {
		t.Fatalf("discover: %v %v", err, out.Err)
	}
	cat := &synclet.ConfiguredCatalog{}
	for _, s := range out.Catalog().Streams {
		if len(streams) > 0 && !slices.Contains(streams, s.Name) {
			continue
		}
		dsm := synclet.DestinationSyncModeOverwrite
		if mode == synclet.SyncModeIncremental {
			dsm = synclet.DestinationSyncModeAppendDedup
		}
		cat.Streams = append(cat.Streams, synclet.ConfiguredStream{Stream: s, SyncMode: mode, DestinationSyncMode: dsm})
	}
	return cat
}

// Run checks a manifest connector: it lints the manifest, runs the SDK
// conformance suite for full refresh over every stream and once per
// incremental stream, checks that check with BrokenConfig reports the text
// of the unauthorized fixture and that every paginated stream's fixtures
// span MinPages pages.
func Run(t *testing.T, s Suite) {
	t.Helper()
	h := New(t, s)
	s = h.suite
	m := h.Manifest
	full := h.Catalog(t, synclet.SyncModeFullRefresh)

	t.Run("full refresh", func(t *testing.T) {
		testkit.RunSourceSuite(t, h.Source, testkit.SourceSuite{Config: s.Config, BrokenConfig: s.BrokenConfig, Catalog: full})
	})
	for _, st := range m.Streams {
		if st.Incremental == nil {
			continue
		}
		t.Run("incremental "+st.Name, func(t *testing.T) {
			h.incremental(t, &st)
		})
	}
	t.Run("broken config", func(t *testing.T) {
		h.brokenConfig(t)
	})
	t.Run("pages", func(t *testing.T) {
		h.mu.Lock()
		defer h.mu.Unlock()
		for _, st := range m.Streams {
			if st.Pagination != nil && h.pages[st.Name] < s.MinPages {
				t.Errorf("stream %s: the fixtures span %d page(s), want at least %d", st.Name, h.pages[st.Name], s.MinPages)
			}
		}
	})
	if s.RateLimitOnce {
		t.Run("rate limit once", func(t *testing.T) {
			h.mu.Lock()
			h.rateLimit = true
			h.mu.Unlock()
			out := h.Read(t, full, nil)
			if out.Err != nil {
				t.Errorf("read after a 429: %v", out.Err)
			}
		})
	}
}

func (h *Harness) brokenConfig(t *testing.T) {
	s := h.suite
	body, err := os.ReadFile(filepath.Join(s.Fixtures, filepath.FromSlash(s.Unauthorized)))
	if err != nil {
		t.Fatalf("unauthorized fixture: %v", err)
	}
	want := strings.TrimSpace(string(body))
	if len(want) > 200 {
		want = want[:200]
	}
	if p := h.Manifest.Errors.Message; p != nil {
		var doc any
		d := json.NewDecoder(bytes.NewReader(body))
		d.UseNumber()
		if err := d.Decode(&doc); err == nil {
			if v, ok := p.Get(doc); ok {
				if str, ok := v.(string); ok {
					want = str
				}
			}
		}
	}
	out, err := testkit.RunSource(context.Background(), h.Source, synclet.Request{Command: synclet.CommandCheck, Config: s.BrokenConfig})
	if err != nil {
		t.Fatal(err)
	}
	st := out.ConnectionStatus()
	switch {
	case st == nil:
		t.Errorf("check: no CONNECTION_STATUS")
	case st.Status != synclet.ConnectionStatusFailed:
		t.Errorf("check with BrokenConfig: %s", st.Status)
	case !strings.Contains(st.Message, want):
		t.Errorf("check with BrokenConfig: message %q does not carry the API's text %q", st.Message, want)
	}
}

// incremental runs the conformance suite with st alone, incrementally.
func (h *Harness) incremental(t *testing.T, st *manifest.Stream) {
	s := h.suite
	cat := h.Catalog(t, synclet.SyncModeIncremental, st.Name)
	timed := st.Incremental
	if p := st.Partition; p != nil && p.Parent != "" {
		for i := range h.Manifest.Streams {
			if h.Manifest.Streams[i].Name == p.Parent {
				timed = h.Manifest.Streams[i].Incremental
			}
		}
	}
	format := st.Incremental.CursorFormat
	compare := func(a, b any) (int, error) {
		x, err := cursorTime(a, format)
		if err != nil {
			return 0, err
		}
		y, err := cursorTime(b, format)
		if err != nil {
			return 0, err
		}
		return x.Compare(y), nil
	}
	start, err := h.start(timed)
	if err != nil {
		t.Fatal(err)
	}
	bound := func(top time.Time) time.Time { return resumeSince(timed, start, top) }
	var lookback func(synclet.StreamDescriptor, any) (any, error)
	if p := st.Partition; p != nil && p.Values != nil {
		oldest, ok := h.perValue(t, cat, st, bound)
		if !ok {
			return
		}
		lookback = func(synclet.StreamDescriptor, any) (any, error) { return oldest, nil }
	} else {
		lookback = func(_ synclet.StreamDescriptor, top any) (any, error) {
			tt, err := cursorTime(top, format)
			if err != nil {
				return nil, err
			}
			return bound(tt), nil
		}
	}
	testkit.RunSourceSuite(t, h.Source, testkit.SourceSuite{
		Config: s.Config, BrokenConfig: s.BrokenConfig, Catalog: cat,
		CompareCursor: compare, Lookback: lookback,
	})
}

// perValue reads a partition.values stream twice and checks that every
// value's cursor resumes; it returns the oldest bound a second read may
// emit.
func (h *Harness) perValue(t *testing.T, cat *synclet.ConfiguredCatalog, st *manifest.Stream, bound func(time.Time) time.Time) (time.Time, bool) {
	format, key := st.Incremental.CursorFormat, st.Incremental.CursorField.Last()
	first := h.Read(t, cat, nil)
	if first.Err != nil {
		t.Errorf("first read: %v", first.Err)
		return time.Time{}, false
	}
	parts, err := partitions(first.NextState(), st.Name)
	if err != nil {
		t.Error(err)
		return time.Time{}, false
	}
	var oldest time.Time
	for v, entry := range parts {
		c, err := cursorTime(entry[key], format)
		if err != nil {
			t.Errorf("partition %q: %v", v, err)
			return time.Time{}, false
		}
		if b := bound(c); oldest.IsZero() || b.Before(oldest) {
			oldest = b
		}
	}
	second := h.Read(t, cat, first.NextState())
	if second.Err != nil {
		t.Errorf("second read: %v", second.Err)
		return time.Time{}, false
	}
	next, err := partitions(second.NextState(), st.Name)
	if err != nil {
		t.Error(err)
		return time.Time{}, false
	}
	for v, entry := range parts {
		was, _ := cursorTime(entry[key], format)
		got, ok := next[v]
		if !ok {
			t.Errorf("partition %q: missing from the second read's state", v)
			continue
		}
		now, err := cursorTime(got[key], format)
		if err != nil || now.Before(was) {
			t.Errorf("partition %q: cursor went from %v to %v", v, entry[key], got[key])
		}
	}
	return oldest, true
}

func partitions(state json.RawMessage, stream string) (map[string]map[string]any, error) {
	var msgs []synclet.StateMessage
	if err := json.Unmarshal(state, &msgs); err != nil {
		return nil, fmt.Errorf("state: %w", err)
	}
	for _, m := range msgs {
		if m.Stream == nil || m.Stream.StreamDescriptor.Name != stream {
			continue
		}
		var s struct {
			Partitions map[string]map[string]any `json:"partitions"`
		}
		d := json.NewDecoder(bytes.NewReader(m.Stream.StreamState))
		d.UseNumber()
		if err := d.Decode(&s); err != nil {
			return nil, err
		}
		return s.Partitions, nil
	}
	return nil, errors.New("no state for " + stream)
}

func (h *Harness) start(inc *manifest.Incremental) (time.Time, error) {
	var cfg map[string]any
	d := json.NewDecoder(bytes.NewReader(h.suite.Config))
	d.UseNumber()
	if err := d.Decode(&cfg); err != nil {
		return time.Time{}, err
	}
	s, err := inc.Start.Render(manifest.Scope{Config: cfg}, nil)
	if err != nil {
		return time.Time{}, err
	}
	for _, layout := range []string{time.RFC3339Nano, "2006-01-02"} {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UTC(), nil
		}
	}
	return time.Time{}, fmt.Errorf("incremental.start %q", s)
}

// resumeSince is the lower bound of a read resumed from top: top minus
// lookback, or with closed windows the window after top's minus lookback,
// aligned down; never before start.
func resumeSince(inc *manifest.Incremental, start, top time.Time) time.Time {
	var lookback time.Duration
	if inc.Lookback != nil {
		lookback = time.Duration(*inc.Lookback)
	}
	s := top.Add(-lookback)
	if w := inc.Window; w != nil && w.ClosedOnly {
		step := time.Duration(w.Step)
		floor := func(t time.Time) time.Time {
			if t.Before(start) {
				return start
			}
			return start.Add(t.Sub(start) / step * step)
		}
		s = floor(floor(top).Add(step).Add(-lookback))
	}
	if s.Before(start) {
		return start
	}
	return s
}

// cursorTime parses a cursor value as decoded from record JSON, or a
// time.Time a Lookback returned.
func cursorTime(v any, format string) (time.Time, error) {
	if t, ok := v.(time.Time); ok {
		return t, nil
	}
	switch format {
	case manifest.FormatUnixS, manifest.FormatUnixMS:
		n, ok := v.(json.Number)
		if !ok {
			if s, isStr := v.(string); isStr {
				n, ok = json.Number(s), true
			}
		}
		if !ok {
			return time.Time{}, fmt.Errorf("cursor %v is not a number", v)
		}
		f, err := n.Float64()
		if err != nil {
			return time.Time{}, err
		}
		if format == manifest.FormatUnixMS {
			return time.UnixMilli(int64(f)).UTC(), nil
		}
		return time.Unix(int64(f), 0).UTC(), nil
	}
	s, ok := v.(string)
	if !ok {
		return time.Time{}, fmt.Errorf("cursor %v is not a string", v)
	}
	layout := format
	switch format {
	case manifest.FormatRFC3339:
		layout = time.RFC3339Nano
	case manifest.FormatDate:
		layout = "2006-01-02"
	}
	return time.Parse(layout, s)
}
