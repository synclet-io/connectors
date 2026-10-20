package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// The fixture server accepts the service account testUser:testSecret with
// project_id testProject, and the project secret testAPISecret without a
// project_id.
const (
	testUser      = "test-sa.mixpanel-service-account"
	testSecret    = "test-sa-secret"
	testProject   = 1234567
	testAPISecret = "test-project-secret"

	queryPrefix  = "/api/2.0/"
	exportPrefix = "/export/api/2.0/"
)

// fixture replaces the response of one request key (see hits) with status,
// headers and the body of testdata/<file>. times > 0 sends it for the first
// times requests of the key only.
type fixture struct {
	status  int
	file    string
	headers map[string]string
	times   int
}

// hits records the requests of the fixture server, in order, as
// "<api> <path>?<sorted params>", api being query or export and params the
// query and form values without project_id.
type hits struct {
	mu   sync.Mutex
	reqs []string
}

func (h *hits) add(p string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.reqs = append(h.reqs, p)
}

func (h *hits) list() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]string(nil), h.reqs...)
}

// projectZone is the zone of the fixture project, in which the server reads
// from_date and to_date.
var projectZone = func() *time.Location {
	loc, err := time.LoadLocation("US/Pacific")
	if err != nil {
		panic(err)
	}
	return loc
}()

// fixtureServer serves the Query API under /api/2.0/ and the Export API
// under /export/api/2.0/ from testdata, or from an override keyed by path
// (the path under the API root, such as "export" or "engage"). It answers
// 401 to other credentials, 400 to a service account request without
// project_id testProject, fails the test when a request carries a secret
// in its URL, filters the date-ranged fixtures by from_date and to_date and
// pages engage by page and session_id.
func fixtureServer(t *testing.T, overrides map[string]fixture) (*httptest.Server, *hits) {
	t.Helper()
	h := &hits{}
	var mu sync.Mutex
	served := map[string]int{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		for _, s := range []string{testSecret, testAPISecret} {
			if strings.Contains(r.URL.RawQuery, s) {
				t.Errorf("request %s carries a secret in its URL", r.URL.Path)
			}
		}
		if err := r.ParseForm(); err != nil {
			t.Errorf("parse form of %s: %v", r.URL, err)
		}
		apiName, path := "query", strings.TrimPrefix(r.URL.Path, queryPrefix)
		if p, ok := strings.CutPrefix(r.URL.Path, exportPrefix); ok {
			apiName, path = "export", p
		}
		params := url.Values{}
		for k, v := range r.Form {
			if k != "project_id" {
				params[k] = v
			}
		}
		key := apiName + " " + path
		if len(params) > 0 {
			key += "?" + params.Encode()
		}
		h.add(key)

		project := r.URL.Query().Get("project_id")
		switch r.Header.Get("Authorization") {
		case "Basic " + base64.StdEncoding.EncodeToString([]byte(testUser+":"+testSecret)):
			if project != strconv.Itoa(testProject) {
				serveFile(t, w, http.StatusBadRequest, "errors/project.json", nil)
				return
			}
		case "Basic " + base64.StdEncoding.EncodeToString([]byte(testAPISecret+":")):
			if project != "" {
				serveFile(t, w, http.StatusBadRequest, "errors/project.json", nil)
				return
			}
		default:
			serveFile(t, w, http.StatusUnauthorized, "errors/unauthorized.json", nil)
			return
		}
		if o, ok := overrides[path]; ok {
			mu.Lock()
			served[path]++
			n := served[path]
			mu.Unlock()
			if o.times == 0 || n <= o.times {
				if o.status == 0 {
					o.status = http.StatusOK
				}
				serveFile(t, w, o.status, o.file, o.headers)
				return
			}
		}
		serveAPI(t, w, r, apiName, path)
	}))
	t.Cleanup(srv.Close)
	return srv, h
}

func serveAPI(t *testing.T, w http.ResponseWriter, r *http.Request, apiName, path string) {
	from, to := r.Form.Get("from_date"), r.Form.Get("to_date")
	switch {
	case apiName == "export" && path == "export":
		serveExport(t, w, from, to)
	case path == "engage" && r.Method == http.MethodPost:
		page := r.Form.Get("page")
		if page == "" {
			page = "0"
		}
		file := "engage/page-" + page + ".json"
		if c := r.Form.Get("filter_by_cohort"); c != "" {
			var f struct {
				ID int64 `json:"id"`
			}
			if err := json.Unmarshal([]byte(c), &f); err != nil {
				t.Errorf("filter_by_cohort %q: %v", c, err)
			}
			file = fmt.Sprintf("engage/cohort-%d-page-%s.json", f.ID, page)
		}
		if page != "0" && r.Form.Get("session_id") == "" {
			t.Errorf("engage page %s without session_id", page)
		}
		serveFile(t, w, http.StatusOK, file, nil)
	case path == "funnels":
		serveFiltered(t, w, "funnels/"+r.Form.Get("funnel_id")+".json", "data", from, to)
	case path == "engage/revenue":
		serveFiltered(t, w, "engage/revenue.json", "results", from, to)
	case path == "cohorts/list" && r.Method != http.MethodPost:
		t.Errorf("cohorts/list is sent with %s", r.Method)
		http.NotFound(w, r)
	default:
		serveFile(t, w, http.StatusOK, path+".json", nil)
	}
}

func serveFile(t *testing.T, w http.ResponseWriter, status int, file string, headers map[string]string) {
	body, err := os.ReadFile(filepath.Join("testdata", filepath.FromSlash(file)))
	if err != nil {
		t.Errorf("no fixture %s", file)
		w.WriteHeader(http.StatusNotFound)
		return
	}
	for k, v := range headers {
		w.Header().Set(k, v)
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(body)
}

func inRange(day, from, to string) bool { return day >= from && day <= to }

// serveExport answers with the events of testdata/export.jsonl whose time
// falls on a project day from from to to.
func serveExport(t *testing.T, w http.ResponseWriter, from, to string) {
	raw, err := os.ReadFile(filepath.Join("testdata", "export.jsonl"))
	if err != nil {
		t.Errorf("read export.jsonl: %v", err)
		return
	}
	var out bytes.Buffer
	sc := bufio.NewScanner(bytes.NewReader(raw))
	for sc.Scan() {
		var ev struct {
			Properties struct {
				Time json.Number `json:"time"`
			} `json:"properties"`
		}
		if err := json.Unmarshal(sc.Bytes(), &ev); err != nil {
			t.Errorf("export.jsonl: %v", err)
			return
		}
		ts, _ := ev.Properties.Time.Int64()
		if ts > 1e11 {
			ts /= 1000
		}
		if inRange(time.Unix(ts, 0).In(projectZone).Format(dateLayout), from, to) {
			out.Write(sc.Bytes())
			out.WriteByte('\n')
		}
	}
	w.Header().Set("Content-Type", "text/plain")
	_, _ = w.Write(out.Bytes())
}

// serveFiltered answers with file, keeping of the object under key only
// the days from from to to and the totals keyed with $.
func serveFiltered(t *testing.T, w http.ResponseWriter, file, key, from, to string) {
	raw, err := os.ReadFile(filepath.Join("testdata", filepath.FromSlash(file)))
	if err != nil {
		t.Errorf("no fixture %s", file)
		w.WriteHeader(http.StatusNotFound)
		return
	}
	var doc map[string]json.RawMessage
	var byDay map[string]json.RawMessage
	if err := json.Unmarshal(raw, &doc); err != nil || json.Unmarshal(doc[key], &byDay) != nil {
		t.Errorf("fixture %s: no object %s", file, key)
		return
	}
	kept := map[string]json.RawMessage{}
	for d, v := range byDay {
		if strings.HasPrefix(d, "$") || inRange(d, from, to) {
			kept[d] = v
		}
	}
	doc[key], _ = json.Marshal(kept)
	body, _ := json.Marshal(doc)
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(body)
}

// clock is a fake clock: wait records each duration and moves the clock
// forward by it instead of sleeping.
type clock struct {
	mu sync.Mutex
	t  time.Time
	ds []time.Duration
}

// newClock starts at 2026-10-18 12:00 in US/Pacific.
func newClock() *clock { return &clock{t: time.Date(2026, 10, 18, 19, 0, 0, 0, time.UTC)} }

func (c *clock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *clock) wait(ctx context.Context, d time.Duration) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ds = append(c.ds, d)
	c.t = c.t.Add(d)
	return ctx.Err()
}

func (c *clock) waits() []time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]time.Duration(nil), c.ds...)
}

// testSource returns a Source that talks to srv on the fake clock clk.
func testSource(srv *httptest.Server, clk *clock) Source {
	return Source{
		queryURL:   srv.URL + queryPrefix,
		exportURL:  srv.URL + exportPrefix,
		client:     srv.Client(),
		now:        clk.now,
		retryDelay: time.Millisecond,
		wait:       clk.wait,
	}
}

// testConfig is a service account configuration reading 2026-10-10 to
// today in windows of 5 days and profiles in pages of 2; mods change it.
func testConfig(t *testing.T, mods ...func(*Config)) json.RawMessage {
	t.Helper()
	window, page := 5, 2
	cfg := Config{
		Credentials:     Credentials{OptionTitle: authServiceAccount, Username: testUser, Secret: testSecret, ProjectID: testProject},
		ProjectTimezone: "US/Pacific",
		StartDate:       "2026-10-10",
		DateWindowSize:  &window,
		PageSize:        &page,
	}
	for _, m := range mods {
		m(&cfg)
	}
	raw, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func projectSecret(c *Config) {
	c.Credentials = Credentials{OptionTitle: authProjectSecret, APISecret: testAPISecret}
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
