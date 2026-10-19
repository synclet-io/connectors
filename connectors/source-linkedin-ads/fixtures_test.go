package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// Synthetic credentials and identifiers of the fixtures; none belongs to a
// real LinkedIn app, user or ad account.
const (
	testClientID     = "test-client-id"
	testClientSecret = "test-client-secret"
	testRefreshToken = "test-refresh-token"
	testAccessPrefix = "test-access-token-"
	testAccountA     = "1001"
	testAccountB     = "1002"
	testAccountC     = "1003"
	restPrefix       = "/rest/"
	tokenPath        = "/oauth/v2/accessToken"
	// tokenRoute is the overrides key of the token endpoint.
	tokenRoute = "token"
)

// fixture is a response the fixture server sends instead of its default:
// status (200 when zero) with the body of testdata/<file>.json. A fixture
// without a file sends the default response.
type fixture struct {
	status int
	file   string
}

// apiRequest is one request the fixture server received. route is the path
// after /rest/, or tokenRoute; params holds the raw, undecoded values.
type apiRequest struct {
	route  string
	params map[string]string
	auth   string
	form   url.Values
}

// hits records the requests the fixture server received, in order.
type hits struct {
	mu   sync.Mutex
	reqs []apiRequest
}

func (h *hits) add(r apiRequest) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.reqs = append(h.reqs, r)
}

func (h *hits) list() []apiRequest {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]apiRequest(nil), h.reqs...)
}

// of returns the requests of route.
func (h *hits) of(route string) []apiRequest {
	var out []apiRequest
	for _, r := range h.list() {
		if r.route == route {
			out = append(out, r)
		}
	}
	return out
}

// rawParams splits a query string without decoding its values, so a test
// sees the Rest.li encoding as it was sent.
func rawParams(raw string) map[string]string {
	out := map[string]string{}
	for _, kv := range strings.Split(raw, "&") {
		if kv == "" {
			continue
		}
		k, v, _ := strings.Cut(kv, "=")
		out[k] = v
	}
	return out
}

// fixtureServer serves the LinkedIn token endpoint and Marketing API from
// testdata:
//
//   - POST /oauth/v2/accessToken exchanges the test refresh token for a new
//     access token test-access-token-<n> on every call;
//   - adAccounts?q=search pages testdata/ad_accounts/search_<pageToken>.json;
//   - adAccounts/<id> answers testdata/ad_accounts/<id>.json, else 404;
//   - adAccounts/<id>/adCampaignGroups, adCampaigns and creatives page
//     testdata/<stream>/<id>_<pageToken>.json, p1 without a pageToken;
//   - adAnalytics answers the rows of testdata/analytics/<id>_<pivot>.json
//     whose day is in dateRange, parsing the Rest.li encoding.
//
// overrides[route] replaces the responses of route with its fixtures in
// order, repeating the last. An API request without a LinkedIn-Version or
// X-Restli-Protocol-Version header, or without the latest issued access
// token, gets 401 INVALID_ACCESS_TOKEN. A request that carries a credential
// in its URL fails the test.
func fixtureServer(t *testing.T, overrides map[string][]fixture) (*httptest.Server, *hits) {
	t.Helper()
	h := &hits{}
	var mu sync.Mutex
	served := map[string]int{}
	issued := 0
	next := func(route string) (fixture, bool) {
		mu.Lock()
		defer mu.Unlock()
		seq, ok := overrides[route]
		if !ok {
			return fixture{}, false
		}
		f := seq[min(served[route], len(seq)-1)]
		served[route]++
		return f, true
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		for _, secret := range []string{testClientSecret, testRefreshToken, testAccessPrefix} {
			if strings.Contains(r.URL.RawQuery, secret) || strings.Contains(r.URL.Path, secret) {
				t.Errorf("request %s carries a credential in its URL", r.URL.Path)
			}
		}
		if r.URL.Path == tokenPath {
			if r.Method != http.MethodPost {
				t.Errorf("token request with %s", r.Method)
			}
			if err := r.ParseForm(); err != nil {
				t.Errorf("token form: %v", err)
			}
			h.add(apiRequest{route: tokenRoute, form: r.PostForm})
			if f, ok := next(tokenRoute); ok && f.file != "" {
				writeFixture(t, w, f)
				return
			}
			form := r.PostForm
			switch {
			case form.Get("client_id") != testClientID || form.Get("client_secret") != testClientSecret:
				writeFixture(t, w, fixture{status: http.StatusUnauthorized, file: "errors/token_invalid_client"})
			case form.Get("grant_type") != "refresh_token" || form.Get("refresh_token") != testRefreshToken:
				writeFixture(t, w, fixture{status: http.StatusBadRequest, file: "errors/token_invalid_grant"})
			default:
				mu.Lock()
				issued++
				token := fmt.Sprintf("%s%d", testAccessPrefix, issued)
				mu.Unlock()
				writeJSON(t, w, http.StatusOK, map[string]any{
					"access_token": token, "expires_in": 5184000,
					"refresh_token": testRefreshToken, "refresh_token_expires_in": 31536000,
				})
			}
			return
		}
		route, ok := strings.CutPrefix(r.URL.Path, restPrefix)
		if !ok {
			http.NotFound(w, r)
			return
		}
		if r.Method != http.MethodGet {
			t.Errorf("request %s %s: only GET is used", r.Method, r.URL.Path)
		}
		req := apiRequest{route: route, params: rawParams(r.URL.RawQuery), auth: r.Header.Get("Authorization")}
		h.add(req)
		mu.Lock()
		current := fmt.Sprintf("Bearer %s%d", testAccessPrefix, issued)
		mu.Unlock()
		if r.Header.Get("LinkedIn-Version") != apiVersion || r.Header.Get("X-Restli-Protocol-Version") != "2.0.0" || req.auth != current {
			writeFixture(t, w, fixture{status: http.StatusUnauthorized, file: "errors/401_invalid"})
			return
		}
		if f, ok := next(route); ok && f.file != "" {
			writeFixture(t, w, f)
			return
		}
		defaultResponse(t, w, route, req.params)
	}))
	t.Cleanup(srv.Close)
	return srv, h
}

func readFixture(t *testing.T, file string) []byte {
	body, err := os.ReadFile(filepath.Join("testdata", filepath.FromSlash(file)+".json"))
	if err != nil {
		t.Errorf("no fixture %s: %v", file, err)
		return nil
	}
	return body
}

func writeFixture(t *testing.T, w http.ResponseWriter, f fixture) {
	if f.status == 0 {
		f.status = http.StatusOK
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(f.status)
	_, _ = w.Write(readFixture(t, f.file))
}

func writeJSON(t *testing.T, w http.ResponseWriter, status int, v any) {
	b, err := json.Marshal(v)
	if err != nil {
		t.Errorf("encode response: %v", err)
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(b)
}

var entityRoute = regexp.MustCompile(`^adAccounts/(\d+)/(adCampaignGroups|adCampaigns|creatives)$`)

// entityDirs maps an entity finder to its testdata directory and finder name.
var entityDirs = map[string][2]string{
	"adCampaignGroups": {"campaign_groups", "search"},
	"adCampaigns":      {"campaigns", "search"},
	"creatives":        {"creatives", "criteria"},
}

func defaultResponse(t *testing.T, w http.ResponseWriter, route string, params map[string]string) {
	pageToken := params["pageToken"]
	if pageToken == "" {
		pageToken = "p1"
	}
	if route == "adAccounts" {
		if params["q"] != "search" {
			t.Errorf("adAccounts without q=search: %v", params)
		}
		writeFixture(t, w, fixture{file: "ad_accounts/search_" + pageToken})
		return
	}
	if id, ok := strings.CutPrefix(route, "adAccounts/"); ok && isDigits(id) {
		if _, err := os.Stat(filepath.Join("testdata", "ad_accounts", id+".json")); err != nil {
			writeFixture(t, w, fixture{status: http.StatusNotFound, file: "errors/404"})
			return
		}
		writeFixture(t, w, fixture{file: "ad_accounts/" + id})
		return
	}
	if m := entityRoute.FindStringSubmatch(route); m != nil {
		dir := entityDirs[m[2]]
		if params["q"] != dir[1] {
			t.Errorf("%s without q=%s: %v", route, dir[1], params)
		}
		if params["pageSize"] == "" {
			t.Errorf("%s without pageSize", route)
		}
		writeFixture(t, w, fixture{file: dir[0] + "/" + m[1] + "_" + pageToken})
		return
	}
	if route == "adAnalytics" {
		analyticsResponse(t, w, params)
		return
	}
	t.Errorf("unexpected request %s", route)
	writeFixture(t, w, fixture{status: http.StatusNotFound, file: "errors/404"})
}

var (
	accountsList  = regexp.MustCompile(`^List\(urn%3Ali%3AsponsoredAccount%3A(\d+)\)$`)
	dateRangeExpr = regexp.MustCompile(`^\(start:\(year:(\d{4}),month:(\d{1,2}),day:(\d{1,2})\),end:\(year:(\d{4}),month:(\d{1,2}),day:(\d{1,2})\)\)$`)
)

func analyticsResponse(t *testing.T, w http.ResponseWriter, params map[string]string) {
	bad := func(format string, args ...any) {
		t.Errorf("adAnalytics: "+format, args...)
		writeFixture(t, w, fixture{status: http.StatusBadRequest, file: "errors/400_fields"})
	}
	if params["q"] != "analytics" || params["timeGranularity"] != "DAILY" {
		bad("want q=analytics and timeGranularity=DAILY: %v", params)
		return
	}
	acc := accountsList.FindStringSubmatch(params["accounts"])
	if acc == nil {
		bad("accounts %q is not a Rest.li List of one account URN", params["accounts"])
		return
	}
	dr := dateRangeExpr.FindStringSubmatch(params["dateRange"])
	if dr == nil {
		bad("dateRange %q is not a Rest.li DateRange", params["dateRange"])
		return
	}
	fieldList := strings.Split(params["fields"], ",")
	if len(fieldList) > 20 || !containsAll(fieldList, "dateRange", "pivotValues") {
		bad("fields %q", params["fields"])
		return
	}
	start, end := ymd(dr[1], dr[2], dr[3]), ymd(dr[4], dr[5], dr[6])
	var rows []map[string]any
	if err := json.Unmarshal(readFixture(t, "analytics/"+acc[1]+"_"+strings.ToLower(params["pivot"])), &rows); err != nil {
		bad("fixture: %v", err)
		return
	}
	elements := []map[string]any{}
	for _, r := range rows {
		s := r["dateRange"].(map[string]any)["start"].(map[string]any)
		day := fmt.Sprintf("%04.0f-%02.0f-%02.0f", s["year"], s["month"], s["day"])
		if day >= start && day <= end {
			elements = append(elements, r)
		}
	}
	writeJSON(t, w, http.StatusOK, map[string]any{"elements": elements, "paging": map[string]any{"start": 0, "count": 10, "links": []any{}}})
}

func ymd(y, m, d string) string {
	mi, _ := strconv.Atoi(m)
	di, _ := strconv.Atoi(d)
	return fmt.Sprintf("%s-%02d-%02d", y, mi, di)
}

func containsAll(list []string, want ...string) bool {
	for _, w := range want {
		found := false
		for _, l := range list {
			found = found || l == w
		}
		if !found {
			return false
		}
	}
	return true
}

// testNow is the fixed clock of every test: today is 2026-10-18.
var testNow = time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC)

// testStartDate is the start_date of the test configuration.
const testStartDate = "2026-08-01"

// waits records the waits a Source asked for instead of sleeping.
type waits struct {
	mu sync.Mutex
	ds []time.Duration
}

func (w *waits) wait(ctx context.Context, d time.Duration) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.ds = append(w.ds, d)
	return ctx.Err()
}

func (w *waits) list() []time.Duration {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]time.Duration(nil), w.ds...)
}

// testSource returns a Source that talks to srv with the test clock and
// records its waits instead of sleeping; the retry delay starts at 1ms.
func testSource(srv *httptest.Server) (Source, *waits) {
	w := &waits{}
	return Source{
		apiURL:     srv.URL + restPrefix,
		tokenURL:   srv.URL + tokenPath,
		now:        func() time.Time { return testNow },
		client:     srv.Client(),
		retryDelay: time.Millisecond,
		wait:       w.wait,
	}, w
}

// testConfig reads both synthetic accounts from testStartDate.
func testConfig(t *testing.T, edit func(*Config)) json.RawMessage {
	t.Helper()
	cfg := Config{
		ClientID:     testClientID,
		ClientSecret: testClientSecret,
		RefreshToken: testRefreshToken,
		AccountIDs:   []string{testAccountA, testAccountB},
		StartDate:    testStartDate,
	}
	if edit != nil {
		edit(&cfg)
	}
	raw, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func decodeConfig(t *testing.T, raw json.RawMessage) Config {
	t.Helper()
	var cfg Config
	if err := json.Unmarshal(raw, &cfg); err != nil {
		t.Fatal(err)
	}
	return cfg
}
