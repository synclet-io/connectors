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
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// Synthetic identifiers of the fixtures; none belongs to a real advertiser.
const (
	testToken       = "test-access-token"
	testAdvertiserA = "1000000001"
	testAdvertiserB = "1000000002"
	testAdvertiserC = "1000000003"
	apiPrefix       = "/open_api/v1.3/"
	reportPath      = "report/integrated/get/"
)

// fixture is a response the fixture server sends instead of its default:
// status (200 when zero) with the body of testdata/<file>.json. A fixture
// without a file sends the default response.
type fixture struct {
	status int
	file   string
}

// apiRequest is one request the fixture server received.
type apiRequest struct {
	path  string
	query url.Values
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

// of returns the requests of path.
func (h *hits) of(path string) []apiRequest {
	var out []apiRequest
	for _, r := range h.list() {
		if r.path == path {
			out = append(out, r)
		}
	}
	return out
}

// fixtureServer serves the TikTok Business API from testdata:
//
//   - advertiser/info/ answers the advertisers of testdata/advertiser/info.json
//     whose ids are in advertiser_ids;
//   - report/integrated/get/ answers the rows of
//     testdata/report/<advertiser_id>_<data_level>.json whose stat_time_day is
//     between start_date and end_date, paged by page and page_size;
//   - any other path answers testdata/<path>/<advertiser_id>_p<page>.json.
//
// overrides[path] replaces the responses of path with its fixtures in order,
// repeating the last. A request without the test token in its Access-Token
// header gets error 40105 with HTTP 200, as the API answers; one that carries
// the token in its URL fails the test.
func fixtureServer(t *testing.T, overrides map[string][]fixture) (*httptest.Server, *hits) {
	t.Helper()
	h := &hits{}
	var mu sync.Mutex
	served := map[string]int{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		for k, vs := range q {
			if strings.EqualFold(k, "access_token") || strings.EqualFold(k, "access-token") || slices.Contains(vs, testToken) {
				t.Errorf("request %s carries the access token in its URL", r.URL.Path)
			}
		}
		if r.Method != http.MethodGet {
			t.Errorf("request %s %s: only GET is used", r.Method, r.URL.Path)
		}
		p, ok := strings.CutPrefix(r.URL.Path, apiPrefix)
		if !ok {
			http.NotFound(w, r)
			return
		}
		h.add(apiRequest{path: p, query: q})
		var f fixture
		mu.Lock()
		if seq, ok := overrides[p]; ok {
			f = seq[min(served[p], len(seq)-1)]
			served[p]++
		}
		mu.Unlock()
		if r.Header.Get("Access-Token") != testToken {
			f = fixture{file: "errors/40105"}
		}
		var body []byte
		if f.file != "" {
			body = readFixture(t, f.file)
		} else {
			body = defaultResponse(t, p, q)
		}
		if body == nil {
			http.NotFound(w, r)
			return
		}
		if f.status == 0 {
			f.status = http.StatusOK
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(f.status)
		_, _ = w.Write(body)
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

// okBody returns a successful response envelope around data.
func okBody(t *testing.T, data any) []byte {
	b, err := json.Marshal(map[string]any{"code": 0, "message": "OK", "request_id": "test-request", "data": data})
	if err != nil {
		t.Errorf("encode response: %v", err)
	}
	return b
}

func defaultResponse(t *testing.T, p string, q url.Values) []byte {
	switch p {
	case "advertiser/info/":
		var ids []string
		if err := json.Unmarshal([]byte(q.Get("advertiser_ids")), &ids); err != nil {
			t.Errorf("advertiser_ids %q is not a JSON list: %v", q.Get("advertiser_ids"), err)
		}
		var all []map[string]any
		if err := json.Unmarshal(readFixture(t, "advertiser/info"), &all); err != nil {
			t.Errorf("advertiser/info fixture: %v", err)
		}
		list := []map[string]any{}
		for _, a := range all {
			if slices.Contains(ids, fmt.Sprint(a["advertiser_id"])) {
				list = append(list, a)
			}
		}
		return okBody(t, map[string]any{"list": list})
	case reportPath:
		return reportResponse(t, q)
	}
	pageNo := q.Get("page")
	if pageNo == "" {
		pageNo = "1"
	}
	return readFixture(t, strings.TrimSuffix(p, "/")+"/"+q.Get("advertiser_id")+"_p"+pageNo)
}

func reportResponse(t *testing.T, q url.Values) []byte {
	var rows []map[string]any
	if err := json.Unmarshal(readFixture(t, "report/"+q.Get("advertiser_id")+"_"+q.Get("data_level")), &rows); err != nil {
		t.Errorf("report fixture: %v", err)
	}
	start, end := q.Get("start_date"), q.Get("end_date")
	var match []map[string]any
	for _, r := range rows {
		day, _ := r["dimensions"].(map[string]any)["stat_time_day"].(string)
		if day[:10] >= start && day[:10] <= end {
			match = append(match, r)
		}
	}
	size, _ := strconv.Atoi(q.Get("page_size"))
	pageNo, _ := strconv.Atoi(q.Get("page"))
	if size <= 0 || pageNo <= 0 {
		t.Errorf("report request without page and page_size: %v", q)
		return nil
	}
	total := (len(match) + size - 1) / size
	from := min((pageNo-1)*size, len(match))
	to := min(from+size, len(match))
	return okBody(t, map[string]any{
		"list":      append([]map[string]any{}, match[from:to]...),
		"page_info": map[string]any{"page": pageNo, "page_size": size, "total_number": len(match), "total_page": total},
	})
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
		baseURL:    srv.URL + apiPrefix,
		now:        func() time.Time { return testNow },
		client:     srv.Client(),
		retryDelay: time.Millisecond,
		wait:       w.wait,
	}, w
}

// testConfig reads both synthetic advertisers from testStartDate.
func testConfig(t *testing.T, edit func(*Config)) json.RawMessage {
	t.Helper()
	cfg := Config{
		AccessToken:   testToken,
		AdvertiserIDs: []string{testAdvertiserA, testAdvertiserB},
		StartDate:     testStartDate,
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
