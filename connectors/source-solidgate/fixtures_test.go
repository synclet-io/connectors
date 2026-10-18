package main

import (
	"crypto/hmac"
	"crypto/sha512"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// testPublicKey and testSecretKey are the only key pair the fixture server
// accepts.
const (
	testPublicKey = "api_pk_test"
	testSecretKey = "api_sk_test"
)

// fixture is a response the fixture server sends for one request key instead
// of the default: status, the body of testdata/<file> or body and, for a
// 302, the path it redirects to.
type fixture struct {
	status   int
	file     string
	body     string
	location string
}

// hits records the requests the fixture server got, in order, as keys:
// "<report path> <day>[ <iterator>]", "poll <day>", "file <day>" and
// "products <offset>".
type hits struct {
	mu     sync.Mutex
	keys   []string
	bodies []reportRequest
}

func (h *hits) add(key string, body *reportRequest) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.keys = append(h.keys, key)
	if body != nil {
		h.bodies = append(h.bodies, *body)
	}
}

func (h *hits) list() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]string(nil), h.keys...)
}

func (h *hits) requests() []reportRequest {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]reportRequest(nil), h.bodies...)
}

// expectedSignature recomputes the signature header with crypto/hmac.
func expectedSignature(body []byte) string {
	mac := hmac.New(sha512.New, []byte(testSecretKey))
	mac.Write([]byte(testPublicKey + string(body) + testPublicKey))
	return base64.StdEncoding.EncodeToString([]byte(hex.EncodeToString(mac.Sum(nil))))
}

// fixtureServer serves the Solidgate APIs from testdata:
//
//   - POST /reports/<path> answers with testdata/<path>/<day of date_from>.json,
//     or <day>_<next_page_iterator>.json for a later page, or <path>/empty.json;
//   - POST /reports/finance/financial_entries answers with a report_url,
//     GET of that URL redirects to /files/<day>.csv, which serves
//     testdata/financial_entries/<day>.csv or empty.csv;
//   - GET /subscriptions/products answers with testdata/products/offset_<n>.json
//     or products/empty.json.
//
// A signed request with the wrong key pair or signature gets 401 with the
// authentication error. overrides replaces the responses to a request key
// in turn, repeating the last one.
func fixtureServer(t *testing.T, overrides map[string][]fixture) (*httptest.Server, *hits) {
	t.Helper()
	h := &hits{}
	var mu sync.Mutex
	served := map[string]int{}
	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read request body: %v", err)
			return
		}
		key, def, signed := route(t, srv.URL, r, body, h)
		if signed && (r.Header.Get("merchant") != testPublicKey || r.Header.Get("signature") != expectedSignature(body)) {
			def = fixture{status: http.StatusUnauthorized, file: "errors/invalid_key_pair.json"}
		}
		if !signed && (r.Header.Get("merchant") != "" || r.Header.Get("signature") != "") {
			t.Errorf("request %s carries Solidgate headers", key)
		}
		if list := overrides[key]; len(list) > 0 {
			mu.Lock()
			i := min(served[key], len(list)-1)
			served[key]++
			mu.Unlock()
			def = list[i]
		}
		if def.location != "" {
			w.Header().Set("Location", srv.URL+def.location)
		}
		if def.file == "" {
			w.WriteHeader(def.status)
			_, _ = w.Write([]byte(def.body))
			return
		}
		data, err := os.ReadFile(filepath.Join("testdata", filepath.FromSlash(def.file)))
		if err != nil {
			t.Errorf("fixture %s: %v", def.file, err)
			http.NotFound(w, r)
			return
		}
		w.WriteHeader(def.status)
		_, _ = w.Write(data)
	}))
	t.Cleanup(srv.Close)
	return srv, h
}

// route returns the key of a request, its default response and whether it
// must be signed.
func route(t *testing.T, base string, r *http.Request, body []byte, h *hits) (string, fixture, bool) {
	t.Helper()
	p := r.URL.Path
	switch {
	case p == "/reports/"+financialEntriesPath:
		var req reportRequest
		if err := json.Unmarshal(body, &req); err != nil {
			t.Errorf("financial entries request body: %v", err)
		}
		d := req.DateFrom[:10]
		key := financialEntriesPath + " " + d
		h.add(key, &req)
		reportBody, _ := json.Marshal(map[string]string{"report_url": base + "/reports/" + financialEntriesPath + "/report/" + d})
		return key, fixture{status: http.StatusOK, body: string(reportBody)}, true
	case strings.HasPrefix(p, "/reports/"+financialEntriesPath+"/report/"):
		d := strings.TrimPrefix(p, "/reports/"+financialEntriesPath+"/report/")
		key := "poll " + d
		h.add(key, nil)
		return key, fixture{status: http.StatusFound, location: "/files/" + d + ".csv"}, true
	case strings.HasPrefix(p, "/files/"):
		d := strings.TrimSuffix(strings.TrimPrefix(p, "/files/"), ".csv")
		key := "file " + d
		h.add(key, nil)
		file := "financial_entries/" + d + ".csv"
		if _, err := os.Stat(filepath.Join("testdata", file)); err != nil {
			file = "financial_entries/empty.csv"
		}
		return key, fixture{status: http.StatusOK, file: file}, false
	case p == "/subscriptions/products":
		offset := r.URL.Query().Get("pagination[offset]")
		key := "products " + offset
		h.add(key, nil)
		file := "products/offset_" + offset + ".json"
		if _, err := os.Stat(filepath.Join("testdata", file)); err != nil {
			file = "products/empty.json"
		}
		return key, fixture{status: http.StatusOK, file: file}, true
	case strings.HasPrefix(p, "/reports/"):
		path := strings.TrimPrefix(p, "/reports/")
		var req reportRequest
		if err := json.Unmarshal(body, &req); err != nil {
			t.Errorf("report request body: %v", err)
		}
		d := req.DateFrom[:min(10, len(req.DateFrom))]
		key, file := path+" "+d, path+"/"+d
		if req.NextPageIterator != "" {
			key += " " + req.NextPageIterator
			file += "_" + req.NextPageIterator
		}
		h.add(key, &req)
		file += ".json"
		if _, err := os.Stat(filepath.Join("testdata", file)); err != nil {
			file = path + "/empty.json"
		}
		return key, fixture{status: http.StatusOK, file: file}, true
	}
	t.Errorf("unexpected request %s %s", r.Method, p)
	return p, fixture{status: http.StatusNotFound}, true
}

// testNow is the fixed clock of every test: today is 2026-10-18 UTC.
var testNow = time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC)

// testSource returns a Source that talks to srv with the test clock, short
// delays and no rate limit.
func testSource(srv *httptest.Server) Source {
	return Source{
		reportsURL:       srv.URL + "/reports/",
		subscriptionsURL: srv.URL + "/subscriptions/",
		now:              func() time.Time { return testNow },
		client:           srv.Client(),
		retryDelay:       time.Millisecond,
		pollDelay:        time.Millisecond,
		pollTimeout:      time.Second,
		noRateLimit:      true,
	}
}
