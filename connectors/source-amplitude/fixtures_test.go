package main

import (
	"archive/zip"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// The only credentials the fixture server accepts.
const (
	testAPIKey    = "test-api-key"
	testSecretKey = "test-secret-key"
	testProjectID = "100001"
)

// fixture is a response the fixture server sends instead of the one it
// builds from testdata: status with the body of testdata/<file>, or an empty
// body when file is empty.
type fixture struct {
	status int
	file   string
}

// hit is one request the fixture server received.
type hit struct {
	path  string
	query url.Values
}

// hits records the requests the fixture server received, in order.
type hits struct {
	mu   sync.Mutex
	list []hit
}

func (h *hits) add(x hit) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.list = append(h.list, x)
}

func (h *hits) all() []hit {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]hit(nil), h.list...)
}

// of returns the queries of the requests to path, in order.
func (h *hits) of(path string) []url.Values {
	var qs []url.Values
	for _, x := range h.all() {
		if x.path == path {
			qs = append(qs, x.query)
		}
	}
	return qs
}

// fixtureServer serves the Amplitude API from testdata:
//
//   - GET /2/export?start=<hour>&end=<hour> answers a zip with one gzipped
//     file per testdata/2/export/<hour>.jsonl in the range, named like
//     Amplitude names them, or 404 when there is none;
//   - GET /2/users and /2/sessions/average answer testdata/2/users.json
//     (users_country.json with g=country) and testdata/2/sessions/average.json
//     cut to the days from start to end;
//   - any other path answers testdata/<path>.json.
//
// An override replaces the response of a path; for /2/export the key is
// "2/export?<start>-<end>". A request without the test credentials as basic
// auth gets 401, and a request carrying a credential in its query fails the
// test.
func fixtureServer(t *testing.T, overrides map[string]fixture) (*httptest.Server, *hits) {
	t.Helper()
	h := &hits{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p := strings.TrimPrefix(r.URL.Path, "/")
		q := r.URL.Query()
		h.add(hit{path: p, query: q})
		for _, v := range q {
			for _, s := range v {
				if strings.Contains(s, testAPIKey) || strings.Contains(s, testSecretKey) {
					t.Errorf("request %s carries a credential in its query", r.URL)
				}
			}
		}
		if user, pass, ok := r.BasicAuth(); !ok || user != testAPIKey || pass != testSecretKey {
			serveFile(w, fixture{status: http.StatusUnauthorized, file: "errors/invalid_credentials.json"})
			return
		}
		key := p
		if p == exportPath {
			key += "?" + q.Get("start") + "-" + q.Get("end")
		}
		if f, ok := overrides[key]; ok {
			serveFile(w, f)
			return
		}
		switch p {
		case exportPath:
			serveExport(t, w, q)
		case "2/users", "2/sessions/average":
			file := p
			if q.Get("g") == "country" {
				file += "_country"
			}
			serveSeries(t, w, file, q)
		default:
			serveFile(w, fixture{status: http.StatusOK, file: p + ".json"})
		}
	}))
	t.Cleanup(srv.Close)
	return srv, h
}

func serveFile(w http.ResponseWriter, f fixture) {
	var body []byte
	if f.file != "" {
		var err error
		body, err = os.ReadFile(filepath.Join("testdata", filepath.FromSlash(f.file)))
		if err != nil {
			w.WriteHeader(http.StatusTeapot)
			return
		}
		w.Header().Set("Content-Type", "application/json")
	}
	w.WriteHeader(f.status)
	_, _ = w.Write(body)
}

// serveExport answers an export request with the hours of testdata/2/export
// from start to end.
func serveExport(t *testing.T, w http.ResponseWriter, q url.Values) {
	from, err1 := time.Parse(hourLayout, q.Get("start"))
	to, err2 := time.Parse(hourLayout, q.Get("end"))
	if err1 != nil || err2 != nil || to.Before(from) {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	files := map[string][]byte{}
	for hr := from; !hr.After(to); hr = hr.Add(time.Hour) {
		b, err := os.ReadFile(filepath.Join("testdata", "2", "export", hr.Format(hourLayout)+".jsonl"))
		if err != nil {
			continue
		}
		name := fmt.Sprintf("%s/%s_%s_%d#0.json.gz", testProjectID, testProjectID, hr.Format("2006-01-02"), hr.Hour())
		files[name] = b
	}
	if len(files) == 0 {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/zip")
	_, _ = w.Write(exportZip(t, files))
}

// exportZip returns a zip holding each file gzipped.
func exportZip(t *testing.T, files map[string][]byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	for name, content := range files {
		fw, err := zw.Create(name)
		require.NoError(t, err)
		gz := gzip.NewWriter(fw)
		_, err = gz.Write(content)
		require.NoError(t, err)
		require.NoError(t, gz.Close())
	}
	require.NoError(t, zw.Close())
	return buf.Bytes()
}

// serveSeries answers testdata/<file>.json with the days outside start to
// end removed.
func serveSeries(t *testing.T, w http.ResponseWriter, file string, q url.Values) {
	raw, err := os.ReadFile(filepath.Join("testdata", filepath.FromSlash(file)+".json"))
	require.NoError(t, err)
	var resp struct {
		Data struct {
			Series       [][]any  `json:"series"`
			SeriesMeta   []any    `json:"seriesMeta"`
			SeriesLabels []any    `json:"seriesLabels,omitempty"`
			XValues      []string `json:"xValues"`
		} `json:"data"`
	}
	require.NoError(t, json.Unmarshal(raw, &resp))
	from, err1 := time.Parse(dashboardDateLayout, q.Get("start"))
	to, err2 := time.Parse(dashboardDateLayout, q.Get("end"))
	if err1 != nil || err2 != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	d := resp.Data
	var xs []string
	series := make([][]any, len(d.Series))
	for i, x := range d.XValues {
		if x < from.Format(dateLayout) || x > to.Format(dateLayout) {
			continue
		}
		xs = append(xs, x)
		for j := range d.Series {
			series[j] = append(series[j], d.Series[j][i])
		}
	}
	d.XValues, d.Series = xs, series
	resp.Data = d
	require.NoError(t, json.NewEncoder(w).Encode(resp))
}

// testNow is the fixed clock of every test: 2026-10-18 12:00 UTC, so the
// last export hour read is 2026-10-18 09:00.
var testNow = time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC)

// testSource returns a Source that talks to srv with the test clock.
func testSource(srv *httptest.Server) Source {
	return Source{
		baseURL:    srv.URL + "/",
		now:        func() time.Time { return testNow },
		client:     srv.Client(),
		retryDelay: time.Millisecond,
	}
}
