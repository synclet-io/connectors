package main

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAPIErrors(t *testing.T) {
	path := "campaign/get/"
	ok := fixture{}
	code := func(c int) fixture { return fixture{file: fmt.Sprintf("errors/%d", c)} }
	repeat := func(f fixture, n int) []fixture {
		seq := make([]fixture, n)
		for i := range seq {
			seq[i] = f
		}
		return seq
	}
	ms := func(ns ...int) []time.Duration {
		var ds []time.Duration
		for _, n := range ns {
			ds = append(ds, time.Duration(n)*time.Millisecond)
		}
		return ds
	}
	type tc struct {
		name  string
		seq   []fixture
		calls int
		waits []time.Duration
		err   string
	}
	tests := []tc{
		{name: "40100 then success waits once", seq: []fixture{code(40100), ok}, calls: 2, waits: ms(1)},
		{name: "the delay doubles", seq: []fixture{code(40100), code(40100), ok}, calls: 3, waits: ms(1, 2)},
		{name: "HTTP 503 is retried", seq: []fixture{{status: http.StatusServiceUnavailable, file: "errors/http_503"}, ok}, calls: 2, waits: ms(1)},
		{name: "HTTP 429 is retried", seq: []fixture{{status: http.StatusTooManyRequests, file: "errors/http_503"}, ok}, calls: 2, waits: ms(1)},
		{
			name: "other codes fail at once with code and message", seq: []fixture{code(40002)}, calls: 1,
			err: "tiktok campaign/get/: code 40002: Invalid parameter: data_level is not supported.",
		},
		{
			name: "retries end with the last code", seq: repeat(code(51004), 6), calls: 1 + retries, waits: ms(1, 2, 4, 8, 16),
			err: "tiktok campaign/get/: code 51004: Request timed out, please try again later.",
		},
	}
	for _, c := range []int{40016, 40100, 40133, 50000, 51002, 51004, 51041, 60001} {
		tests = append(tests, tc{name: fmt.Sprintf("code %d is retried", c), seq: []fixture{code(c), ok}, calls: 2, waits: ms(1)})
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv, h := fixtureServer(t, map[string][]fixture{path: tt.seq})
			src, w := testSource(srv)
			var names []any
			err := src.apiClient(Config{AccessToken: testToken}).list(t.Context(), path, url.Values{"advertiser_id": {testAdvertiserB}},
				func(obj map[string]any) error {
					names = append(names, obj["campaign_name"])
					return nil
				})
			if tt.err != "" {
				require.EqualError(t, err, tt.err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, []any{"Example Campaign B"}, names)
			}
			assert.Len(t, h.of(path), tt.calls)
			assert.Equal(t, tt.waits, w.list())
		})
	}

	t.Run("pagination follows total_page", func(t *testing.T) {
		srv, h := fixtureServer(t, nil)
		src, _ := testSource(srv)
		var names []any
		err := src.apiClient(Config{AccessToken: testToken}).list(t.Context(), path, url.Values{"advertiser_id": {testAdvertiserA}},
			func(obj map[string]any) error {
				names = append(names, obj["campaign_name"])
				return nil
			})
		require.NoError(t, err)
		assert.Equal(t, []any{"Example Campaign A", "Example Campaign C"}, names)
		reqs := h.of(path)
		require.Len(t, reqs, 2)
		assert.Equal(t, []string{"1", "2"}, []string{reqs[0].query.Get("page"), reqs[1].query.Get("page")})
		assert.Equal(t, "1000", reqs[0].query.Get("page_size"))
	})

	t.Run("invalid token is a config error and not retried", func(t *testing.T) {
		srv, h := fixtureServer(t, nil)
		src, w := testSource(srv)
		err := src.apiClient(Config{AccessToken: "expired-token"}).list(t.Context(), path, nil, func(map[string]any) error { return nil })
		require.EqualError(t, err, "tiktok campaign/get/: code 40105: Access token is incorrect or has been revoked.")
		assert.Len(t, h.of(path), 1)
		assert.Empty(t, w.list())
	})
}

func TestRequestHeaders(t *testing.T) {
	var got http.Header
	var gotURL *url.URL
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.Header.Clone()
		gotURL = r.URL
		_, _ = w.Write([]byte(`{"code":0,"message":"OK","data":{"list":[]}}`))
	}))
	t.Cleanup(srv.Close)
	src, _ := testSource(srv)
	var out map[string]any
	require.NoError(t, src.apiClient(Config{AccessToken: testToken}).get(t.Context(), "advertiser/info/", url.Values{"fields": {jsonList("name")}}, &out))
	assert.Equal(t, testToken, got.Get("Access-Token"))
	assert.Equal(t, apiPrefix+"advertiser/info/", gotURL.Path)
	assert.Equal(t, `["name"]`, gotURL.Query().Get("fields"), "list parameters are JSON-encoded")
}

func TestEnvironmentBaseURL(t *testing.T) {
	assert.Equal(t, "https://business-api.tiktok.com/open_api/v1.3/", Source{}.apiClient(Config{}).root)
	assert.Equal(t, "https://sandbox-ads.tiktok.com/open_api/v1.3/", Source{}.apiClient(Config{Environment: envSandbox}).root)
}
