package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestThrottle(t *testing.T) {
	path := "act_" + testAccountA
	ok := fixture{file: path}
	tests := []struct {
		name  string
		seq   []fixture
		calls int
		waits []time.Duration
		err   string
	}{
		{
			name: "code 4 on HTTP 200 waits estimated_time_to_regain_access",
			seq: []fixture{{status: http.StatusOK, file: "errors/rate_limit", header: map[string]string{
				"X-Business-Use-Case-Usage": `{"3333333333":[{"type":"ads_management","call_count":100,"total_cputime":40,"total_time":50,"estimated_time_to_regain_access":3}]}`,
			}}, ok},
			calls: 2, waits: []time.Duration{3 * time.Minute},
		},
		{
			name: "code 80000 on HTTP 400 waits reset_time_duration",
			seq: []fixture{{status: http.StatusBadRequest, file: "errors/throttled", header: map[string]string{
				"X-Ad-Account-Usage": `{"acc_id_util_pct":100,"reset_time_duration":90,"ads_api_access_tier":"standard_access"}`,
			}}, ok},
			calls: 2, waits: []time.Duration{90 * time.Second},
		},
		{
			name: "the longer of both headers wins",
			seq: []fixture{{status: http.StatusBadRequest, file: "errors/throttled", header: map[string]string{
				"X-Ad-Account-Usage":        `{"acc_id_util_pct":100,"reset_time_duration":600}`,
				"X-Business-Use-Case-Usage": `{"3333333333":[{"estimated_time_to_regain_access":2},{"estimated_time_to_regain_access":0}]}`,
			}}, ok},
			calls: 2, waits: []time.Duration{10 * time.Minute},
		},
		{
			name:  "without usage headers the delay doubles",
			seq:   []fixture{{status: http.StatusBadRequest, file: "errors/throttled"}, {status: http.StatusBadRequest, file: "errors/throttled"}, ok},
			calls: 3, waits: []time.Duration{time.Second, 2 * time.Second},
		},
		{
			name:  "gives up after retries",
			seq:   []fixture{{status: http.StatusBadRequest, file: "errors/throttled"}},
			calls: 1 + retries, waits: []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 16 * time.Second},
			err: "GET act_1111111111: HTTP 400 code 80000/2446079",
		},
		{
			name:  "HTTP 429 without a throttling code is not retried",
			seq:   []fixture{{status: http.StatusTooManyRequests, file: "errors/no_account"}},
			calls: 1, err: "HTTP 429 code 100",
		},
		{
			name:  "other 400 is not retried",
			seq:   []fixture{{status: http.StatusBadRequest, file: "errors/no_account"}},
			calls: 1, err: "HTTP 400 code 100/33",
		},
		{
			name:  "5xx is retried",
			seq:   []fixture{{status: http.StatusServiceUnavailable, file: "errors/no_account"}, ok},
			calls: 2, waits: []time.Duration{time.Second},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv, h := fixtureServer(t, map[string][]fixture{"GET " + path: tt.seq})
			src, w := testSource(srv)
			var out map[string]any
			err := src.apiClient(Config{AccessToken: testToken}).get(t.Context(), path, nil, &out)
			if tt.err != "" {
				require.ErrorContains(t, err, tt.err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, "act_"+testAccountA, out["id"])
			}
			assert.Len(t, h.list(), tt.calls)
			assert.Equal(t, tt.waits, w.list())
		})
	}
}

func TestInvalidTokenIsConfigError(t *testing.T) {
	srv, h := fixtureServer(t, nil)
	src, w := testSource(srv)
	var out map[string]any
	err := src.apiClient(Config{AccessToken: "expired-token"}).get(t.Context(), "me", nil, &out)
	require.ErrorContains(t, err, "invalid or expired access token (GET me: HTTP 400 code 190/463")
	assert.Len(t, h.list(), 1, "error 190 is not retried")
	assert.Empty(t, w.list())
}

func TestRequestHeaders(t *testing.T) {
	var got http.Header
	var gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.Header.Clone()
		gotPath = r.URL.Path
		_, _ = w.Write([]byte(`{"id":"5000001"}`))
	}))
	t.Cleanup(srv.Close)
	src, _ := testSource(srv)
	src.baseURL = "http://unused.invalid"
	cfg := Config{
		AccessToken:     testToken,
		GraphAPIBaseURL: srv.URL + "/",
		GraphAPIHeaders: []Header{{Name: "X-Proxy-Key", Value: "proxy-secret"}},
	}
	var out map[string]any
	require.NoError(t, src.apiClient(cfg).get(t.Context(), "me", nil, &out))
	assert.Equal(t, "/"+apiVersion+"/me", gotPath, "graph_api_base_url replaces the host and keeps the version")
	assert.Equal(t, "proxy-secret", got.Get("X-Proxy-Key"))
	assert.Equal(t, "Bearer "+testToken, got.Get("Authorization"))
}
