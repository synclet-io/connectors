package main

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
)

var testCfg = Config{Subdomain: "acme", Email: testEmail, APIToken: testToken, StartDate: "2026-10-01"}

// statusServer answers the first len(statuses) requests with those statuses
// and every later one with 200 and body.
func statusServer(t *testing.T, header http.Header, body string, statuses ...int) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := int(calls.Add(1))
		if n <= len(statuses) {
			for k, v := range header {
				w.Header()[k] = v
			}
			w.WriteHeader(statuses[n-1])
			_, _ = w.Write([]byte(`{"error":"APIRateLimitExceeded","description":"Number of allowed API requests per minute exceeded"}`))
			return
		}
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	return srv, &calls
}

func repeat(status, n int) []int {
	out := make([]int, n)
	for i := range out {
		out[i] = status
	}
	return out
}

func TestRateLimit(t *testing.T) {
	t.Run("429 waits Retry-After and repeats", func(t *testing.T) {
		srv, calls := statusServer(t, http.Header{"Retry-After": {"7"}}, `{"user":{"role":"admin"}}`, http.StatusTooManyRequests)
		w := &waits{}
		src := testSource(srv)
		src.wait = w.wait
		var out map[string]any
		require.NoError(t, src.apiClient(testCfg).get(t.Context(), "users/me.json", nil, &out))
		assert.Equal(t, []time.Duration{7 * time.Second}, w.list())
		assert.Equal(t, int32(2), calls.Load())
	})

	t.Run("429 without Retry-After waits a minute", func(t *testing.T) {
		srv, _ := statusServer(t, nil, `{}`, http.StatusTooManyRequests)
		w := &waits{}
		src := testSource(srv)
		src.wait = w.wait
		var out map[string]any
		require.NoError(t, src.apiClient(testCfg).get(t.Context(), "users/me.json", nil, &out))
		assert.Equal(t, []time.Duration{time.Minute}, w.list())
	})

	t.Run("429 six times fails", func(t *testing.T) {
		srv, calls := statusServer(t, http.Header{"Retry-After": {"1"}}, `{}`, repeat(http.StatusTooManyRequests, 6)...)
		w := &waits{}
		src := testSource(srv)
		src.wait = w.wait
		var out map[string]any
		err := src.apiClient(testCfg).get(t.Context(), "incremental/tickets/cursor.json", nil, &out)
		require.ErrorContains(t, err, "GET /api/v2/incremental/tickets/cursor.json (HTTP 429): rate limited; gave up after 5 waits")
		assert.Equal(t, int32(6), calls.Load())
		assert.Len(t, w.list(), rateLimitRetries)
	})

	t.Run("export requests are spaced", func(t *testing.T) {
		srv, h := fixtureServer(t, nil)
		w := &waits{}
		src := testSource(srv)
		src.wait = w.wait
		cat := catalogOf(t, src, synclet.SyncModeIncremental, "tickets", "ticket_events")
		runRead(t, src, cat, nil)
		assert.Len(t, h.list(), 4)
		assert.Equal(t, []time.Duration{6 * time.Second, 6 * time.Second, 6 * time.Second}, w.list(),
			"6s before every export request after the first, across streams")
	})

	t.Run("export spacing counts elapsed time", func(t *testing.T) {
		srv, _ := fixtureServer(t, nil)
		w := &waits{}
		now := testNow
		src := testSource(srv)
		src.wait = w.wait
		src.now = func() time.Time { return now }
		c := src.apiClient(testCfg)
		var out map[string]any
		require.NoError(t, c.exportGet(t.Context(), "incremental/tickets/cursor.json", nil, &out))
		now = now.Add(4 * time.Second)
		require.NoError(t, c.exportGet(t.Context(), "incremental/tickets/cursor.json", nil, &out))
		now = now.Add(10 * time.Second)
		require.NoError(t, c.exportGet(t.Context(), "incremental/tickets/cursor.json", nil, &out))
		assert.Equal(t, []time.Duration{2 * time.Second}, w.list())
	})
}

func TestErrorsNotRetried(t *testing.T) {
	for _, tt := range []struct {
		status int
		err    string
	}{
		{status: http.StatusUnauthorized, err: "invalid email or API token (HTTP 401): APIRateLimitExceeded"},
		{status: http.StatusForbidden, err: "missing permission for GET /api/v2/users/me.json (HTTP 403)"},
		{status: http.StatusBadRequest, err: "GET /api/v2/users/me.json (HTTP 400)"},
	} {
		t.Run(http.StatusText(tt.status), func(t *testing.T) {
			srv, calls := statusServer(t, nil, `{}`, tt.status)
			var out map[string]any
			err := testSource(srv).apiClient(testCfg).get(t.Context(), "users/me.json", nil, &out)
			require.ErrorContains(t, err, tt.err)
			assert.Equal(t, int32(1), calls.Load())
		})
	}
}

func TestRetry(t *testing.T) {
	t.Run("5xx then 200", func(t *testing.T) {
		srv, calls := statusServer(t, nil, `{"ok":true}`, http.StatusServiceUnavailable)
		var out map[string]any
		require.NoError(t, testSource(srv).apiClient(testCfg).get(t.Context(), "users/me.json", nil, &out))
		assert.Equal(t, int32(2), calls.Load())
		assert.Equal(t, true, out["ok"])
	})

	t.Run("gives up", func(t *testing.T) {
		srv, calls := statusServer(t, nil, `{}`, repeat(http.StatusBadGateway, 10)...)
		var out map[string]any
		err := testSource(srv).apiClient(testCfg).get(t.Context(), "users/me.json", nil, &out)
		require.ErrorContains(t, err, "GET /api/v2/users/me.json (HTTP 502)")
		assert.Equal(t, int32(1+retries), calls.Load())
	})
}

func TestAuthHeader(t *testing.T) {
	var auth, query string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth, query = r.Header.Get("Authorization"), r.URL.RawQuery
		_, _ = w.Write([]byte(`{}`))
	}))
	t.Cleanup(srv.Close)
	var out map[string]any
	require.NoError(t, testSource(srv).apiClient(testCfg).get(t.Context(), "users/me.json", nil, &out))
	assert.Equal(t, testAuth, auth)
	assert.Empty(t, query)
}

func TestDefaultBaseURL(t *testing.T) {
	assert.Equal(t, "https://acme.zendesk.com/api/v2/", Source{}.apiClient(testCfg).baseURL)
	upper := testCfg
	upper.Subdomain = "ACME"
	assert.Equal(t, "https://acme.zendesk.com/api/v2/", Source{}.apiClient(upper).baseURL, "the subdomain is lowercased")
}
