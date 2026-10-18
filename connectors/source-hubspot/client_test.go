package main

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// statusServer answers the n-th request with statuses[n] and body, and 200
// with {} once statuses run out; the header goes with every non-200
// response. It counts the requests.
func statusServer(t *testing.T, header http.Header, body string, statuses ...int) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	var n atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		i := int(n.Add(1)) - 1
		if i < len(statuses) {
			for k, v := range header {
				w.Header()[k] = v
			}
			w.WriteHeader(statuses[i])
			_, _ = w.Write([]byte(body))
			return
		}
		_, _ = w.Write([]byte(`{}`))
	}))
	t.Cleanup(srv.Close)
	return srv, &n
}

func clientOf(srv *httptest.Server, w *waits) *apiClient {
	src := testSource(srv)
	src.wait = w.wait
	return src.apiClient(Config{AccessToken: testToken})
}

func TestRateLimit(t *testing.T) {
	const secondly = `{"status":"error","message":"You have reached your secondly limit.","errorType":"RATE_LIMIT","policyName":"SECONDLY"}`

	t.Run("Retry-After is waited for", func(t *testing.T) {
		srv, n := statusServer(t, http.Header{"Retry-After": {"3"}}, secondly, http.StatusTooManyRequests)
		w := &waits{}
		var out struct{}
		require.NoError(t, clientOf(srv, w).get(t.Context(), "crm/v3/owners", nil, &out))
		assert.Equal(t, int32(2), n.Load())
		assert.Contains(t, w.list(), 3*time.Second)
	})

	t.Run("a 429 without Retry-After waits 10 s", func(t *testing.T) {
		srv, _ := statusServer(t, nil, secondly, http.StatusTooManyRequests)
		w := &waits{}
		var out struct{}
		require.NoError(t, clientOf(srv, w).get(t.Context(), "crm/v3/owners", nil, &out))
		assert.Contains(t, w.list(), 10*time.Second)
	})

	t.Run("gives up after five 429", func(t *testing.T) {
		srv, n := statusServer(t, nil, secondly, repeat(http.StatusTooManyRequests, 6)...)
		var out struct{}
		err := clientOf(srv, &waits{}).get(t.Context(), "crm/v3/owners", nil, &out)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "gave up after 5 waits")
		assert.Equal(t, int32(6), n.Load())
	})

	t.Run("the daily limit fails at once", func(t *testing.T) {
		srv, n := statusServer(t, nil, `{"status":"error","message":"You have reached your daily limit.","policyName":"DAILY"}`, http.StatusTooManyRequests)
		var out struct{}
		err := clientOf(srv, &waits{}).get(t.Context(), "crm/v3/owners", nil, &out)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "daily API limit")
		assert.Equal(t, int32(1), n.Load())
	})

	t.Run("5xx is retried with a doubling delay", func(t *testing.T) {
		srv, n := statusServer(t, nil, `{"status":"error","message":"internal error"}`, http.StatusBadGateway, http.StatusInternalServerError)
		w := &waits{}
		var out struct{}
		require.NoError(t, clientOf(srv, w).get(t.Context(), "crm/v3/owners", nil, &out))
		assert.Equal(t, int32(3), n.Load())
		pace := defaultWindow / defaultMax
		assert.Equal(t, []time.Duration{time.Millisecond, pace, 2 * time.Millisecond, pace}, w.list(), "each retry waits its delay and the pacing interval")
	})

	t.Run("5xx gives up after three retries", func(t *testing.T) {
		srv, n := statusServer(t, nil, `{"status":"error","message":"internal error"}`, repeat(http.StatusServiceUnavailable, 4)...)
		var out struct{}
		err := clientOf(srv, &waits{}).get(t.Context(), "crm/v3/owners", nil, &out)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "HTTP 503")
		assert.Equal(t, int32(4), n.Load())
	})

	for _, status := range []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusBadRequest} {
		t.Run(http.StatusText(status)+" is not retried", func(t *testing.T) {
			srv, n := statusServer(t, nil, `{"status":"error","message":"no"}`, status)
			var out struct{}
			require.Error(t, clientOf(srv, &waits{}).get(t.Context(), "crm/v3/owners", nil, &out))
			assert.Equal(t, int32(1), n.Load())
		})
	}

	t.Run("search requests are spaced 250 ms", func(t *testing.T) {
		srv, _ := statusServer(t, nil, "")
		w := &waits{}
		c := clientOf(srv, w)
		var out struct{}
		require.NoError(t, c.post(t.Context(), "crm/v3/objects/deals/search", nil, map[string]any{}, &out))
		require.NoError(t, c.post(t.Context(), "crm/v3/objects/deals/search", nil, map[string]any{}, &out))
		assert.Equal(t, []time.Duration{searchInterval}, w.list())
	})

	t.Run("general requests follow the rate limit headers", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("X-HubSpot-RateLimit-Interval-Milliseconds", "10000")
			w.Header().Set("X-HubSpot-RateLimit-Max", "200")
			_, _ = w.Write([]byte(`{}`))
		}))
		t.Cleanup(srv.Close)
		w := &waits{}
		c := clientOf(srv, w)
		var out struct{}
		require.NoError(t, c.get(t.Context(), "crm/v3/owners", nil, &out))
		require.NoError(t, c.get(t.Context(), "crm/v3/owners", nil, &out))
		require.NoError(t, c.post(t.Context(), "crm/v3/objects/deals/batch/read", nil, map[string]any{}, &out))
		assert.Equal(t, []time.Duration{50 * time.Millisecond, 50 * time.Millisecond}, w.list())
	})
}

func repeat(status, n int) []int {
	out := make([]int, n)
	for i := range out {
		out[i] = status
	}
	return out
}

func TestAuthHeader(t *testing.T) {
	srv, h := fixtureServer(t, nil)
	var out struct{}
	require.NoError(t, testSource(srv).apiClient(Config{AccessToken: testToken}).get(t.Context(), "crm/v3/owners", nil, &out))
	assert.Len(t, h.list(), 1)
}

func TestDefaultBaseURL(t *testing.T) {
	c := Source{}.apiClient(Config{AccessToken: testToken})
	assert.Equal(t, "https://api.hubapi.com/", c.baseURL)
	assert.Equal(t, defaultWindow/defaultMax, c.interval)
}
