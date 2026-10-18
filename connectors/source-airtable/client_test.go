package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var testCfg = Config{PersonalAccessToken: testToken}

// fakeClock is a clock that moves only when the client waits.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
	ds  []time.Duration
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) wait(ctx context.Context, d time.Duration) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ds = append(c.ds, d)
	c.now = c.now.Add(d)
	return ctx.Err()
}

func (c *fakeClock) waits() []time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]time.Duration(nil), c.ds...)
}

// statusServer answers the first len(statuses) requests with those statuses
// and the body of an Airtable error, and every later one with 200 and body.
func statusServer(t *testing.T, body string, statuses ...int) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := int(calls.Add(1))
		if n <= len(statuses) {
			w.WriteHeader(statuses[n-1])
			_, _ = w.Write([]byte(`{"error":{"type":"SOME_ERROR","message":"something failed"}}`))
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

func clockedClient(srv *httptest.Server) (*apiClient, *fakeClock) {
	clock := &fakeClock{now: testNow}
	src := testSource(srv)
	src.now = clock.Now
	src.wait = clock.wait
	return src.apiClient(testCfg), clock
}

func TestRateLimit(t *testing.T) {
	t.Run("429 waits 30 s and repeats", func(t *testing.T) {
		srv, calls := statusServer(t, `{}`, http.StatusTooManyRequests)
		c, clock := clockedClient(srv)
		var out map[string]any
		require.NoError(t, c.get(t.Context(), base1, base1+"/tblTasks00000001", nil, &out))
		assert.Equal(t, []time.Duration{30 * time.Second}, clock.waits())
		assert.Equal(t, int32(2), calls.Load())
	})

	t.Run("five 429s are waited out", func(t *testing.T) {
		srv, calls := statusServer(t, `{}`, repeat(http.StatusTooManyRequests, 5)...)
		c, clock := clockedClient(srv)
		var out map[string]any
		require.NoError(t, c.get(t.Context(), base1, base1+"/tblTasks00000001", nil, &out))
		assert.Len(t, clock.waits(), 5)
		assert.Equal(t, int32(6), calls.Load())
	})

	t.Run("a sixth 429 fails", func(t *testing.T) {
		srv, calls := statusServer(t, `{}`, repeat(http.StatusTooManyRequests, 6)...)
		c, clock := clockedClient(srv)
		var out map[string]any
		err := c.get(t.Context(), base1, base1+"/tblTasks00000001", nil, &out)
		require.ErrorContains(t, err, "GET /v0/appBase0000000001/tblTasks00000001 (HTTP 429): rate limited; gave up after 5 waits of 30s")
		assert.Equal(t, int32(6), calls.Load())
		assert.Equal(t, repeat30s(5), clock.waits())
	})

	t.Run("requests to one base are spaced by 200 ms, other bases are not", func(t *testing.T) {
		srv, _ := statusServer(t, `{}`)
		c, clock := clockedClient(srv)
		var out map[string]any
		for _, b := range []string{base1, base1, base2, base1} {
			require.NoError(t, c.get(t.Context(), b, "meta/bases/"+b+"/tables", nil, &out))
		}
		assert.Equal(t, []time.Duration{200 * time.Millisecond, 200 * time.Millisecond}, clock.waits())
	})
}

func repeat30s(n int) []time.Duration {
	out := make([]time.Duration, n)
	for i := range out {
		out[i] = 30 * time.Second
	}
	return out
}

func TestRetries(t *testing.T) {
	t.Run("500 is repeated", func(t *testing.T) {
		srv, calls := statusServer(t, `{"ok":true}`, http.StatusInternalServerError, http.StatusBadGateway)
		c, _ := clockedClient(srv)
		var out map[string]any
		require.NoError(t, c.get(t.Context(), "", "meta/whoami", nil, &out))
		assert.Equal(t, int32(3), calls.Load())
	})

	t.Run("500 four times fails", func(t *testing.T) {
		srv, calls := statusServer(t, `{}`, repeat(http.StatusInternalServerError, 4)...)
		c, _ := clockedClient(srv)
		var out map[string]any
		err := c.get(t.Context(), "", "meta/whoami", nil, &out)
		require.EqualError(t, err, "GET /v0/meta/whoami (HTTP 500): SOME_ERROR: something failed")
		assert.Equal(t, int32(4), calls.Load())
	})

	t.Run("401 is requested once", func(t *testing.T) {
		srv, calls := statusServer(t, `{}`, http.StatusUnauthorized)
		c, _ := clockedClient(srv)
		var out map[string]any
		err := c.get(t.Context(), "", "meta/whoami", nil, &out)
		require.EqualError(t, err, "invalid personal access token (HTTP 401): SOME_ERROR: something failed")
		assert.Equal(t, int32(1), calls.Load())
	})

	t.Run("404 is requested once", func(t *testing.T) {
		srv, calls := statusServer(t, `{}`, http.StatusNotFound)
		c, _ := clockedClient(srv)
		var out map[string]any
		err := c.get(t.Context(), base1, "meta/bases/"+base1+"/tables", nil, &out)
		assert.Equal(t, http.StatusNotFound, statusOf(err))
		assert.Equal(t, int32(1), calls.Load())
	})
}

func TestErrorDetail(t *testing.T) {
	tests := map[string]string{
		`{"error":{"type":"INVALID_PERMISSIONS","message":"no access"}}`: "INVALID_PERMISSIONS: no access",
		`{"error":{"type":"INVALID_PERMISSIONS"}}`:                       "INVALID_PERMISSIONS",
		`{"error":{"message":"no access"}}`:                              "no access",
		`{"error":"NOT_FOUND"}`:                                          "NOT_FOUND",
		`{}`:                                                             "",
		`<html>`:                                                         "",
	}
	for body, want := range tests {
		assert.Equal(t, want, errorDetail([]byte(body)), body)
	}
}
