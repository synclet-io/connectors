package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
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

// statusServer answers the first len(statuses) requests with those statuses,
// a Notion error body and, on a 429, retryAfter as Retry-After; every later
// request gets 200 and body.
func statusServer(t *testing.T, body, retryAfter string, statuses ...int) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := int(calls.Add(1))
		if n <= len(statuses) {
			if statuses[n-1] == http.StatusTooManyRequests && retryAfter != "" {
				w.Header().Set("Retry-After", retryAfter)
			}
			w.WriteHeader(statuses[n-1])
			_, _ = w.Write([]byte(`{"object":"error","status":0,"code":"some_error","message":"something failed"}`))
			return
		}
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	return srv, &calls
}

func clockClient(srv *httptest.Server, clock *fakeClock) *apiClient {
	return Source{
		baseURL: srv.URL + "/v1/", client: srv.Client(), retryDelay: time.Millisecond,
		now: clock.Now, wait: clock.wait,
	}.apiClient(Config{AccessToken: testToken})
}

func repeat(n, status int) []int {
	out := make([]int, n)
	for i := range out {
		out[i] = status
	}
	return out
}

func TestClientRetryAfter(t *testing.T) {
	srv, calls := statusServer(t, `{"id":"x"}`, "2", http.StatusTooManyRequests)
	clock := &fakeClock{now: testNow}
	var out struct{ ID string }
	require.NoError(t, clockClient(srv, clock).get(t.Context(), "users/me", nil, &out))
	assert.Equal(t, "x", out.ID)
	assert.Equal(t, int32(2), calls.Load())
	assert.Equal(t, []time.Duration{2 * time.Second}, clock.waits())
}

func TestClientRetryAfterDefault(t *testing.T) {
	srv, _ := statusServer(t, `{}`, "soon", http.StatusTooManyRequests)
	clock := &fakeClock{now: testNow}
	var out struct{}
	require.NoError(t, clockClient(srv, clock).get(t.Context(), "users/me", nil, &out))
	assert.Equal(t, []time.Duration{time.Second}, clock.waits())
}

func TestClientRateLimitGivesUp(t *testing.T) {
	srv, calls := statusServer(t, `{}`, "1", repeat(6, http.StatusTooManyRequests)...)
	clock := &fakeClock{now: testNow}
	var out struct{}
	err := clockClient(srv, clock).post(t.Context(), "search", map[string]any{}, &out)
	require.Error(t, err)
	assert.Equal(t, "POST /v1/search (HTTP 429): rate limited; gave up after 5 waits", err.Error())
	assert.Equal(t, int32(6), calls.Load())
}

func TestClientServerErrorRetried(t *testing.T) {
	srv, calls := statusServer(t, `{"id":"x"}`, "", http.StatusInternalServerError, http.StatusBadGateway)
	var out struct{ ID string }
	require.NoError(t, clockClient(srv, &fakeClock{now: testNow}).get(t.Context(), "users/me", nil, &out))
	assert.Equal(t, int32(3), calls.Load())
}

func TestClientServerErrorGivesUp(t *testing.T) {
	srv, calls := statusServer(t, `{}`, "", repeat(4, http.StatusServiceUnavailable)...)
	var out struct{}
	err := clockClient(srv, &fakeClock{now: testNow}).get(t.Context(), "users/me", nil, &out)
	require.EqualError(t, err, "GET /v1/users/me (HTTP 503): some_error: something failed")
	assert.Equal(t, int32(4), calls.Load())
}

func TestClientClientErrorNotRetried(t *testing.T) {
	for _, tt := range []struct {
		status int
		msg    string
	}{
		{http.StatusBadRequest, "POST /v1/search (HTTP 400): some_error: something failed"},
		{http.StatusNotFound, "POST /v1/search (HTTP 404): some_error: something failed"},
		{http.StatusUnauthorized, "integration token is invalid (HTTP 401): some_error: something failed"},
		{http.StatusForbidden, "the integration lacks a capability for POST /v1/search (HTTP 403): some_error: something failed"},
	} {
		srv, calls := statusServer(t, `{}`, "", tt.status)
		var out struct{}
		err := clockClient(srv, &fakeClock{now: testNow}).post(t.Context(), "search", map[string]any{}, &out)
		require.Error(t, err)
		assert.Equal(t, tt.msg, err.Error())
		assert.Equal(t, int32(1), calls.Load())
	}
}

func TestClientSpacing(t *testing.T) {
	srv, _ := statusServer(t, `{}`, "")
	clock := &fakeClock{now: testNow}
	c := clockClient(srv, clock)
	var out struct{}
	require.NoError(t, c.get(t.Context(), "users/me", nil, &out))
	require.NoError(t, c.get(t.Context(), "users/me", nil, &out))
	clock.advance(100 * time.Millisecond)
	require.NoError(t, c.get(t.Context(), "users/me", nil, &out))
	clock.advance(time.Second)
	require.NoError(t, c.get(t.Context(), "users/me", nil, &out))
	assert.Equal(t, []time.Duration{350 * time.Millisecond, 250 * time.Millisecond}, clock.waits())
}

func TestClientHasMoreWithoutCursor(t *testing.T) {
	srv, _ := fixtureServer(t, map[string]fixture{"users": {status: http.StatusOK, file: "errors/has_more_without_cursor.json"}})
	c := testSource(srv).apiClient(Config{AccessToken: testToken})
	err := c.getAll(t.Context(), "users", func(json.RawMessage) (bool, error) { return true, nil })
	require.EqualError(t, err, "GET users: has_more is true but next_cursor is empty")
}
