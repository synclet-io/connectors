package main

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
	"github.com/synclet-io/synclet-go-sdk/testkit"
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
	return src.apiClient(testCfg, 0), clock
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
		require.ErrorContains(t, err, "GET /v0/appBase0000000001/tblTasks00000001 (HTTP 429): SOME_ERROR: something failed; gave up after 5 waits of 30s")
		assert.Equal(t, int32(6), calls.Load())
		assert.Equal(t, repeat30s(5), clock.waits())
	})

	t.Run("requests to one base are spaced by 200 ms, any two requests by 20 ms", func(t *testing.T) {
		srv, _ := statusServer(t, `{}`)
		c, clock := clockedClient(srv)
		var out map[string]any
		for _, b := range []string{base1, base1, base2, base1} {
			require.NoError(t, c.get(t.Context(), b, "meta/bases/"+b+"/tables", nil, &out))
		}
		assert.Equal(t, []time.Duration{200 * time.Millisecond, 20 * time.Millisecond, 180 * time.Millisecond}, clock.waits())
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

// errorServer answers the first len(statuses) requests with those statuses
// and errBody, and every later one with 200 and body.
func errorServer(t *testing.T, errBody, body string, statuses ...int) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := int(calls.Add(1))
		if n <= len(statuses) {
			w.WriteHeader(statuses[n-1])
			_, _ = w.Write([]byte(errBody))
			return
		}
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	return srv, &calls
}

func logged(t *testing.T) (context.Context, *bytes.Buffer) {
	t.Helper()
	var buf bytes.Buffer
	l := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	return synclet.WithLogger(t.Context(), l), &buf
}

const rateLimitBody = `{"errors":[{"error":"RATE_LIMIT_REACHED","message":"Rate limit exceeded. Please try again later"}]}`

func TestBillingLimit(t *testing.T) {
	bodies := map[string]string{
		"errors list":  `{"errors":[{"error":"PUBLIC_API_BILLING_LIMIT_EXCEEDED","message":"over the limit"}]}`,
		"error object": `{"error":{"type":"PUBLIC_API_BILLING_LIMIT_EXCEEDED","message":"over the limit"}}`,
		"error code":   `{"error":"PUBLIC_API_BILLING_LIMIT_EXCEEDED"}`,
	}
	for name, body := range bodies {
		t.Run(name, func(t *testing.T) {
			srv, calls := errorServer(t, body, `{}`, repeat(http.StatusTooManyRequests, 6)...)
			c, clock := clockedClient(srv)
			var out map[string]any
			err := c.get(t.Context(), base1, "meta/bases/"+base1+"/tables", nil, &out)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "the Airtable workspace has used up the API calls its plan allows")
			assert.Contains(t, err.Error(), "PUBLIC_API_BILLING_LIMIT_EXCEEDED")
			assert.Equal(t, int32(1), calls.Load(), "a used-up API call limit is not retried")
			assert.Empty(t, clock.waits())
		})
	}

	t.Run("discover fails with a config error", func(t *testing.T) {
		srv, h := fixtureServer(t, map[string]fixture{"meta/bases/" + base2 + "/tables": {status: http.StatusTooManyRequests, file: "errors/billing-limit.json"}})
		raw, err := json.Marshal(testCfg)
		require.NoError(t, err)
		out, err := testkit.RunSource(t.Context(), testSource(srv), synclet.Request{Command: synclet.CommandDiscover, Config: raw})
		require.NoError(t, err)
		require.Error(t, out.Err)
		traces := out.ErrorTraces()
		require.NotEmpty(t, traces)
		assert.Equal(t, synclet.FailureTypeConfigError, traces[0].FailureType)
		assert.Contains(t, traces[0].Message, "PUBLIC_API_BILLING_LIMIT_EXCEEDED")
		assert.Equal(t, 1, count(h.list(), "meta/bases/"+base2+"/tables"))
	})
}

func count(list []string, s string) int {
	n := 0
	for _, v := range list {
		if v == s {
			n++
		}
	}
	return n
}

func TestRateLimitLogs(t *testing.T) {
	srv, _ := errorServer(t, rateLimitBody, `{}`, repeat(http.StatusTooManyRequests, 2)...)
	c, _ := clockedClient(srv)
	ctx, buf := logged(t)
	var out map[string]any
	require.NoError(t, c.get(ctx, base1, base1+"/tblTasks00000001", url.Values{"offset": {"itrSecret/rec1"}}, &out))
	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	require.Len(t, lines, 2, buf.String())
	assert.Contains(t, lines[0], "path=/v0/appBase0000000001/tblTasks00000001 ")
	assert.Contains(t, lines[0], "attempt=1/5")
	assert.Contains(t, lines[0], "wait=30s")
	assert.Contains(t, lines[0], "RATE_LIMIT_REACHED")
	assert.Contains(t, lines[1], "attempt=2/5")
	assert.NotContains(t, buf.String(), "itrSecret")
	assert.NotContains(t, buf.String(), testToken)
}

func TestRateLimitBudget(t *testing.T) {
	t.Run("check and discover give up once the budget is spent", func(t *testing.T) {
		srv, calls := errorServer(t, rateLimitBody, `{}`, repeat(http.StatusTooManyRequests, 6)...)
		c, clock := clockedClient(srv)
		c.rateLimitBudget = metadataRateLimitBudget
		var out map[string]any
		err := c.get(t.Context(), base1, "meta/bases/"+base1+"/tables", nil, &out)
		require.ErrorContains(t, err, "GET /v0/meta/bases/appBase0000000001/tables (HTTP 429): RATE_LIMIT_REACHED: Rate limit exceeded. Please try again later; gave up after waiting 1m0s in total for Airtable's rate limit")
		assert.Equal(t, repeat30s(2), clock.waits())
		assert.Equal(t, int32(3), calls.Load())
	})

	t.Run("the budget is shared by every request of a run", func(t *testing.T) {
		srv, calls := errorServer(t, rateLimitBody, `{}`, http.StatusTooManyRequests, http.StatusTooManyRequests, http.StatusOK, http.StatusTooManyRequests)
		c, clock := clockedClient(srv)
		c.rateLimitBudget = metadataRateLimitBudget
		var out map[string]any
		require.NoError(t, c.get(t.Context(), base1, "meta/bases/"+base1+"/tables", nil, &out))
		err := c.get(t.Context(), base2, "meta/bases/"+base2+"/tables", nil, &out)
		require.ErrorContains(t, err, "gave up after waiting 1m0s in total")
		assert.Equal(t, int32(4), calls.Load())
		assert.Len(t, clock.waits(), 3, "two rate-limit waits and the spacing of base2's request")
	})

	t.Run("discover fails within the budget", func(t *testing.T) {
		srv, h := fixtureServer(t, map[string]fixture{"meta/bases/" + base2 + "/tables": {status: http.StatusTooManyRequests, file: "errors/rate-limited.json"}})
		clock := &fakeClock{now: testNow}
		src := testSource(srv)
		src.now = clock.Now
		src.wait = clock.wait
		out := discover(t, src, testCfg)
		require.ErrorContains(t, out.Err, "gave up after waiting 1m0s in total")
		assert.Equal(t, 3, count(h.list(), "meta/bases/"+base2+"/tables"))
		var limited time.Duration
		for _, d := range clock.waits() {
			if d == rateLimitWait {
				limited += d
			}
		}
		assert.Equal(t, time.Minute, limited)
	})
}

func TestSpacingIsSafeForConcurrentRequests(t *testing.T) {
	srv, _ := statusServer(t, `{}`)
	c, _ := clockedClient(srv)
	c.wait = func(ctx context.Context, _ time.Duration) error { return ctx.Err() }
	type slot struct {
		base string
		at   time.Time
	}
	var mu sync.Mutex
	var slots []slot
	var wg sync.WaitGroup
	for i := range 40 {
		b := []string{base1, base2, base3, ""}[i%4]
		wg.Go(func() {
			at := c.reserve(b)
			mu.Lock()
			slots = append(slots, slot{b, at})
			mu.Unlock()
		})
	}
	wg.Wait()
	sort.Slice(slots, func(i, j int) bool { return slots[i].at.Before(slots[j].at) })
	last := map[string]time.Time{}
	for i, s := range slots {
		if i > 0 {
			assert.GreaterOrEqual(t, s.at.Sub(slots[i-1].at), tokenSpacing, "requests of one token are %s apart", tokenSpacing)
		}
		if l, ok := last[s.base]; ok {
			assert.GreaterOrEqual(t, s.at.Sub(l), requestSpacing, "requests to base %q are %s apart", s.base, requestSpacing)
		}
		last[s.base] = s.at
	}
}
