package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
)

func TestRateLimitRetryAfter(t *testing.T) {
	srv, h := fixtureServer(t, map[string]fixture{
		"engage/revenue": {status: http.StatusTooManyRequests, file: "errors/rate_limit.json", headers: map[string]string{"Retry-After": "90"}, times: 1},
	})
	clk := newClock()
	src := testSource(srv, clk)
	cfg := testConfig(t, func(c *Config) { c.StartDate = "2026-10-15" })
	cat := catalogOf(t, src, cfg, synclet.SyncModeFullRefresh, streamRevenue)
	before := len(h.list())
	out := read(t, src, cfg, cat, nil)
	require.NoError(t, out.Err)
	assert.Equal(t, []time.Duration{90 * time.Second}, clk.waits())
	assert.Len(t, h.list()[before:], 2)
	assert.Len(t, records(t, out, streamRevenue), 4)
}

func TestRateLimitBackoff(t *testing.T) {
	srv, h := fixtureServer(t, map[string]fixture{
		"engage/revenue": {status: http.StatusTooManyRequests, file: "errors/rate_limit.json"},
	})
	clk := newClock()
	src := testSource(srv, clk)
	cfg := testConfig(t, func(c *Config) { c.StartDate = "2026-10-15" })
	cat := catalogOf(t, src, cfg, synclet.SyncModeFullRefresh, streamRevenue)
	before := len(h.list())
	out := read(t, src, cfg, cat, nil)
	require.Error(t, out.Err)
	assert.Contains(t, out.Err.Error(), "gave up after 8 waits")
	assert.Contains(t, out.Err.Error(), "limit is 60 queries per hour")
	assert.Len(t, h.list()[before:], 9)
	m := time.Minute
	assert.Equal(t, []time.Duration{m, 2 * m, 4 * m, 8 * m, 15 * m, 15 * m, 15 * m, 15 * m}, clk.waits())
}

func TestServerErrorRetried(t *testing.T) {
	srv, h := fixtureServer(t, map[string]fixture{
		"funnels/list": {status: http.StatusBadGateway, file: "errors/server.json", times: 2},
	})
	clk := newClock()
	src := testSource(srv, clk)
	cfg := testConfig(t, func(c *Config) { c.StartDate = "2026-10-18" })
	cat := catalogOf(t, src, cfg, synclet.SyncModeFullRefresh, streamFunnels)
	before := len(h.list())
	out := read(t, src, cfg, cat, nil)
	require.NoError(t, out.Err)
	assert.Equal(t, []time.Duration{time.Millisecond, 2 * time.Millisecond}, clk.waits())
	assert.Equal(t, []string{"query funnels/list", "query funnels/list", "query funnels/list"}, h.list()[before:before+3])
	assert.Len(t, records(t, out, streamFunnels), 2)
}

func TestBadRequestNotRetried(t *testing.T) {
	srv, h := fixtureServer(t, map[string]fixture{
		"funnels/list": {status: http.StatusBadRequest, file: "errors/server.json"},
	})
	src := testSource(srv, newClock())
	cfg := testConfig(t)
	cat := catalogOf(t, src, cfg, synclet.SyncModeFullRefresh, streamFunnels)
	before := len(h.list())
	out := read(t, src, cfg, cat, nil)
	require.Error(t, out.Err)
	assert.Contains(t, out.Err.Error(), "Query API funnels/list: HTTP 400: Internal server error")
	assert.Len(t, h.list()[before:], 1)
}

func TestForbiddenIsConfigError(t *testing.T) {
	srv, _ := fixtureServer(t, map[string]fixture{
		"annotations": {status: http.StatusForbidden, file: "errors/forbidden.json"},
	})
	src := testSource(srv, newClock())
	cfg := testConfig(t)
	out := read(t, src, cfg, catalogOf(t, src, cfg, synclet.SyncModeFullRefresh, streamAnnotations), nil)
	require.Error(t, out.Err)
	assert.Contains(t, out.Err.Error(), "Permission denied")
	require.NotEmpty(t, out.ErrorTraces())
	assert.Equal(t, synclet.FailureTypeConfigError, out.ErrorTraces()[0].FailureType)
}

func TestTransportErrorRetried(t *testing.T) {
	var n atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if n.Add(1) == 1 {
			hj, _ := w.(http.Hijacker)
			conn, _, _ := hj.Hijack()
			_ = conn.Close()
			return
		}
		_, _ = w.Write([]byte(`[]`))
	}))
	t.Cleanup(srv.Close)
	clk := newClock()
	var cfg Config
	require.NoError(t, json.Unmarshal(testConfig(t), &cfg))
	c := testSource(srv, clk).apiClient(cfg)
	var out []any
	require.NoError(t, c.getJSON(t.Context(), get(queryAPI, "funnels/list", nil), &out))
	assert.Equal(t, int32(2), n.Load())
	assert.Equal(t, []time.Duration{time.Millisecond}, clk.waits())
}

func TestLimiterHourlyBudget(t *testing.T) {
	clk := newClock()
	l := &limiter{limit: 3, window: time.Hour}
	ctx := context.Background()
	for range 3 {
		require.NoError(t, l.next(ctx, clk.now, clk.wait))
		require.NoError(t, clk.wait(ctx, 10*time.Minute))
	}
	require.NoError(t, l.next(ctx, clk.now, clk.wait))
	// The first request was 30 minutes ago: the fourth waits 30 more.
	assert.Equal(t, 30*time.Minute, clk.waits()[3])
	require.NoError(t, l.next(ctx, clk.now, clk.wait))
	assert.Equal(t, 10*time.Minute, clk.waits()[4])
}
