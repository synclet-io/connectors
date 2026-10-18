package main

import (
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
)

const customersPath = "projects/" + testProj + "/customers"

func repeat(d time.Duration, n int) []time.Duration {
	out := make([]time.Duration, n)
	for i := range out {
		out[i] = d
	}
	return out
}

func TestFanOutPacing(t *testing.T) {
	srv, h := fixtureServer(t, nil)
	clk := newClock()
	src := testSource(srv, clk)
	out := read(t, src, testConfig, catalogOf(t, src, synclet.SyncModeFullRefresh,
		streamApps, streamProducts, streamCustomers, streamCustomerSubscriptions, streamCustomerPurchases, streamCustomerActiveEntitlements))
	require.NoError(t, out.Err)
	require.Len(t, h.list(), 4+14)

	// 4 project configuration requests at 60/min, then 14 customer
	// information requests at 480/min; the first of each domain waits for
	// nothing.
	want := append(repeat(time.Second, 3), repeat(125*time.Millisecond, 13)...)
	assert.Equal(t, want, clk.waits())
}

func TestRateLimitHeaderLowersLimit(t *testing.T) {
	tests := []struct {
		name  string
		usage string
		want  time.Duration
	}{
		{name: "limit from header", usage: "1", want: 250 * time.Millisecond},
		{name: "usage at the limit adds an interval", usage: "240", want: 500 * time.Millisecond},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv, _ := fixtureServer(t, map[string]fixture{customersPath: {
				status: http.StatusOK, file: customersPath + ".json", times: 1,
				headers: map[string]string{"RevenueCat-Rate-Limit-Current-Limit": "240", "RevenueCat-Rate-Limit-Current-Usage": tt.usage},
			}})
			clk := newClock()
			src := testSource(srv, clk)
			out := read(t, src, testConfig, catalogOf(t, src, synclet.SyncModeFullRefresh, streamCustomers))
			require.NoError(t, out.Err)
			assert.Equal(t, []time.Duration{tt.want}, clk.waits())
		})
	}
}

func TestRetryAfter429(t *testing.T) {
	tests := []struct {
		name    string
		headers map[string]string
		want    time.Duration
	}{
		{name: "Retry-After header", headers: map[string]string{"Retry-After": "7"}, want: 7 * time.Second},
		{name: "backoff_ms in the body", want: 3 * time.Second},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv, h := fixtureServer(t, map[string]fixture{customersPath: {
				status: http.StatusTooManyRequests, file: "errors/rate_limit.json", headers: tt.headers, times: 1,
			}})
			clk := newClock()
			src := testSource(srv, clk)
			out := read(t, src, testConfig, catalogOf(t, src, synclet.SyncModeFullRefresh, streamCustomers))
			require.NoError(t, out.Err)
			assert.Equal(t, []string{"cust_test_001", "cust_test_002", "cust_test_003"}, field(t, out, streamCustomers, "id"))
			assert.Equal(t, []time.Duration{tt.want, 125 * time.Millisecond}, clk.waits())
			assert.Equal(t, []string{customersPath + "?limit=100", customersPath + "?limit=100", customersPath + "?limit=100&starting_after=cust_test_002"}, h.list())
		})
	}

	t.Run("gives up after five waits", func(t *testing.T) {
		srv, h := fixtureServer(t, map[string]fixture{customersPath: {
			status: http.StatusTooManyRequests, file: "errors/rate_limit.json", headers: map[string]string{"Retry-After": "7"},
		}})
		src := testSource(srv, newClock())
		out := read(t, src, testConfig, catalogOf(t, src, synclet.SyncModeFullRefresh, streamCustomers))
		require.Error(t, out.Err)
		assert.Contains(t, out.Err.Error(), "gave up after 5 waits")
		assert.Len(t, h.list(), 6)
		assert.Empty(t, out.Records(synclet.StreamDescriptor{Name: streamCustomers}))
	})
}

func TestServerErrorRetries(t *testing.T) {
	t.Run("recovers", func(t *testing.T) {
		srv, h := fixtureServer(t, map[string]fixture{customersPath: {status: http.StatusInternalServerError, file: "errors/server_error.json", times: 2}})
		src := testSource(srv, newClock())
		out := read(t, src, testConfig, catalogOf(t, src, synclet.SyncModeFullRefresh, streamCustomers))
		require.NoError(t, out.Err)
		assert.Equal(t, []string{"cust_test_001", "cust_test_002", "cust_test_003"}, field(t, out, streamCustomers, "id"))
		assert.Len(t, h.list(), 4)
	})
	t.Run("gives up", func(t *testing.T) {
		srv, h := fixtureServer(t, map[string]fixture{customersPath: {status: http.StatusInternalServerError, file: "errors/server_error.json"}})
		src := testSource(srv, newClock())
		out := read(t, src, testConfig, catalogOf(t, src, synclet.SyncModeFullRefresh, streamCustomers))
		require.Error(t, out.Err)
		assert.Contains(t, out.Err.Error(), "HTTP 500")
		assert.Len(t, h.list(), 4)
	})
	t.Run("does not retry a 401", func(t *testing.T) {
		srv, h := fixtureServer(t, nil)
		src := testSource(srv, newClock())
		out := read(t, src, `{"api_key":"wrong-key","project_id":"proj_test1"}`, catalogOf(t, src, synclet.SyncModeFullRefresh, streamCustomers))
		require.Error(t, out.Err)
		assert.Len(t, h.list(), 1)
	})
}
