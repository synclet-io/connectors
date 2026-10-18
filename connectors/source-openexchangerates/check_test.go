package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
	"github.com/synclet-io/synclet-go-sdk/testkit"
)

func runCheck(t *testing.T, src Source, appID string) (*synclet.ConnectionStatus, error) {
	t.Helper()
	cfg, err := json.Marshal(Config{AppID: appID, StartDate: "2026-10-14", Base: "EUR"})
	require.NoError(t, err)
	out, err := testkit.RunSource(t.Context(), src, synclet.Request{Command: synclet.CommandCheck, Config: cfg})
	require.NoError(t, err)
	return out.ConnectionStatus(), out.Err
}

func TestCheckErrors(t *testing.T) {
	tests := []struct {
		name     string
		appID    string
		override map[string]fixture
		message  string
	}{
		{name: "401", appID: "wrong-app-id", message: "invalid app_id (HTTP 401 invalid_app_id): Invalid App ID provided."},
		{
			name: "429", appID: testAppID,
			override: map[string]fixture{"latest.json": {status: http.StatusTooManyRequests, file: "errors/not_allowed.json"}},
			message:  "plan does not allow this route (HTTP 429 not_allowed): Changing the API `base` currency",
		},
		{
			name: "403", appID: testAppID,
			override: map[string]fixture{"latest.json": {status: http.StatusForbidden, file: "errors/access_restricted.json"}},
			message:  "access restricted (HTTP 403 access_restricted): Access restricted",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv, h := fixtureServer(t, tt.override)
			status, err := runCheck(t, testSource(srv), tt.appID)
			require.NoError(t, err)
			require.NotNil(t, status)
			assert.Equal(t, synclet.ConnectionStatusFailed, status.Status)
			assert.Contains(t, status.Message, tt.message)
			assert.Equal(t, []string{"latest.json"}, h.list(), "an error status is not retried")
		})
	}
}

func TestCheckSucceeds(t *testing.T) {
	var query string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		query = r.URL.RawQuery
		_, _ = w.Write([]byte(`{"timestamp":1,"base":"EUR","rates":{}}`))
	}))
	t.Cleanup(srv.Close)
	status, err := runCheck(t, testSource(srv), testAppID)
	require.NoError(t, err)
	assert.Equal(t, synclet.ConnectionStatusSucceeded, status.Status)
	assert.Equal(t, "base=EUR", query)
}

func TestRetry(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"error":true,"status":503,"message":"unavailable","description":"Try again."}`))
			return
		}
		_, _ = w.Write([]byte(`{"timestamp":1,"base":"USD","rates":{"EUR":0.9}}`))
	}))
	t.Cleanup(srv.Close)
	var r ratesResponse
	require.NoError(t, testSource(srv).apiClient(Config{AppID: testAppID}).get(t.Context(), "latest.json", nil, &r))
	assert.Equal(t, int32(2), calls.Load())
	assert.Equal(t, "0.9", r.Rates["EUR"].String())
}

func TestRetryGivesUp(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusBadGateway)
	}))
	t.Cleanup(srv.Close)
	var r ratesResponse
	err := testSource(srv).apiClient(Config{AppID: testAppID}).get(t.Context(), "latest.json", nil, &r)
	require.ErrorContains(t, err, "GET /api/latest.json (HTTP 502)")
	assert.Equal(t, int32(1+retries), calls.Load())
}
