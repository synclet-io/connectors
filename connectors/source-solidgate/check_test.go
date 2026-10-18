package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
	"github.com/synclet-io/synclet-go-sdk/testkit"
)

func runCheck(t *testing.T, src Source, secret string) *synclet.ConnectionStatus {
	t.Helper()
	cfg, err := json.Marshal(Config{Accounts: []Account{{Name: "acme_test", PublicKey: testPublicKey, SecretKey: secret}}})
	require.NoError(t, err)
	out, err := testkit.RunSource(t.Context(), src, synclet.Request{Command: synclet.CommandCheck, Config: cfg})
	require.NoError(t, err)
	require.NoError(t, out.Err)
	require.NotNil(t, out.ConnectionStatus())
	return out.ConnectionStatus()
}

func TestCheckErrors(t *testing.T) {
	const check = "card-orders 2026-10-18"
	tests := []struct {
		name     string
		secret   string
		override []fixture
		message  string
		requests int
	}{
		{
			name: "invalid key pair", secret: "api_sk_wrong",
			message:  `account "acme_test": invalid key pair: Solidgate rejected the public and secret key (POST /reports/card-orders (HTTP 401, code 1.01): Authentication failed)`,
			requests: 1,
		},
		{
			name: "invalid key pair in 200", secret: testSecretKey,
			override: []fixture{{status: http.StatusOK, file: "errors/invalid_key_pair.json"}},
			message:  `account "acme_test": invalid key pair: Solidgate rejected the public and secret key (POST /reports/card-orders (HTTP 200, code 1.01): Authentication failed)`,
			requests: 1,
		},
		{
			name: "WAF block", secret: testSecretKey,
			override: []fixture{{status: http.StatusForbidden, file: "errors/waf.html"}},
			message:  `account "acme_test": blocked by Solidgate WAF; check the API host (POST /reports/card-orders (HTTP 403))`,
			requests: 1,
		},
		{
			name: "rate limit", secret: testSecretKey,
			override: []fixture{{status: http.StatusTooManyRequests, file: "errors/rate_limited.json"}},
			message:  `account "acme_test": rate limited by Solidgate (POST /reports/card-orders (HTTP 429, code 5.07): Too many requests)`,
			requests: maxAttempts,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv, h := fixtureServer(t, map[string][]fixture{check: tt.override})
			status := runCheck(t, testSource(srv), tt.secret)
			assert.Equal(t, synclet.ConnectionStatusFailed, status.Status)
			assert.Equal(t, tt.message, status.Message)
			assert.Len(t, h.list(), tt.requests)
		})
	}
}

func TestCheckSucceeds(t *testing.T) {
	srv, h := fixtureServer(t, nil)
	status := runCheck(t, testSource(srv), testSecretKey)
	assert.Equal(t, synclet.ConnectionStatusSucceeded, status.Status)
	assert.Equal(t, []reportRequest{{DateFrom: "2026-10-18 11:00:00", DateTo: "2026-10-18 12:00:00", Filter: "updated_at", Limit: 1}}, h.requests())
}

func TestCheckWAFOnWrongHost(t *testing.T) {
	waf := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte("<html><body>Forbidden</body></html>"))
	}))
	t.Cleanup(waf.Close)
	src := testSource(waf)
	status := runCheck(t, src, testSecretKey)
	assert.Equal(t, synclet.ConnectionStatusFailed, status.Status)
	assert.Contains(t, status.Message, "blocked by Solidgate WAF; check the API host")
}
