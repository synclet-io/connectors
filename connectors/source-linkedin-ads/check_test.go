package main

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
	"github.com/synclet-io/synclet-go-sdk/testkit"
)

func runCheck(t *testing.T, src Source, cfg json.RawMessage) *synclet.ConnectionStatus {
	t.Helper()
	out, err := testkit.RunSource(t.Context(), src, synclet.Request{Command: synclet.CommandCheck, Config: cfg})
	require.NoError(t, err)
	require.NoError(t, out.Err)
	status := out.ConnectionStatus()
	require.NotNil(t, status)
	return status
}

func TestCheck(t *testing.T) {
	tests := []struct {
		name      string
		edit      func(*Config)
		overrides map[string][]fixture
		message   string
		routes    []string
	}{
		{name: "succeeds", routes: []string{tokenRoute, "adAccounts", "adAccounts/1001", "adAccounts/1002"}},
		{
			name: "invalid or expired refresh token", edit: func(c *Config) { c.RefreshToken = "revoked-refresh-token" },
			message: "refresh_token is invalid or expired; authorize the app again for a new one (linkedin token endpoint: HTTP 400 invalid_request: The provided authorization grant or refresh token is invalid, expired or revoked)",
			routes:  []string{tokenRoute},
		},
		{
			name: "invalid client", edit: func(c *Config) { c.ClientSecret = "wrong-secret" },
			message: "client_id or client_secret is invalid (linkedin token endpoint: HTTP 401 invalid_client: Client authentication failed)",
			routes:  []string{tokenRoute},
		},
		{
			name:      "app lacks the Advertising API product",
			overrides: map[string][]fixture{"adAccounts": {{status: http.StatusForbidden, file: "errors/403_access_denied"}}},
			message:   "the LinkedIn app lacks the Advertising API product or the token lacks the r_ads scope (linkedin GET adAccounts: HTTP 403 ACCESS_DENIED (100): Not enough permissions to access: adAccounts.FINDER-search.20260901)",
			routes:    []string{tokenRoute, "adAccounts"},
		},
		{
			name: "ad account refused", edit: func(c *Config) { c.AccountIDs = []string{testAccountA, testAccountC} },
			overrides: map[string][]fixture{"adAccounts/1003": {{status: http.StatusForbidden, file: "errors/403_account"}}},
			message:   "ad account 1003 is not accessible to the user who authorized the app (linkedin GET adAccounts/1003: HTTP 403 ACCESS_DENIED (100): Not enough permissions to access: adAccounts.GET.20260901)",
			routes:    []string{tokenRoute, "adAccounts", "adAccounts/1001", "adAccounts/1003"},
		},
		{
			name: "ad account not found", edit: func(c *Config) { c.AccountIDs = []string{testAccountC} },
			message: "ad account 1003 is not accessible to the user who authorized the app (linkedin GET adAccounts/1003: HTTP 404 NOT_FOUND: Not Found)",
			routes:  []string{tokenRoute, "adAccounts", "adAccounts/1003"},
		},
		{
			name:      "access token refused after a refresh",
			overrides: map[string][]fixture{"adAccounts": {{status: http.StatusUnauthorized, file: "errors/401_invalid"}}},
			message:   "LinkedIn refused a newly issued access token (linkedin GET adAccounts: HTTP 401 INVALID_ACCESS_TOKEN (65600): Invalid access token)",
			routes:    []string{tokenRoute, "adAccounts", tokenRoute, "adAccounts"},
		},
		{
			name: "bad start_date", edit: func(c *Config) { c.StartDate = "2026-8-1" },
			message: `start_date "2026-8-1" is not a YYYY-MM-DD date`,
		},
		{
			name: "bad account id", edit: func(c *Config) { c.AccountIDs = []string{"urn:li:sponsoredAccount:1001"} },
			message: `account id "urn:li:sponsoredAccount:1001" is not a number`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv, h := fixtureServer(t, tt.overrides)
			src, _ := testSource(srv)
			status := runCheck(t, src, testConfig(t, tt.edit))
			var routes []string
			for _, r := range h.list() {
				routes = append(routes, r.route)
			}
			assert.Equal(t, tt.routes, routes)
			if tt.message == "" {
				assert.Equal(t, synclet.ConnectionStatusSucceeded, status.Status, status.Message)
				return
			}
			assert.Equal(t, synclet.ConnectionStatusFailed, status.Status)
			assert.Contains(t, status.Message, tt.message)
		})
	}
}

func TestCheckWithoutAccountIDsNeedsOneAccount(t *testing.T) {
	srv, _ := fixtureServer(t, map[string][]fixture{"adAccounts": {{file: "ad_accounts/empty"}}})
	src, _ := testSource(srv)
	status := runCheck(t, src, testConfig(t, func(c *Config) { c.AccountIDs = nil }))
	assert.Equal(t, synclet.ConnectionStatusFailed, status.Status)
	assert.Contains(t, status.Message, "the user who authorized the app has no ad account")
}
