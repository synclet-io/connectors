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
	managerSearch := "POST customers/" + testManager + "/googleAds:search"
	tests := []struct {
		name      string
		edit      func(*Config)
		overrides map[string][]fixture
		message   string
	}{
		{name: "succeeds"},
		{
			name:    "invalid refresh token",
			edit:    func(c *Config) { c.RefreshToken = "revoked-refresh-token" },
			message: "refresh_token is invalid or revoked (token endpoint: HTTP 400 invalid_grant: Token has been expired or revoked.)",
		},
		{
			name:    "invalid client",
			edit:    func(c *Config) { c.ClientSecret = "wrong-client-secret" },
			message: "client_id or client_secret is invalid (token endpoint: HTTP 401 invalid_client",
		},
		{
			name:    "developer token not approved",
			edit:    func(c *Config) { c.DeveloperToken = "unapproved-developer-token" },
			message: "developer token is not approved for this account (test-only token?) (customers/1111111111/googleAds:search: HTTP 403 DEVELOPER_TOKEN_NOT_APPROVED",
		},
		{
			name:      "developer token prohibited",
			overrides: map[string][]fixture{managerSearch: {{status: http.StatusForbidden, file: "errors/developer_token_prohibited"}}},
			message:   "developer token is not approved for this account (test-only token?) (customers/1111111111/googleAds:search: HTTP 403 DEVELOPER_TOKEN_PROHIBITED",
		},
		{
			name:      "customer not accessible",
			overrides: map[string][]fixture{managerSearch: {{status: http.StatusForbidden, file: "errors/user_permission_denied"}}},
			message:   "customer 1111111111 is not accessible; if it is managed through a manager account, set login_customer_id (customers/1111111111/googleAds:search: HTTP 403 USER_PERMISSION_DENIED",
		},
		{
			name:    "invalid config",
			edit:    func(c *Config) { c.CustomerIDs = []string{"not-a-number"} },
			message: `customer id "not-a-number" is not a number`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv, h := fixtureServer(t, tt.overrides)
			src, _ := testSource(srv)
			status := runCheck(t, src, testConfig(t, tt.edit))
			if tt.message == "" {
				assert.Equal(t, synclet.ConnectionStatusSucceeded, status.Status, status.Message)
				reqs := h.searches("customer")
				require.Len(t, reqs, 1)
				assert.Equal(t, "SELECT customer.id FROM customer LIMIT 1", reqs[0].query)
				assert.Equal(t, "customers/"+testManager+"/googleAds:search", reqs[0].path)
				return
			}
			assert.Equal(t, synclet.ConnectionStatusFailed, status.Status)
			assert.Contains(t, status.Message, tt.message)
		})
	}

	t.Run("login_customer_id is sent", func(t *testing.T) {
		srv, h := fixtureServer(t, nil)
		src, _ := testSource(srv)
		status := runCheck(t, src, testConfig(t, func(c *Config) {
			c.CustomerIDs = []string{testClientA}
			c.LoginCustomerID = "111-111-1111"
		}))
		assert.Equal(t, synclet.ConnectionStatusSucceeded, status.Status, status.Message)
		require.Len(t, h.searches("customer"), 1)
		assert.Equal(t, testManager, h.searches("customer")[0].login)
	})
}
