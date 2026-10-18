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
	accountB := "GET act_" + testAccountB
	tests := []struct {
		name      string
		token     string
		overrides map[string][]fixture
		message   string
	}{
		{name: "succeeds"},
		{name: "invalid or expired token", token: "expired-token", message: "invalid or expired access token (GET me: HTTP 400 code 190/463: Error validating access token"},
		{
			name:      "missing ads_read",
			overrides: map[string][]fixture{"GET me/permissions": {{file: "me/permissions_without_ads_read"}}},
			message:   "missing ads_read permission",
		},
		{
			name:      "unknown ad account",
			overrides: map[string][]fixture{accountB: {{status: http.StatusBadRequest, file: "errors/no_account"}}},
			message:   "ad account act_2222222222 not accessible (GET act_2222222222: HTTP 400 code 100/33",
		},
		{
			name:      "ad account without permission",
			overrides: map[string][]fixture{accountB: {{status: http.StatusForbidden, file: "errors/no_permission"}}},
			message:   "ad account act_2222222222 not accessible (GET act_2222222222: HTTP 403 code 200",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv, h := fixtureServer(t, tt.overrides)
			src, _ := testSource(srv)
			status := runCheck(t, src, testConfig(t, func(c *Config) {
				if tt.token != "" {
					c.AccessToken = tt.token
				}
			}))
			if tt.message == "" {
				assert.Equal(t, synclet.ConnectionStatusSucceeded, status.Status, status.Message)
				assert.Equal(t, []string{"GET me", "GET me/permissions", "GET act_" + testAccountA, "GET act_" + testAccountB}, h.keys())
				return
			}
			assert.Equal(t, synclet.ConnectionStatusFailed, status.Status)
			assert.Contains(t, status.Message, tt.message)
		})
	}
}
