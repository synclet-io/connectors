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
	metaB := "GET properties/" + testPropB + "/metadata"
	tests := []struct {
		name      string
		edit      func(*Config)
		overrides map[string][]fixture
		message   string
		// requests is the exact requests of a failed check, when the test
		// pins them.
		requests []string
	}{
		{name: "service account succeeds"},
		{name: "oauth succeeds", edit: withOAuth},
		{name: "custom report succeeds", edit: withMyReport},
		{
			name:     "revoked service account key",
			edit:     func(c *Config) { c.Credentials.ServiceAccountKey = serviceAccountKey(t, otherKey()) },
			message:  "service account key is invalid, disabled or deleted (token endpoint: HTTP 400 invalid_grant: Invalid JWT Signature.)",
			requests: []string{"POST token"},
		},
		{
			name:     "malformed service account key",
			edit:     func(c *Config) { c.Credentials.ServiceAccountKey = `{"type":"authorized_user"}` },
			message:  `service_account_key is not a valid service account key: type is "authorized_user", a service account key has type service_account`,
			requests: []string{},
		},
		{
			name:     "revoked refresh token",
			edit:     func(c *Config) { withOAuth(c); c.Credentials.RefreshToken = "revoked-refresh-token" },
			message:  "refresh_token is invalid or revoked (token endpoint: HTTP 400 invalid_grant: Token has been expired or revoked.)",
			requests: []string{"POST token"},
		},
		{
			name:     "invalid oauth client",
			edit:     func(c *Config) { withOAuth(c); c.Credentials.ClientSecret = "wrong-client-secret" },
			message:  "client_id or client_secret is invalid (token endpoint: HTTP 401 invalid_client: Unauthorized)",
			requests: []string{"POST token"},
		},
		{
			name:      "data api not enabled",
			overrides: map[string][]fixture{"GET properties/" + testPropA + "/metadata": {{status: http.StatusForbidden, file: "errors/service_disabled"}}},
			message:   "Google Analytics Data API is not enabled in Google Cloud project 123456789012: enable analyticsdata.googleapis.com (properties/123456789/metadata: HTTP 403 SERVICE_DISABLED",
			requests:  []string{"POST token", "GET properties/123456789/metadata"},
		},
		{
			name:      "no access to property",
			overrides: map[string][]fixture{metaB: {{status: http.StatusForbidden, file: "errors/permission_denied"}}},
			message:   "no access to property 987654321: grant the service account or user Viewer on it (properties/987654321/metadata: HTTP 403 PERMISSION_DENIED",
		},
		{
			name:      "property not found",
			overrides: map[string][]fixture{metaB: {{status: http.StatusNotFound, file: "errors/not_found"}}},
			message:   "no access to property 987654321: grant the service account or user Viewer on it (properties/987654321/metadata: HTTP 404 NOT_FOUND",
		},
		{
			name:      "credentials without the analytics scope",
			overrides: map[string][]fixture{metaB: {{status: http.StatusForbidden, file: "errors/scope_insufficient"}}},
			message:   "the credentials lack the https://www.googleapis.com/auth/analytics.readonly scope: authorize the refresh token with it (properties/987654321/metadata: HTTP 403 ACCESS_TOKEN_SCOPE_INSUFFICIENT",
		},
		{
			name:      "other permission denied",
			overrides: map[string][]fixture{metaB: {{status: http.StatusForbidden, file: "errors/permission_denied_other"}}},
			message:   "permission denied for property 987654321 (properties/987654321/metadata: HTTP 403 IAM_PERMISSION_DENIED: The caller does not have permission.)",
		},
		{
			name:    "property without fixtures",
			edit:    func(c *Config) { c.PropertyIDs = []string{"555555555"} },
			message: "no access to property 555555555",
		},
		{
			name: "unknown custom dimension",
			edit: func(c *Config) {
				c.CustomReports = []CustomReport{{Name: "bad", Dimensions: []string{"customEvent:unknown_param"}, Metrics: []string{"sessions"}}}
			},
			message: "custom report bad: property 123456789 has no dimension customEvent:unknown_param",
		},
		{
			name: "unknown custom metric",
			edit: func(c *Config) {
				c.CustomReports = []CustomReport{{Name: "bad", Metrics: []string{"customEvent:unknown_metric"}}}
			},
			message: "custom report bad: property 123456789 has no metric customEvent:unknown_metric",
		},
		{
			name:    "invalid config",
			edit:    func(c *Config) { c.PropertyIDs = []string{"UA-1"} },
			message: `property id "UA-1" is not a number, with or without the properties/ prefix`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv, h := fixtureServer(t, tt.overrides)
			src, _ := testSource(srv)
			status := runCheck(t, src, testConfig(t, tt.edit))
			if tt.message == "" {
				assert.Equal(t, synclet.ConnectionStatusSucceeded, status.Status, status.Message)
				return
			}
			assert.Equal(t, synclet.ConnectionStatusFailed, status.Status)
			assert.Contains(t, status.Message, tt.message)
			if tt.requests != nil {
				assert.Equal(t, tt.requests, append([]string{}, h.keys()...))
			}
		})
	}
}
