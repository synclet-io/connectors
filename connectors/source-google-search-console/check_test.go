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
	getA := "GET " + sitePath(testSiteA)
	getB := "GET " + sitePath(testSiteB)
	tests := []struct {
		name      string
		edit      func(*Config)
		overrides map[string][]fixture
		message   string
		// requests is the exact requests of a check, when the test pins
		// them.
		requests []string
	}{
		{
			name:     "service account succeeds",
			requests: []string{"POST token", "GET sites/https%3A%2F%2Fwww.example.com%2F", "GET sites/sc-domain%3Aexample.org"},
		},
		{name: "oauth succeeds", edit: withOAuth},
		{name: "custom report succeeds", edit: withDiscoverPages},
		{
			name:     "revoked service account key",
			edit:     func(c *Config) { c.Credentials.ServiceAccountKey = serviceAccountKey(t, otherKey()) },
			message:  "service account key is invalid, disabled or deleted (token endpoint: HTTP 400 invalid_grant: Invalid JWT Signature.)",
			requests: []string{"POST token"},
		},
		{
			name:     "malformed service account key",
			edit:     func(c *Config) { c.Credentials.ServiceAccountKey = `{"type":"authorized_user"}` },
			message:  `service_account_key is not a valid service account key: google: read JWT from JSON credentials: 'type' field is "authorized_user" (expected "service_account")`,
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
			name:      "site forbidden for the service account",
			overrides: map[string][]fixture{getB: {{status: http.StatusForbidden, file: "errors/forbidden_site"}}},
			message:   "site sc-domain:example.org is not verified for service account " + testClientEmail + ": add it as a user in Search Console (sites/sc-domain%3Aexample.org: HTTP 403 forbidden: User does not have sufficient permission",
		},
		{
			name:      "site forbidden for the oauth user",
			edit:      withOAuth,
			overrides: map[string][]fixture{getA: {{status: http.StatusForbidden, file: "errors/forbidden_site"}}},
			message:   "site https://www.example.com/ is not verified for the Google user of the refresh token: add it as a user in Search Console",
			requests:  []string{"POST token", "GET sites/https%3A%2F%2Fwww.example.com%2F"},
		},
		{
			name:      "site not found",
			overrides: map[string][]fixture{getB: {{status: http.StatusNotFound, file: "errors/not_found"}}},
			message:   "site sc-domain:example.org is not verified for service account " + testClientEmail + ": add it as a user in Search Console (sites/sc-domain%3Aexample.org: HTTP 404 notFound",
		},
		{
			name:      "unverified user of the site",
			overrides: map[string][]fixture{getA: {{body: []byte(`{"siteUrl":"https://www.example.com/","permissionLevel":"siteUnverifiedUser"}`)}}},
			message:   "site https://www.example.com/ is not verified for service account " + testClientEmail + ": add it as a user in Search Console",
			requests:  []string{"POST token", "GET sites/https%3A%2F%2Fwww.example.com%2F"},
		},
		{
			name:    "site without fixtures",
			edit:    func(c *Config) { c.SiteURLs = []string{"https://shop.example.net/"} },
			message: "site https://shop.example.net/ is not verified for service account",
		},
		{
			name:      "search console api not enabled",
			overrides: map[string][]fixture{getA: {{status: http.StatusForbidden, file: "errors/service_disabled"}}},
			message:   "Google Search Console API is not enabled in Google Cloud project 123456789012: enable searchconsole.googleapis.com",
		},
		{
			name:      "credentials without the webmasters scope",
			overrides: map[string][]fixture{getA: {{status: http.StatusForbidden, file: "errors/scope_insufficient"}}},
			message:   "the credentials lack the https://www.googleapis.com/auth/webmasters.readonly scope",
		},
		{
			name:    "invalid config",
			edit:    func(c *Config) { c.SiteURLs = []string{"example.com"} },
			message: `site "example.com" is neither a URL-prefix property`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv, h := fixtureServer(t, tt.overrides)
			src, _ := testSource(srv)
			status := runCheck(t, src, testConfig(t, tt.edit))
			if tt.message == "" {
				assert.Equal(t, synclet.ConnectionStatusSucceeded, status.Status, status.Message)
			} else {
				assert.Equal(t, synclet.ConnectionStatusFailed, status.Status)
				assert.Contains(t, status.Message, tt.message)
			}
			if tt.requests != nil {
				assert.Equal(t, tt.requests, append([]string{}, h.keys()...))
			}
		})
	}
}

// TestCredentialErrorsAreConfigErrors checks the failure class of a rejected
// credential: a check status carries only the message, so the same
// credentials run a read, whose error trace carries the failure type.
func TestCredentialErrorsAreConfigErrors(t *testing.T) {
	tests := []struct {
		name    string
		edit    func(*Config)
		message string
	}{
		{
			name:    "revoked refresh token",
			edit:    func(c *Config) { withOAuth(c); c.Credentials.RefreshToken = "revoked-refresh-token" },
			message: "refresh_token is invalid or revoked",
		},
		{
			name:    "invalid oauth client",
			edit:    func(c *Config) { withOAuth(c); c.Credentials.ClientSecret = "wrong-client-secret" },
			message: "client_id or client_secret is invalid",
		},
		{
			name:    "revoked service account key",
			edit:    func(c *Config) { c.Credentials.ServiceAccountKey = serviceAccountKey(t, otherKey()) },
			message: "service account key is invalid, disabled or deleted",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv, _ := fixtureServer(t, nil)
			src, _ := testSource(srv)
			cfg := testConfig(t, tt.edit)
			status := runCheck(t, src, cfg)
			assert.Equal(t, synclet.ConnectionStatusFailed, status.Status)
			assert.Contains(t, status.Message, tt.message)

			out := runRead(t, src, cfg, configured(t, src, cfg, synclet.SyncModeFullRefresh, streamSites), nil)
			require.Error(t, out.Err)
			assert.Contains(t, out.Err.Error(), tt.message)
			require.Len(t, out.ErrorTraces(), 1)
			assert.Equal(t, synclet.FailureTypeConfigError, out.ErrorTraces()[0].FailureType)
		})
	}
}
