package main

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
)

func runCheck(t *testing.T, src Source, cfg Config) *synclet.ConnectionStatus {
	t.Helper()
	out := run(t, src, synclet.Request{Command: synclet.CommandCheck, Config: rawConfig(t, cfg)})
	require.NoError(t, out.Err)
	require.NotNil(t, out.ConnectionStatus())
	return out.ConnectionStatus()
}

func TestCheck(t *testing.T) {
	const prodGrant = "refresh_token is invalid, expired or revoked, or belongs to a sandbox org while is_sandbox is false"
	const sandboxGrant = "refresh_token is invalid, expired or revoked, or belongs to a production org while is_sandbox is true"
	tests := []struct {
		name      string
		edit      func(*Config)
		overrides map[string][]fixture
		want      string
		tokenHost string
	}{
		{name: "valid production org", tokenHost: "login/token"},
		{
			name: "valid sandbox org", tokenHost: "test/token",
			edit: func(c *Config) { c.IsSandbox, c.RefreshToken = true, testSandboxRefreshToken },
		},
		{
			name: "wrong client id", edit: func(c *Config) { c.ClientID = "wrong-client-id" },
			want: "client_id or client_secret is invalid",
		},
		{
			name: "wrong client secret", edit: func(c *Config) { c.ClientSecret = "wrong-client-secret" },
			want: "client_id or client_secret is invalid",
		},
		{
			name: "revoked refresh token", edit: func(c *Config) { c.RefreshToken = "revoked-refresh-token" },
			want: prodGrant,
		},
		{
			name: "sandbox token without sandbox flag", edit: func(c *Config) { c.RefreshToken = testSandboxRefreshToken },
			want: prodGrant,
		},
		{
			name: "production token with sandbox flag", edit: func(c *Config) { c.IsSandbox = true },
			want: sandboxGrant,
		},
		{
			name:      "token of another org",
			overrides: map[string][]fixture{"POST token": {{file: "token_other_instance"}}},
			want:      "does not match the org of the refresh token (https://other-org.example.com)",
		},
		{
			name:      "API disabled for the org",
			overrides: map[string][]fixture{"GET sobjects": {{status: http.StatusForbidden, file: "errors/API_DISABLED_FOR_ORG"}}},
			want:      "API access is not enabled for this user or org",
		},
		{
			name:      "API disabled for the user",
			overrides: map[string][]fixture{"GET sobjects": {{status: http.StatusForbidden, file: "errors/API_CURRENTLY_DISABLED"}}},
			want:      "API access is not enabled for this user or org",
		},
		{
			name: "instance URL without https", edit: func(c *Config) { c.InstanceURL = "http://acme.example.com" },
			want: "instance_url \"http://acme.example.com\" must be the https URL of the org",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			o := fixtureServer(t, tt.overrides)
			src, _ := testSource(o)
			cfg := testCfg(o)
			if tt.edit != nil {
				tt.edit(&cfg)
			}
			st := runCheck(t, src, cfg)
			if tt.want == "" {
				assert.Equal(t, synclet.ConnectionStatusSucceeded, st.Status, st.Message)
				assert.Equal(t, []string{"POST " + tt.tokenHost, "GET sobjects"}, o.keys())
				return
			}
			assert.Equal(t, synclet.ConnectionStatusFailed, st.Status)
			assert.Contains(t, st.Message, tt.want)
		})
	}
}

func TestCheckInstanceMismatchNamesBoth(t *testing.T) {
	o := fixtureServer(t, map[string][]fixture{"POST token": {{file: "token_other_instance"}}})
	src, _ := testSource(o)
	st := runCheck(t, src, testCfg(o))
	assert.Equal(t, synclet.ConnectionStatusFailed, st.Status)
	assert.Contains(t, st.Message, "instance_url "+o.srv.URL+" does not match the org of the refresh token (https://other-org.example.com)")
	assert.NotContains(t, o.keys(), "GET sobjects")
}

func TestCheckRotatedTokenWarns(t *testing.T) {
	o := fixtureServer(t, map[string][]fixture{"POST token": {{file: "token_rotated"}}})
	src, _ := testSource(o)
	out := run(t, src, synclet.Request{Command: synclet.CommandCheck, Config: rawConfig(t, testCfg(o))})
	require.NoError(t, out.Err)
	assert.Equal(t, synclet.ConnectionStatusSucceeded, out.ConnectionStatus().Status)
	var warned bool
	for _, l := range out.Logs() {
		if l.Level == synclet.LogLevelWarn && assert.NotContains(t, l.Message, testRotatedRefreshToken) {
			warned = warned || containsAll(l.Message, "refresh token", "rotat")
		}
	}
	assert.True(t, warned, "check logs a warning about the rotated refresh token")
	for _, m := range out.Messages {
		assert.NotEqual(t, synclet.MessageTypeControl, m.Type, "check emits no CONTROL message")
	}
}
