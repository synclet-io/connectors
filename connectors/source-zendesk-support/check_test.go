package main

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
	"github.com/synclet-io/synclet-go-sdk/testkit"
)

func runCheck(t *testing.T, src Source, cfg Config) *synclet.ConnectionStatus {
	t.Helper()
	raw, err := json.Marshal(cfg)
	require.NoError(t, err)
	out, err := testkit.RunSource(t.Context(), src, synclet.Request{Command: synclet.CommandCheck, Config: raw})
	require.NoError(t, err)
	require.NoError(t, out.Err)
	require.NotNil(t, out.ConnectionStatus())
	return out.ConnectionStatus()
}

// unknownHost returns a client whose every request fails the way a lookup
// of a subdomain without a Zendesk account does.
func unknownHost() *http.Client {
	return &http.Client{Transport: &http.Transport{
		DialContext: func(_ context.Context, _, _ string) (net.Conn, error) {
			return nil, &net.DNSError{Err: "no such host", Name: "nosuch.zendesk.com", IsNotFound: true}
		},
	}}
}

func TestCheck(t *testing.T) {
	wrongToken := testCfg
	wrongToken.APIToken = "wrong-api-token"
	tests := []struct {
		name     string
		cfg      Config
		override map[string]fixture
		client   *http.Client
		message  string
		requests int
	}{
		{
			name: "wrong subdomain, 404", cfg: testCfg,
			override: map[string]fixture{"users/me.json": {status: http.StatusNotFound, file: "errors/not_found.json"}},
			message:  `wrong subdomain "acme": GET /api/v2/users/me.json (HTTP 404): InvalidEndpoint: Not found`, requests: 1,
		},
		{
			name: "wrong subdomain, unknown host", cfg: testCfg, client: unknownHost(),
			message: `wrong subdomain "acme": host nosuch.zendesk.com not found`,
		},
		{
			name: "invalid token", cfg: wrongToken,
			message: "invalid email or API token (HTTP 401): Couldn't authenticate you", requests: 1,
		},
		{
			name: "agent role", cfg: testCfg,
			override: map[string]fixture{"users/me.json": {status: http.StatusOK, file: "users/me-agent.json"}},
			message:  `missing admin role for user export (role "agent")`, requests: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv, h := fixtureServer(t, tt.override)
			src := testSource(srv)
			if tt.client != nil {
				src.client = tt.client
			}
			status := runCheck(t, src, tt.cfg)
			assert.Equal(t, synclet.ConnectionStatusFailed, status.Status)
			assert.Equal(t, tt.message, status.Message)
			assert.Len(t, h.list(), tt.requests, "a failed check is not retried")
		})
	}
}

func TestCheckSucceeds(t *testing.T) {
	srv, h := fixtureServer(t, nil)
	status := runCheck(t, testSource(srv), testCfg)
	assert.Equal(t, synclet.ConnectionStatusSucceeded, status.Status)
	assert.Equal(t, []string{"users/me.json"}, h.list())
}

func TestCheckConfig(t *testing.T) {
	cfg := testCfg
	cfg.Subdomain = "acme.zendesk.com"
	status := runCheck(t, Source{}, cfg)
	assert.Equal(t, synclet.ConnectionStatusFailed, status.Status)
	assert.Contains(t, status.Message, `must be the account name only, such as "acme"`)
}
