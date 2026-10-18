package main

import (
	"encoding/json"
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
	info := "advertiser/info/"
	errCode := func(c string) fixture { return fixture{file: "errors/" + c} }
	tests := []struct {
		name      string
		edit      func(*Config)
		overrides map[string][]fixture
		message   string
		requests  int
	}{
		{name: "succeeds", requests: 1},
		{
			name: "revoked token (40105)", edit: func(c *Config) { c.AccessToken = "expired-token" },
			message: "access token is invalid or expired (code 40105)", requests: 1,
		},
		{
			name: "expired token (40102)", overrides: map[string][]fixture{info: {errCode("40102")}},
			message: "access token is invalid or expired (code 40102)", requests: 1,
		},
		{
			name: "invalid token (40104)", overrides: map[string][]fixture{info: {errCode("40104")}},
			message: "access token is invalid or expired (code 40104)", requests: 1,
		},
		{
			name: "advertiser refused with 40001", edit: func(c *Config) { c.AdvertiserIDs = []string{testAdvertiserC} },
			overrides: map[string][]fixture{info: {errCode("40001")}},
			message:   "advertiser 1000000003 is not authorised for this access token (tiktok advertiser/info/: code 40001: No permission to operate advertiser 1000000003.)",
			requests:  1,
		},
		{
			name:      "40001 for a batch checks each advertiser",
			overrides: map[string][]fixture{info: {errCode("40001"), {}, errCode("40001")}},
			message:   "advertiser 1000000002 is not authorised for this access token",
			requests:  3,
		},
		{
			name: "advertiser missing from the result", edit: func(c *Config) { c.AdvertiserIDs = append(c.AdvertiserIDs, testAdvertiserC) },
			message: "advertiser 1000000003 is not authorised for this access token", requests: 1,
		},
		{
			name: "bad start_date", edit: func(c *Config) { c.StartDate = "2026-8-1" },
			message: `start_date "2026-8-1" is not a YYYY-MM-DD date`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv, h := fixtureServer(t, tt.overrides)
			src, _ := testSource(srv)
			status := runCheck(t, src, testConfig(t, tt.edit))
			assert.Len(t, h.of(info), tt.requests)
			if tt.message == "" {
				assert.Equal(t, synclet.ConnectionStatusSucceeded, status.Status, status.Message)
				return
			}
			assert.Equal(t, synclet.ConnectionStatusFailed, status.Status)
			assert.Contains(t, status.Message, tt.message)
		})
	}
}
