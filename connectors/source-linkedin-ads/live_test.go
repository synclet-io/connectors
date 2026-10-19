package main

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
)

// liveConfigEnv names a config file of a real LinkedIn app and ad account.
// Without it the live test is skipped; CI never sets it.
const liveConfigEnv = "LINKEDIN_ADS_LIVE_CONFIG"

// TestLive runs check, discover and a full refresh of ad_accounts against
// the real LinkedIn API. It prints no record.
func TestLive(t *testing.T) {
	path := os.Getenv(liveConfigEnv)
	if path == "" {
		t.Skipf("%s is not set", liveConfigEnv)
	}
	cfg, err := os.ReadFile(path)
	require.NoError(t, err)
	src := Source{}

	status := runCheck(t, src, cfg)
	require.Equal(t, synclet.ConnectionStatusSucceeded, status.Status, status.Message)

	out := runRead(t, src, cfg, configured(t, src, synclet.SyncModeFullRefresh, streamAdAccounts), nil)
	require.NoError(t, out.Err)
	assert.NotEmpty(t, records(t, out, streamAdAccounts))
}
