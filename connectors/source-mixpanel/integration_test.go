package main

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
)

// liveConfigEnv names a config file of a real Mixpanel project. Without it
// the integration test is skipped; CI never sets it.
const liveConfigEnv = "MIXPANEL_TEST_CONFIG"

// TestLive runs check, discover and a full refresh of cohorts and
// annotations against the real Mixpanel API, five Query API requests of the
// hourly 60. It prints no record.
func TestLive(t *testing.T) {
	path := os.Getenv(liveConfigEnv)
	if path == "" {
		t.Skipf("%s is not set", liveConfigEnv)
	}
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	cfg := json.RawMessage(raw)
	src := Source{}

	st := runCheck(t, src, cfg)
	require.Equal(t, synclet.ConnectionStatusSucceeded, st.Status, st.Message)

	out := read(t, src, cfg, catalogOf(t, src, cfg, synclet.SyncModeFullRefresh, streamCohorts, streamAnnotations), nil)
	require.NoError(t, out.Err)
	t.Logf("cohorts: %d, annotations: %d", len(records(t, out, streamCohorts)), len(records(t, out, streamAnnotations)))
}
