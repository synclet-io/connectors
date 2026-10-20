package main

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
	"github.com/synclet-io/synclet-go-sdk/testkit"
)

// testConfigEnv names a config file of a real Amplitude project. Without it
// the integration test is skipped; CI never sets it.
const testConfigEnv = "AMPLITUDE_TEST_CONFIG"

// TestIntegration runs check and a read of every stream against the real
// Amplitude API. It prints no record.
func TestIntegration(t *testing.T) {
	path := os.Getenv(testConfigEnv)
	if path == "" {
		t.Skipf("%s is not set", testConfigEnv)
	}
	cfg, err := os.ReadFile(path)
	require.NoError(t, err)
	src := Source{}

	out, err := testkit.RunSource(t.Context(), src, synclet.Request{Command: synclet.CommandCheck, Config: json.RawMessage(cfg)})
	require.NoError(t, err)
	status := out.ConnectionStatus()
	require.NotNil(t, status)
	require.Equal(t, synclet.ConnectionStatusSucceeded, status.Status, status.Message)

	out = runRead(t, src, string(cfg), configured(t, src, synclet.SyncModeIncremental, streamNames...), "")
	require.NoError(t, out.Err)
	for _, name := range streamNames {
		t.Logf("%s: %d records", name, len(out.Records(synclet.StreamDescriptor{Name: name})))
	}
}
