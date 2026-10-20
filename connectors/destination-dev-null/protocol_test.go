package main

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
	"github.com/synclet-io/synclet-go-sdk/testkit"
)

func TestProtocolSpec(t *testing.T) {
	out, err := testkit.RunDestination(t.Context(), Destination{}, synclet.Request{Command: synclet.CommandSpec})
	require.NoError(t, err)
	require.NoError(t, out.Err)
	spec := out.Spec()
	require.NotNil(t, spec)
	assert.Equal(t, "object", spec.ConnectionSpecification.Type)
	assert.True(t, spec.SupportsIncremental)
	assert.ElementsMatch(t, []synclet.DestinationSyncMode{
		synclet.DestinationSyncModeOverwrite, synclet.DestinationSyncModeAppend, synclet.DestinationSyncModeAppendDedup,
	}, spec.SupportedDestinationSyncModes)

	got, err := json.Marshal(spec)
	require.NoError(t, err)
	want, err := os.ReadFile("spec.json")
	require.NoError(t, err)
	assert.JSONEq(t, string(want), string(got), "spec.json is out of date; run: task spec CONNECTOR=destination-dev-null")
}

func TestProtocolCheck(t *testing.T) {
	for _, c := range []struct {
		config string
		status synclet.ConnectionStatusType
	}{
		{`{"test_destination":{"test_destination_type":"SILENT"}}`, synclet.ConnectionStatusSucceeded},
		{`{"test_destination":{"test_destination_type":"LOGGING","logging_config":{"logging_type":"FirstN","max_entry_count":100}}}`, synclet.ConnectionStatusSucceeded},
		{`{"test_destination":{"test_destination_type":"THROTTLED","millis_per_record":10}}`, synclet.ConnectionStatusSucceeded},
		{`{"test_destination":{"test_destination_type":"FAILING","num_messages":10}}`, synclet.ConnectionStatusSucceeded},
		{`{"test_destination":{"test_destination_type":"FAILING"}}`, synclet.ConnectionStatusFailed},
		{`{"test_destination":"SILENT"}`, synclet.ConnectionStatusFailed},
	} {
		out, err := testkit.RunDestination(t.Context(), Destination{}, synclet.Request{
			Command: synclet.CommandCheck,
			Config:  json.RawMessage(c.config),
		})
		require.NoError(t, err)
		require.NoError(t, out.Err)
		status := out.ConnectionStatus()
		require.NotNil(t, status)
		assert.Equal(t, c.status, status.Status, c.config)
	}
}
