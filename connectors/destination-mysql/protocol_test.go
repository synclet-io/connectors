package main

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
	"github.com/synclet-io/synclet-go-sdk/testkit"
)

func TestProtocolSpec(t *testing.T) {
	out, err := testkit.RunDestination(t.Context(), NewDestination(), synclet.Request{Command: synclet.CommandSpec})
	require.NoError(t, err)
	require.NoError(t, out.Err)
	spec := out.Spec()
	require.NotNil(t, spec)
	assert.Equal(t, "object", spec.ConnectionSpecification.Type)
	assert.ElementsMatch(t, []synclet.DestinationSyncMode{
		synclet.DestinationSyncModeOverwrite, synclet.DestinationSyncModeAppend, synclet.DestinationSyncModeAppendDedup,
	}, spec.SupportedDestinationSyncModes)
}

func TestProtocolCheckFailure(t *testing.T) {
	cfg, err := json.Marshal(Config{Host: "127.0.0.1", Port: closedPort(t), Database: "d", Username: "u", SSLMode: SSLMode{Mode: sslDisabled}})
	require.NoError(t, err)
	out, err := testkit.RunDestination(t.Context(), NewDestination(), synclet.Request{Command: synclet.CommandCheck, Config: cfg})
	require.NoError(t, err)
	require.NoError(t, out.Err)
	status := out.ConnectionStatus()
	require.NotNil(t, status)
	assert.Equal(t, synclet.ConnectionStatusFailed, status.Status)
	assert.Contains(t, status.Message, "cannot reach 127.0.0.1:")
}

// TestProtocolWrite writes one stream per supported sync mode through the
// SDK's transport twice and checks the echoed states and the tables.
func TestProtocolWrite(t *testing.T) {
	cfg := sharedMySQL(t, "protocol")
	for i := range 2 {
		out := runTestdata(t, cfg, "protocol_catalog.json", "protocol_messages.jsonl")
		require.NoError(t, out.Err, "sync %d", i+1)
		assert.Empty(t, out.ErrorTraces())
		var got []string
		for _, st := range out.States() {
			require.Equal(t, synclet.StateTypeStream, st.Type)
			got = append(got, st.Stream.StreamDescriptor.Name+" "+string(st.Stream.StreamState))
		}
		assert.Equal(t, []string{
			`events {"id":2}`,
			`users {"updated_at":"2024-02-01T00:00:00Z"}`,
			`rates {}`,
		}, got, "sync %d", i+1)
		assert.Equal(t, 2*(i+1), countRows(t, cfg, "events"), "append adds rows")
		assert.Equal(t, [][]string{{"1", "Ann B", "2024-02-01 00:00:00.000000"}, {"2", "Bob", "2024-01-01 00:00:00.000000"}},
			queryStrings(t, cfg, "SELECT id, name, updated_at FROM users ORDER BY id"))
		assert.Equal(t, [][]string{{"EUR", "0.912345678901234567890000000000"}, {"UAH", "41.250000000000000000000000000000"}},
			queryStrings(t, cfg, "SELECT code, rate FROM rates ORDER BY code"))
		assert.Equal(t, []string{"events", "rates", "users"}, tables(t, cfg), "no shadow or old table is left")
	}
}
