package main

import (
	"encoding/json"
	"fmt"
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
	uri := "http://127.0.0.1:1"
	cfg, err := json.Marshal(Config{
		Catalog:   CatalogConfig{CatalogType: catalogREST, URI: uri},
		Namespace: "lake",
		S3: S3Config{
			WarehouseLocation: "s3://lake/wh",
			Endpoint:          fmt.Sprintf("http://127.0.0.1:%d", closedPort(t)),
			ForcePathStyle:    true,
			Credentials:       Credentials{AuthType: authAccessKey, AccessKeyID: "k", SecretAccessKey: "s"},
		},
	})
	require.NoError(t, err)
	out, err := testkit.RunDestination(t.Context(), NewDestination(), synclet.Request{Command: synclet.CommandCheck, Config: cfg})
	require.NoError(t, err)
	require.NoError(t, out.Err)
	status := out.ConnectionStatus()
	require.NotNil(t, status)
	assert.Equal(t, synclet.ConnectionStatusFailed, status.Status)
	assert.Contains(t, status.Message, "s3://lake/wh/_synclet_check_")
}

// TestProtocolWrite writes one stream per supported sync mode through the
// SDK's transport twice and checks the echoed states and the tables.
func TestProtocolWrite(t *testing.T) {
	e := startLake(t)
	cfg := e.config(t)
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
		assert.Len(t, tableRows(t, cfg, "events", "id"), 2*(i+1), "append adds rows")
		assert.Equal(t, [][]string{{"1", "Ann B"}, {"2", "Bob"}}, tableRows(t, cfg, "users", "id", "name"), "append_dedup keeps one row per key")
		assert.Equal(t, [][]string{{"EUR", "0.912345678"}, {"UAH", "41.250000000"}}, tableRows(t, cfg, "rates", "code", "rate"), "overwrite replaces")
	}
	assert.Equal(t, []string{"append", "overwrite"}, snapshots(t, cfg, "rates"))
	assert.Equal(t, []string{"append", "append"}, snapshots(t, cfg, "events"))
}
