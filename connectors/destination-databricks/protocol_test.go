package main

import (
	"encoding/json"
	"os"
	"strings"
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
	ws := newWorkspace(t)
	sql := newFakeSQL(ws)
	sql.missing["volume"] = true
	cfg, err := json.Marshal(ws.config(t))
	require.NoError(t, err)
	out, err := testkit.RunDestination(t.Context(), ws.destination(sql), synclet.Request{Command: synclet.CommandCheck, Config: cfg})
	require.NoError(t, err)
	require.NoError(t, out.Err)
	status := out.ConnectionStatus()
	require.NotNil(t, status)
	assert.Equal(t, synclet.ConnectionStatusFailed, status.Status)
	assert.Contains(t, status.Message, `volume "main.raw.stg" does not exist`)
}

// TestProtocolWrite drives Write through the SDK's transport with one stream
// per supported sync mode and checks the echoed states and the statements
// each mode commits with.
func TestProtocolWrite(t *testing.T) {
	ws := newWorkspace(t)
	sql := newFakeSQL(ws)
	cfg, err := json.Marshal(ws.config(t))
	require.NoError(t, err)
	catalog, err := os.ReadFile("testdata/protocol_catalog.json")
	require.NoError(t, err)
	messages, err := os.Open("testdata/protocol_messages.jsonl")
	require.NoError(t, err)
	defer func() { _ = messages.Close() }()

	out, err := testkit.RunDestination(t.Context(), ws.destination(sql), synclet.Request{
		Command: synclet.CommandWrite, Config: cfg, Catalog: catalog, Input: messages,
	})
	require.NoError(t, err)
	require.NoError(t, out.Err)
	assert.Empty(t, out.ErrorTraces())

	var got []string
	for _, st := range out.States() {
		if st.Type == synclet.StateTypeGlobal {
			got = append(got, "GLOBAL "+string(st.Global.SharedState))
			continue
		}
		got = append(got, stateText(st))
	}
	assert.Equal(t, []string{
		`events {"id":2}`,
		`users {"updated_at":"2024-02-01T00:00:00Z"}`,
		`rates {}`,
		`GLOBAL {"lsn":7}`,
	}, got)

	assert.Len(t, sql.matching("INSERT INTO `main`.`raw`.`events`"), 1, "append")
	assert.Len(t, sql.matching("MERGE INTO `main`.`raw`.`users`"), 1, "append_dedup")
	assert.Len(t, sql.matching("COPY INTO `main`.`raw`.`_synclet_tmp_rates`"), 1, "overwrite stages into the shadow table")
	assert.Equal(t, []string{"CREATE OR REPLACE TABLE `main`.`raw`.`rates`" + tableProps + " AS SELECT * FROM `main`.`raw`.`_synclet_tmp_rates`"},
		sql.matching("CREATE OR REPLACE TABLE `main`.`raw`.`rates`"), "overwrite replaces")
	for _, s := range sql.statements() {
		assert.False(t, strings.Contains(s, "_synclet_stage_rates"), s)
	}
	assert.Zero(t, ws.fileCount())
}
