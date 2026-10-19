package main

import (
	"encoding/json"
	"net/http"
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

func TestProtocolCheck(t *testing.T) {
	cfg, err := json.Marshal(testConfig(t))
	require.NoError(t, err)
	out, err := testkit.RunDestination(t.Context(), newFake(t).on(sessionOK, schemaExists).destination(),
		synclet.Request{Command: synclet.CommandCheck, Config: cfg})
	require.NoError(t, err)
	require.NoError(t, out.Err)
	require.NotNil(t, out.ConnectionStatus())
	assert.Equal(t, synclet.ConnectionStatusSucceeded, out.ConnectionStatus().Status)

	f := newFake(t)
	f.loginStatus = http.StatusForbidden
	out, err = testkit.RunDestination(t.Context(), f.destination(), synclet.Request{Command: synclet.CommandCheck, Config: cfg})
	require.NoError(t, err)
	require.NoError(t, out.Err)
	assert.Equal(t, synclet.ConnectionStatusFailed, out.ConnectionStatus().Status)
	assert.Contains(t, out.ConnectionStatus().Message, `account "synclet-test" not found`)
}

// TestProtocolWrite drives Write through the SDK's transport with one stream
// per supported sync mode, once in a single batch and once in a batch per
// record, and checks the echoed states and what each table received.
func TestProtocolWrite(t *testing.T) {
	for _, maxRows := range []int{100_000, 1} {
		f := protocolFake(t)
		d := f.destination()
		d.maxRows = maxRows
		out := runWrite(t, d, "protocol_catalog.json", readTestdata(t, "protocol_messages.jsonl"))
		require.NoError(t, out.Err, "maxRows %d", maxRows)
		assert.Empty(t, out.ErrorTraces())
		assert.Equal(t, []string{
			`events {"id":2}`,
			`users {"updated_at":"2024-02-01T00:00:00Z"}`,
			`rates {}`,
		}, stateNames(out), "maxRows %d", maxRows)
		assert.Len(t, f.rowsOf(sch+`."_SYNCLET_STAGE_0"`), 2)
		assert.Len(t, f.rowsOf(sch+`."_SYNCLET_STAGE_1"`), 3)
		assert.Len(t, f.rowsOf(sch+`."_SYNCLET_STAGE_2"`), 3)
		sqls := f.executed()
		commits := count(sqls, "COMMIT")
		if maxRows == 1 {
			assert.Equal(t, 8, commits, "a batch per record")
		} else {
			assert.Equal(t, 1, commits)
		}
		assert.Equal(t, `DROP TABLE IF EXISTS `+sch+`."_SYNCLET_TMP_RATES"`, sqls[len(sqls)-2])
		assert.Equal(t, `REMOVE @~/synclet/<run>/`, normalized(sqls)[len(sqls)-1])
	}
}
