package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestToMap_RoundTripsStruct(t *testing.T) {
	state := IncrementalState{
		Phase:        "incremental",
		CursorField:  "updated_at",
		CursorValue:  "2026-01-01T00:00:00Z",
		SnapshotDone: true,
	}

	out := toMap(state)
	require.NotNil(t, out)

	assert.Equal(t, "incremental", out["phase"])
	assert.Equal(t, "updated_at", out["cursor_field"])
	assert.Equal(t, "2026-01-01T00:00:00Z", out["cursor_value"])
	assert.Equal(t, true, out["snapshot_done"])
}

func TestToMap_HandlesNil(t *testing.T) {
	// toMap returns nil on marshal/unmarshal failure. nil input marshals to
	// JSON null which fails to unmarshal into a map — verifies the error path.
	got := toMap(nil)
	assert.Nil(t, got)
}

func TestParseStreamState_RoundTrips(t *testing.T) {
	raw := map[string]interface{}{
		"phase":         "snapshot",
		"cursor_field":  "id",
		"snapshot_done": false,
	}

	var target IncrementalState

	require.NoError(t, parseStreamState(raw, &target))
	assert.Equal(t, "snapshot", target.Phase)
	assert.Equal(t, "id", target.CursorField)
	assert.False(t, target.SnapshotDone)
}

func TestParseStreamState_ErrorsOnTargetMismatch(t *testing.T) {
	// Passing a non-pointer must surface as an error.
	raw := map[string]interface{}{"phase": "snapshot"}

	var target IncrementalState

	require.Error(t, parseStreamState(raw, target)) // intentionally not &target
}

func TestLoadStreamState_EmptyPathReturnsNilNil(t *testing.T) {
	state, err := loadStreamState("", "anything")
	require.NoError(t, err)
	assert.Nil(t, state)
}

func TestLoadStreamState_MissingFileFails(t *testing.T) {
	_, err := loadStreamState(filepath.Join(t.TempDir(), "does-not-exist.json"), "stream")
	require.Error(t, err)
}

func TestLoadCDCState_EmptyPathReturnsNilNil(t *testing.T) {
	state, err := loadCDCState("")
	require.NoError(t, err)
	assert.Nil(t, state)
}

func TestLoadCDCState_ParsesLegacyEnvelope(t *testing.T) {
	tmp := filepath.Join(t.TempDir(), "state.json")
	contents := `[
		{"type":"STREAM","data":{"phase":"snapshot"}},
		{"type":"LEGACY","data":{"binlog_file":"mysql-bin.000123","binlog_pos":4242,"snapshot_done":true}}
	]`
	require.NoError(t, os.WriteFile(tmp, []byte(contents), 0o600))

	state, err := loadCDCState(tmp)
	require.NoError(t, err)
	require.NotNil(t, state)
	assert.Equal(t, "mysql-bin.000123", state.BinlogFile)
	assert.Equal(t, uint32(4242), state.BinlogPos)
	assert.True(t, state.SnapshotDone)
}

func TestLoadCDCState_ReturnsNilWhenNoLegacyEntry(t *testing.T) {
	tmp := filepath.Join(t.TempDir(), "state.json")
	contents := `[{"type":"STREAM","data":{"phase":"incremental"}}]`
	require.NoError(t, os.WriteFile(tmp, []byte(contents), 0o600))

	state, err := loadCDCState(tmp)
	require.NoError(t, err)
	assert.Nil(t, state)
}
