package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
)

func emittedStates(t *testing.T, out *bytes.Buffer) []synclet.StateMessage {
	t.Helper()
	var states []synclet.StateMessage
	sc := bufio.NewScanner(out)
	for sc.Scan() {
		var msg synclet.Message
		require.NoError(t, json.Unmarshal(sc.Bytes(), &msg))
		if msg.Type == synclet.MessageTypeState {
			states = append(states, *msg.State)
		}
	}
	require.NoError(t, sc.Err())
	return states
}

func TestEmitStreamState(t *testing.T) {
	var out bytes.Buffer
	state := &IncrementalState{Phase: "incremental", CursorField: "updated_at", CursorValue: "2026-01-01T00:00:00Z", SnapshotDone: true}
	require.NoError(t, emitStreamState(synclet.NewEmitter(&out), "users", state))

	states := emittedStates(t, &out)
	require.Len(t, states, 1)
	assert.Equal(t, synclet.StateTypeStream, states[0].Type)
	assert.Equal(t, synclet.StreamDescriptor{Name: "users"}, states[0].Stream.StreamDescriptor)
	assert.JSONEq(t, `{"phase":"incremental","cursor_field":"updated_at","cursor_value":"2026-01-01T00:00:00Z","snapshot_done":true}`,
		string(states[0].Stream.StreamState))
}

func TestStreamStateRoundTrip(t *testing.T) {
	var out bytes.Buffer
	require.NoError(t, emitStreamState(synclet.NewEmitter(&out), "users", &FullRefreshState{LastPKVals: map[string]any{"id": float64(7)}}))
	states := emittedStates(t, &out)
	saved, err := json.Marshal(states)
	require.NoError(t, err)

	prev, err := synclet.ParseInputState(saved)
	require.NoError(t, err)
	raw := prev.Stream(streamDescriptor("users"))
	require.NotNil(t, raw)

	var got FullRefreshState
	parseStreamState(t.Context(), "users", raw, &got)
	assert.Equal(t, map[string]any{"id": float64(7)}, got.LastPKVals)
	assert.False(t, got.Done)
}

func TestParseStreamState_KeepsPartialStateOnError(t *testing.T) {
	var got IncrementalState
	parseStreamState(t.Context(), "users", json.RawMessage(`{"phase":"snapshot","snapshot_done":"no"}`), &got)
	assert.Equal(t, "snapshot", got.Phase)
}

func TestLoadCDCState_EmptyState(t *testing.T) {
	state, err := loadCDCState(&synclet.InputState{})
	require.NoError(t, err)
	assert.Nil(t, state)
}

func TestLoadCDCState_ReturnsNilForStreamState(t *testing.T) {
	prev, err := synclet.ParseInputState([]byte(`[{"type":"STREAM","stream":{"stream_descriptor":{"name":"users"},"stream_state":{"phase":"incremental"}}}]`))
	require.NoError(t, err)

	state, err := loadCDCState(prev)
	require.NoError(t, err)
	assert.Nil(t, state)
}

func TestLoadCDCState_ParsesLegacyState(t *testing.T) {
	for name, saved := range map[string]string{
		"array":       `[{"type":"LEGACY","data":{"binlog_file":"mysql-bin.000123","binlog_pos":4242,"snapshot_done":true,"snapshot_streams":{"users":{"done":true}}}}]`,
		"bare object": `{"binlog_file":"mysql-bin.000123","binlog_pos":4242,"snapshot_done":true,"snapshot_streams":{"users":{"done":true}}}`,
	} {
		t.Run(name, func(t *testing.T) {
			prev, err := synclet.ParseInputState([]byte(saved))
			require.NoError(t, err)

			state, err := loadCDCState(prev)
			require.NoError(t, err)
			require.NotNil(t, state)
			assert.Equal(t, "mysql-bin.000123", state.BinlogFile)
			assert.Equal(t, uint32(4242), state.BinlogPos)
			assert.True(t, state.SnapshotDone)
			require.Contains(t, state.SnapshotStreams, "users")
			assert.True(t, state.SnapshotStreams["users"].Done)
		})
	}
}

func TestEmitCDCState_IsGlobal(t *testing.T) {
	var out bytes.Buffer
	state := &CDCState{
		BinlogFile: "mysql-bin.000007",
		BinlogPos:  1234,
		GTIDSet:    "uuid:1-5",
		SnapshotStreams: map[string]*CDCStreamSnapshotState{
			"users":  {Done: true},
			"orders": {LastPKVals: map[string]any{"id": float64(42)}},
		},
	}
	require.NoError(t, emitCDCState(synclet.NewEmitter(&out), state))

	states := emittedStates(t, &out)
	require.Len(t, states, 1)
	require.Equal(t, synclet.StateTypeGlobal, states[0].Type)
	require.NotNil(t, states[0].Global)
	assert.JSONEq(t, `{"binlog_file":"mysql-bin.000007","binlog_pos":1234,"gtid_set":"uuid:1-5","snapshot_done":false}`,
		string(states[0].Global.SharedState))
	require.Len(t, states[0].Global.StreamStates, 2)
	assert.Equal(t, streamDescriptor("orders"), states[0].Global.StreamStates[0].StreamDescriptor)
	assert.JSONEq(t, `{"last_pk_vals":{"id":42},"done":false}`, string(states[0].Global.StreamStates[0].StreamState))
	assert.Equal(t, streamDescriptor("users"), states[0].Global.StreamStates[1].StreamDescriptor)
	assert.JSONEq(t, `{"done":true}`, string(states[0].Global.StreamStates[1].StreamState))
}

func TestCDCStateGlobalRoundTrip(t *testing.T) {
	var out bytes.Buffer
	want := &CDCState{
		BinlogFile:   "mysql-bin.000009",
		BinlogPos:    99,
		SnapshotDone: true,
		SnapshotStreams: map[string]*CDCStreamSnapshotState{
			"users": {Done: true},
		},
	}
	require.NoError(t, emitCDCState(synclet.NewEmitter(&out), want))
	saved, err := json.Marshal(emittedStates(t, &out))
	require.NoError(t, err)

	prev, err := synclet.ParseInputState(saved)
	require.NoError(t, err)
	require.Equal(t, synclet.StateTypeGlobal, prev.Type)

	got, err := loadCDCState(prev)
	require.NoError(t, err)
	assert.Equal(t, want, got)
}
