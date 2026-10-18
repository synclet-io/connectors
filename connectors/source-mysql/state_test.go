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

func TestLoadCDCState_EmptyState(t *testing.T) {
	state, err := loadCDCState(&synclet.InputState{}, nil)
	require.NoError(t, err)
	assert.Nil(t, state)
}

func TestLoadCDCState_ReturnsNilForStreamState(t *testing.T) {
	prev, err := synclet.ParseInputState([]byte(`[{"type":"STREAM","stream":{"stream_descriptor":{"name":"users"},"stream_state":{"phase":"incremental"}}}]`))
	require.NoError(t, err)

	state, err := loadCDCState(prev, []string{"users"})
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

			state, err := loadCDCState(prev, []string{"users"})
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
	require.NoError(t, emitCDCState(synclet.NewEmitter(&out), "shop", state))

	states := emittedStates(t, &out)
	require.Len(t, states, 1)
	require.Equal(t, synclet.StateTypeGlobal, states[0].Type)
	require.NotNil(t, states[0].Global)
	assert.JSONEq(t, `{"binlog_file":"mysql-bin.000007","binlog_pos":1234,"gtid_set":"uuid:1-5","snapshot_done":false}`,
		string(states[0].Global.SharedState))
	require.Len(t, states[0].Global.StreamStates, 2)
	assert.Equal(t, streamDescriptor("shop", "orders"), states[0].Global.StreamStates[0].StreamDescriptor)
	assert.JSONEq(t, `{"last_pk_vals":{"id":42},"done":false}`, string(states[0].Global.StreamStates[0].StreamState))
	assert.Equal(t, streamDescriptor("shop", "users"), states[0].Global.StreamStates[1].StreamDescriptor)
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
	require.NoError(t, emitCDCState(synclet.NewEmitter(&out), "shop", want))
	saved, err := json.Marshal(emittedStates(t, &out))
	require.NoError(t, err)

	prev, err := synclet.ParseInputState(saved)
	require.NoError(t, err)
	require.Equal(t, synclet.StateTypeGlobal, prev.Type)

	got, err := loadCDCState(prev, []string{"users"})
	require.NoError(t, err)
	assert.Equal(t, want, got)
}

func TestSavedStreamState_ReadsStateWithoutNamespace(t *testing.T) {
	prev, err := synclet.ParseInputState([]byte(`[{"type":"STREAM","stream":{"stream_descriptor":{"name":"users"},"stream_state":{"done":true}}}]`))
	require.NoError(t, err)

	assert.JSONEq(t, `{"done":true}`, string(savedStreamState(prev, streamDescriptor("shop", "users"))))
	assert.Nil(t, savedStreamState(prev, streamDescriptor("shop", "orders")))
}

func TestLoadCDCState_KeepsPrimaryKeysExact(t *testing.T) {
	for name, saved := range map[string]string{
		"global": `[{"type":"GLOBAL","global":{"shared_state":{"binlog_file":"b.1","binlog_pos":4},"stream_states":[{"stream_descriptor":{"name":"users"},"stream_state":{"last_pk_vals":{"id":9007199254740993}}}]}}]`,
		"legacy": `{"binlog_file":"b.1","binlog_pos":4,"snapshot_streams":{"users":{"last_pk_vals":{"id":9007199254740993}}}}`,
	} {
		t.Run(name, func(t *testing.T) {
			prev, err := synclet.ParseInputState([]byte(saved))
			require.NoError(t, err)

			state, err := loadCDCState(prev, []string{"users"})
			require.NoError(t, err)
			assert.Equal(t, json.Number("9007199254740993"), state.SnapshotStreams["users"].LastPKVals["id"])
		})
	}
}

func TestLoadCDCState_DropsStreamsOutsideCatalog(t *testing.T) {
	for name, saved := range map[string]string{
		"global": `[{"type":"GLOBAL","global":{"shared_state":{"binlog_file":"b.1","binlog_pos":4},"stream_states":[
			{"stream_descriptor":{"name":"users","namespace":"shop"},"stream_state":{"done":true}},
			{"stream_descriptor":{"name":"p_users","namespace":"p_shop"},"stream_state":{"done":true}},
			{"stream_descriptor":{"name":"p_p_users"},"stream_state":{"done":true}}]}}]`,
		"legacy": `{"binlog_file":"b.1","binlog_pos":4,"snapshot_streams":{"users":{"done":true},"p_users":{"done":true}}}`,
	} {
		t.Run(name, func(t *testing.T) {
			prev, err := synclet.ParseInputState([]byte(saved))
			require.NoError(t, err)

			state, err := loadCDCState(prev, []string{"users", "orders"})
			require.NoError(t, err)
			assert.Equal(t, map[string]*CDCStreamSnapshotState{"users": {Done: true}}, state.SnapshotStreams)
			assert.Equal(t, "b.1", state.BinlogFile)
		})
	}
}
