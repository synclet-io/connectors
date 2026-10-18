package main

import (
	"encoding/json"
	"fmt"

	airbyte "github.com/saturn4er/airbyte-go-sdk"
)

// FullRefreshState tracks progress for full refresh reads with PK-based chunking.
type FullRefreshState struct {
	LastPKVals map[string]interface{} `json:"last_pk_vals,omitempty"`
	Done       bool                   `json:"done"`
}

// IncrementalState tracks progress for incremental reads.
type IncrementalState struct {
	Phase           string                 `json:"phase"` // "snapshot" or "incremental"
	CursorField     string                 `json:"cursor_field"`
	CursorValue     interface{}            `json:"cursor_value,omitempty"`
	MaxCursorValue  interface{}            `json:"max_cursor_value,omitempty"`
	SnapshotLastPK  map[string]interface{} `json:"snapshot_last_pk,omitempty"`
	SnapshotDone    bool                   `json:"snapshot_done"`
}

// CDCState tracks progress for CDC (binlog) replication.
// Emitted as StateTypeLegacy because the SDK's globalState is unexported.
type CDCState struct {
	BinlogFile     string                             `json:"binlog_file"`
	BinlogPos      uint32                             `json:"binlog_pos"`
	GTIDSet        string                             `json:"gtid_set,omitempty"`
	SnapshotStreams map[string]*CDCStreamSnapshotState `json:"snapshot_streams"`
	SnapshotDone   bool                               `json:"snapshot_done"`
}

// CDCStreamSnapshotState tracks per-stream progress during CDC initial snapshot.
type CDCStreamSnapshotState struct {
	LastPKVals map[string]interface{} `json:"last_pk_vals,omitempty"`
	Done       bool                   `json:"done"`
}

// loadStreamState loads the state for a specific stream from the previous state file.
func loadStreamState(prevStatePath string, streamName string) (map[string]interface{}, error) {
	if prevStatePath == "" {
		return nil, nil
	}

	states, err := airbyte.LoadStreamStates(prevStatePath)
	if err != nil {
		return nil, fmt.Errorf("loading stream states: %w", err)
	}

	return states[streamName], nil
}

// loadCDCState loads the CDC (legacy) state from the previous state file.
func loadCDCState(prevStatePath string) (*CDCState, error) {
	if prevStatePath == "" {
		return nil, nil
	}

	var entries []json.RawMessage
	if err := airbyte.UnmarshalFromPath(prevStatePath, &entries); err != nil {
		return nil, nil // no state file or invalid format
	}

	for _, raw := range entries {
		var envelope struct {
			Type string          `json:"type"`
			Data json.RawMessage `json:"data"`
		}
		if err := json.Unmarshal(raw, &envelope); err != nil {
			continue
		}
		if envelope.Type == string(airbyte.StateTypeLegacy) {
			var cdcState CDCState
			if err := json.Unmarshal(envelope.Data, &cdcState); err != nil {
				return nil, fmt.Errorf("parsing CDC state: %w", err)
			}
			return &cdcState, nil
		}
	}

	return nil, nil
}

// emitStreamState emits a per-stream state checkpoint.
func emitStreamState(tracker airbyte.MessageTracker, streamName string, state interface{}) error {
	return tracker.State(airbyte.StateTypeStream, airbyte.StreamState{
		StreamDescriptor: airbyte.StreamDescriptor{Name: streamName},
		StreamState:      toMap(state),
	})
}

// emitCDCState emits a legacy (global) CDC state checkpoint.
func emitCDCState(tracker airbyte.MessageTracker, state *CDCState) error {
	return tracker.State(airbyte.StateTypeLegacy, state)
}

// toMap converts a struct to map[string]interface{} via JSON round-trip.
func toMap(v interface{}) map[string]interface{} {
	data, err := json.Marshal(v)
	if err != nil {
		return nil
	}
	var m map[string]interface{}
	if err := json.Unmarshal(data, &m); err != nil {
		return nil
	}
	return m
}

// parseStreamState deserializes a stream state map into the target struct.
func parseStreamState(raw map[string]interface{}, target interface{}) error {
	data, err := json.Marshal(raw)
	if err != nil {
		return fmt.Errorf("marshaling state: %w", err)
	}
	return json.Unmarshal(data, target)
}
