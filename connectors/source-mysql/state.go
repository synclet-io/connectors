package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"slices"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

// FullRefreshState tracks progress for full refresh reads with PK-based chunking.
type FullRefreshState struct {
	LastPKVals map[string]interface{} `json:"last_pk_vals,omitempty"`
	Done       bool                   `json:"done"`
}

// IncrementalState tracks progress for incremental reads.
type IncrementalState struct {
	Phase          string                 `json:"phase"` // "snapshot" or "incremental"
	CursorField    string                 `json:"cursor_field"`
	CursorValue    interface{}            `json:"cursor_value,omitempty"`
	MaxCursorValue interface{}            `json:"max_cursor_value,omitempty"`
	SnapshotLastPK map[string]interface{} `json:"snapshot_last_pk,omitempty"`
	SnapshotDone   bool                   `json:"snapshot_done"`
}

// CDCState tracks progress for CDC (binlog) replication.
type CDCState struct {
	BinlogFile      string                             `json:"binlog_file"`
	BinlogPos       uint32                             `json:"binlog_pos"`
	GTIDSet         string                             `json:"gtid_set,omitempty"`
	SnapshotStreams map[string]*CDCStreamSnapshotState `json:"snapshot_streams"`
	SnapshotDone    bool                               `json:"snapshot_done"`
}

// CDCStreamSnapshotState tracks per-stream progress during CDC initial snapshot.
type CDCStreamSnapshotState struct {
	LastPKVals map[string]interface{} `json:"last_pk_vals,omitempty"`
	Done       bool                   `json:"done"`
}

// streamDescriptor identifies a table's stream in records and state
// messages: the table name, with its database as namespace.
func streamDescriptor(database, table string) synclet.StreamDescriptor {
	return synclet.StreamDescriptor{Name: table, Namespace: database}
}

// savedStreamState returns the saved state of a stream, falling back to an
// entry without namespace as written by 0.1.x of this connector.
func savedStreamState(prev *synclet.InputState, desc synclet.StreamDescriptor) json.RawMessage {
	if raw := prev.Stream(desc); raw != nil {
		return raw
	}
	return prev.Stream(synclet.StreamDescriptor{Name: desc.Name})
}

// decodeState decodes saved state into target, keeping numbers as
// json.Number so cursor and primary key values stay exact.
func decodeState(raw json.RawMessage, target any) error {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	return dec.Decode(target)
}

// parseStreamState decodes a stream's saved state into target. A state that
// does not decode is logged and leaves target partly filled.
func parseStreamState(ctx context.Context, streamName string, raw json.RawMessage, target any) {
	if err := decodeState(raw, target); err != nil {
		synclet.Logger(ctx).Warn(fmt.Sprintf("Ignoring unreadable state of stream %s: %v", streamName, err))
	}
}

// cdcSharedState is the shared part of the GLOBAL CDC state: the binlog
// position every stream continues from.
type cdcSharedState struct {
	BinlogFile   string `json:"binlog_file"`
	BinlogPos    uint32 `json:"binlog_pos"`
	GTIDSet      string `json:"gtid_set,omitempty"`
	SnapshotDone bool   `json:"snapshot_done"`
}

// loadCDCState returns the CDC state saved by the previous sync, or nil when
// there is none. It reads a GLOBAL state, and a LEGACY state as written by
// 0.1.x of this connector. Snapshot progress is kept only for the streams
// named in streams.
func loadCDCState(prev *synclet.InputState, streams []string) (*CDCState, error) {
	cdcState, err := decodeCDCState(prev)
	if err != nil || cdcState == nil {
		return cdcState, err
	}
	for name := range cdcState.SnapshotStreams {
		if !slices.Contains(streams, name) {
			delete(cdcState.SnapshotStreams, name)
		}
	}
	return cdcState, nil
}

func decodeCDCState(prev *synclet.InputState) (*CDCState, error) {
	switch prev.Type {
	case synclet.StateTypeGlobal:
		var shared cdcSharedState
		if err := json.Unmarshal(prev.Shared, &shared); err != nil {
			return nil, fmt.Errorf("parsing CDC shared state: %w", err)
		}
		cdcState := &CDCState{
			BinlogFile:      shared.BinlogFile,
			BinlogPos:       shared.BinlogPos,
			GTIDSet:         shared.GTIDSet,
			SnapshotDone:    shared.SnapshotDone,
			SnapshotStreams: make(map[string]*CDCStreamSnapshotState, len(prev.Streams)),
		}
		for _, st := range prev.Streams {
			ss := &CDCStreamSnapshotState{}
			if len(st.StreamState) > 0 {
				if err := decodeState(st.StreamState, ss); err != nil {
					return nil, fmt.Errorf("parsing CDC state of stream %s: %w", st.StreamDescriptor.Name, err)
				}
			}
			cdcState.SnapshotStreams[st.StreamDescriptor.Name] = ss
		}
		return cdcState, nil
	case synclet.StateTypeLegacy:
		var cdcState CDCState
		if err := decodeState(prev.Legacy, &cdcState); err != nil {
			return nil, fmt.Errorf("parsing CDC state: %w", err)
		}
		return &cdcState, nil
	default:
		return nil, nil
	}
}

// emitStreamState emits a per-stream state checkpoint.
func emitStreamState(em *synclet.Emitter, desc synclet.StreamDescriptor, state any) error {
	return em.StreamState(desc, state)
}

// emitCDCState emits a GLOBAL CDC state checkpoint: the binlog position as
// shared state and the snapshot progress of each stream of database as its
// stream state.
func emitCDCState(em *synclet.Emitter, database string, state *CDCState) error {
	streams := make(map[synclet.StreamDescriptor]any, len(state.SnapshotStreams))
	for name, ss := range state.SnapshotStreams {
		streams[streamDescriptor(database, name)] = ss
	}
	return em.GlobalState(cdcSharedState{
		BinlogFile:   state.BinlogFile,
		BinlogPos:    state.BinlogPos,
		GTIDSet:      state.GTIDSet,
		SnapshotDone: state.SnapshotDone,
	}, streams)
}
