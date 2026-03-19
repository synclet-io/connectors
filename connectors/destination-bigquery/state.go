package main

import (
	"encoding/json"
	"fmt"

	airbyte "github.com/saturn4er/airbyte-go-sdk"
)

// pendingState holds a raw state message queued for emission after flush.
type pendingState struct {
	raw json.RawMessage
}

// stateEnvelope is used to detect the state type from the raw JSON.
type stateEnvelope struct {
	Type   airbyte.StateType              `json:"type"`
	Stream *airbyte.StreamState           `json:"stream,omitempty"`
	Global *globalStateEnvelope           `json:"global,omitempty"`
	Data   interface{}                    `json:"data,omitempty"`
}

// globalStateEnvelope mirrors the Airbyte global state structure.
type globalStateEnvelope struct {
	SharedState  map[string]interface{}   `json:"shared_state,omitempty"`
	StreamStates []airbyte.StreamState    `json:"stream_states"`
}

// emitPendingStates emits all queued state messages via the tracker.
// This function is only called after a successful flush (D-13).
func emitPendingStates(tracker airbyte.MessageTracker, states []pendingState) error {
	for i, ps := range states {
		if err := emitSingleState(tracker, ps.raw); err != nil {
			return fmt.Errorf("emitting state %d: %w", i, err)
		}
	}
	return nil
}

func emitSingleState(tracker airbyte.MessageTracker, raw json.RawMessage) error {
	var env stateEnvelope
	if err := json.Unmarshal(raw, &env); err != nil {
		return fmt.Errorf("unmarshaling state: %w", err)
	}

	switch env.Type {
	case airbyte.StateTypeStream:
		if env.Stream == nil {
			return fmt.Errorf("STREAM state missing stream field")
		}
		return tracker.State(airbyte.StateTypeStream, env.Stream)

	case airbyte.StateTypeGlobal:
		if env.Global == nil {
			return fmt.Errorf("GLOBAL state missing global field")
		}
		// The SDK expects *globalState which is an internal type.
		// Re-emit the raw global data structure.
		return tracker.State(airbyte.StateTypeGlobal, env.Global)

	case airbyte.StateTypeLegacy:
		return tracker.State(airbyte.StateTypeLegacy, env.Data)

	default:
		// Default to LEGACY for unknown types.
		return tracker.State(airbyte.StateTypeLegacy, env.Data)
	}
}
