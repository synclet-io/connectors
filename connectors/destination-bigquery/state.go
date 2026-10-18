package main

import (
	"fmt"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

// emitPendingStates echoes the queued state messages. It is called only
// after a successful flush (D-13).
func emitPendingStates(em *synclet.Emitter, states []synclet.StateMessage) error {
	for i, st := range states {
		if err := emitSingleState(em, st); err != nil {
			return fmt.Errorf("emitting state %d: %w", i, err)
		}
	}
	return nil
}

// emitSingleState echoes one state message. A state of an unknown type is
// echoed as LEGACY with its data.
func emitSingleState(em *synclet.Emitter, st synclet.StateMessage) error {
	switch st.Type {
	case synclet.StateTypeStream:
		if st.Stream == nil {
			return fmt.Errorf("STREAM state missing stream field")
		}
		return em.State(synclet.StateMessage{Type: synclet.StateTypeStream, Stream: st.Stream})

	case synclet.StateTypeGlobal:
		if st.Global == nil {
			return fmt.Errorf("GLOBAL state missing global field")
		}
		return em.State(synclet.StateMessage{Type: synclet.StateTypeGlobal, Global: st.Global})

	default:
		return em.State(synclet.StateMessage{Type: synclet.StateTypeLegacy, Data: st.Data})
	}
}
