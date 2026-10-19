package main

import (
	"fmt"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

// emitState echoes one state message as the source sent it.
func emitState(em *synclet.Emitter, st synclet.StateMessage) error {
	switch st.Type {
	case synclet.StateTypeStream:
		if st.Stream == nil {
			return fmt.Errorf("STREAM state without stream")
		}
	case synclet.StateTypeGlobal:
		if st.Global == nil {
			return fmt.Errorf("GLOBAL state without global")
		}
	}
	return em.State(st)
}
