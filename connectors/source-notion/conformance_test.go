package main

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
	"github.com/synclet-io/synclet-go-sdk/testkit"
)

func TestConformance(t *testing.T) {
	srv, _ := fixtureServer(t, nil)
	src := testSource(srv)
	broken := json.RawMessage(`{"access_token":"ntn_wrong"}`)

	t.Run("full refresh", func(t *testing.T) {
		catalog := catalogOf(t, src, synclet.SyncModeFullRefresh, usersStream, databasesStream, pagesStream, blocksStream)
		testkit.RunSourceSuite(t, src, testkit.SourceSuite{Config: json.RawMessage(testConfig), BrokenConfig: broken, Catalog: catalog})
	})

	t.Run("incremental", func(t *testing.T) {
		catalog := catalogOf(t, src, synclet.SyncModeIncremental, databasesStream, pagesStream, blocksStream)
		bounds := lookbackBounds(t, src, catalog)
		testkit.RunSourceSuite(t, src, testkit.SourceSuite{
			Config: json.RawMessage(testConfig), BrokenConfig: broken, Catalog: catalog,
			Lookback: func(desc synclet.StreamDescriptor, cursor any) (any, error) {
				if b, ok := bounds[desc.Name]; ok {
					return b, nil
				}
				s, ok := cursor.(string)
				if !ok {
					return nil, fmt.Errorf("cursor %v is not a string", cursor)
				}
				t, err := time.Parse(time.RFC3339Nano, s)
				if err != nil {
					return nil, err
				}
				return formatTime(t.Add(-defaultLookbackMinutes * time.Minute)), nil
			},
		})
	})
}

// lookbackBounds returns, for the pages and blocks streams, the oldest
// cursor a read from the state of a first read may emit: the oldest of the
// state's cursors, search's and each data source's, less the lookback
// window.
func lookbackBounds(t *testing.T, src Source, catalog *synclet.ConfiguredCatalog) map[string]string {
	t.Helper()
	out := runRead(t, src, testConfig, catalog, nil)
	require.NoError(t, out.Err)
	bounds := map[string]string{}
	for _, st := range out.States() {
		name := st.Stream.StreamDescriptor.Name
		if name != pagesStream && name != blocksStream {
			continue
		}
		var ps pageState
		require.NoError(t, json.Unmarshal(st.Stream.StreamState, &ps))
		oldest := ps.LastEditedTime
		for _, v := range ps.DataSources {
			if oldest == "" || v < oldest {
				oldest = v
			}
		}
		at, err := time.Parse(time.RFC3339Nano, oldest)
		require.NoError(t, err)
		bounds[name] = formatTime(at.Add(-defaultLookbackMinutes * time.Minute))
	}
	return bounds
}
