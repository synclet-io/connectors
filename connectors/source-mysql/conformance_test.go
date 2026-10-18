package main

import (
	"testing"

	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
	"github.com/synclet-io/synclet-go-sdk/testkit"
)

func TestConformance(t *testing.T) {
	cfg := testConfig(t)
	db := openTestDB(t, cfg)
	createUsersTable(t, db, "conf_full")
	createUsersTable(t, db, "conf_inc")

	out, err := testkit.RunSource(t.Context(), NewMySQLSource(), synclet.Request{Command: synclet.CommandDiscover, Config: configJSON(t, cfg)})
	require.NoError(t, err)
	require.NoError(t, out.Err)
	streams := map[string]synclet.Stream{}
	for _, s := range out.Catalog().Streams {
		streams[s.Name] = s
	}
	require.Contains(t, streams, "conf_full")
	require.Contains(t, streams, "conf_inc")

	broken := cfg
	broken.Password = "wrong"
	testkit.RunSourceSuite(t, NewMySQLSource(), testkit.SourceSuite{
		Config:       configJSON(t, cfg),
		BrokenConfig: configJSON(t, broken),
		Catalog: &synclet.ConfiguredCatalog{Streams: []synclet.ConfiguredStream{
			{
				Stream:              streams["conf_full"],
				SyncMode:            synclet.SyncModeFullRefresh,
				DestinationSyncMode: synclet.DestinationSyncModeOverwrite,
			},
			{
				Stream:              streams["conf_inc"],
				SyncMode:            synclet.SyncModeIncremental,
				CursorField:         []string{"id"},
				DestinationSyncMode: synclet.DestinationSyncModeAppend,
			},
		}},
	})
}
