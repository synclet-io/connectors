package main

import (
	"encoding/json"
	"testing"

	synclet "github.com/synclet-io/synclet-go-sdk"
	"github.com/synclet-io/synclet-go-sdk/testkit"
)

func TestConformance(t *testing.T) {
	srv, _ := fixtureServer(t, nil)
	src := testSource(srv)
	streams := discoverStreams(t, src)
	broken := json.RawMessage(`{"app_id":"wrong-app-id","start_date":"2026-10-14"}`)

	t.Run("full refresh", func(t *testing.T) {
		catalog := &synclet.ConfiguredCatalog{}
		for _, name := range []string{streamCurrencies, streamLatestRates, streamHistoricalRates} {
			catalog.Streams = append(catalog.Streams, synclet.ConfiguredStream{
				Stream: streams[name], SyncMode: synclet.SyncModeFullRefresh, DestinationSyncMode: synclet.DestinationSyncModeOverwrite,
			})
		}
		testkit.RunSourceSuite(t, src, testkit.SourceSuite{Config: json.RawMessage(testConfig), BrokenConfig: broken, Catalog: catalog})
	})

	t.Run("incremental", func(t *testing.T) {
		catalog := &synclet.ConfiguredCatalog{Streams: []synclet.ConfiguredStream{{
			Stream: streams[streamHistoricalRates], SyncMode: synclet.SyncModeIncremental, DestinationSyncMode: synclet.DestinationSyncModeAppendDedup,
		}}}
		testkit.RunSourceSuite(t, src, testkit.SourceSuite{Config: json.RawMessage(testConfig), BrokenConfig: broken, Catalog: catalog})
	})
}
