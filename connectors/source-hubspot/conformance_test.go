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
	broken := json.RawMessage(`{"access_token":"wrong-private-app-token","start_date":"2026-10-01"}`)
	objects := []string{"contacts", "companies", "deals", "tickets", "line_items", "products", "cars"}

	t.Run("full refresh", func(t *testing.T) {
		var names []string
		for n := range discoverStreams(t, src) {
			names = append(names, n)
		}
		catalog := catalogOf(t, src, synclet.SyncModeFullRefresh, names...)
		testkit.RunSourceSuite(t, src, testkit.SourceSuite{Config: json.RawMessage(testConfig), BrokenConfig: broken, Catalog: catalog})
	})

	t.Run("incremental", func(t *testing.T) {
		catalog := catalogOf(t, src, synclet.SyncModeIncremental, objects...)
		testkit.RunSourceSuite(t, src, testkit.SourceSuite{Config: json.RawMessage(testConfig), BrokenConfig: broken, Catalog: catalog})
	})
}
