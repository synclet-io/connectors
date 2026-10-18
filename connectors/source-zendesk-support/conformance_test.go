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
	broken := json.RawMessage(`{"subdomain":"acme","email":"` + testEmail + `","api_token":"wrong-api-token","start_date":"2026-10-01"}`)

	t.Run("full refresh", func(t *testing.T) {
		var names []string
		for _, d := range streams {
			names = append(names, d.name)
		}
		catalog := catalogOf(t, src, synclet.SyncModeFullRefresh, names...)
		testkit.RunSourceSuite(t, src, testkit.SourceSuite{Config: json.RawMessage(testConfig), BrokenConfig: broken, Catalog: catalog})
	})

	t.Run("incremental", func(t *testing.T) {
		catalog := catalogOf(t, src, synclet.SyncModeIncremental, "tickets", "users", "ticket_events")
		testkit.RunSourceSuite(t, src, testkit.SourceSuite{Config: json.RawMessage(testConfig), BrokenConfig: broken, Catalog: catalog})
	})
}
