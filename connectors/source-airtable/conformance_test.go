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
	broken := json.RawMessage(`{"personal_access_token":"patWrong.token"}`)

	t.Run("full refresh", func(t *testing.T) {
		catalog := catalogOf(t, src, synclet.SyncModeFullRefresh, tasksStream, allTypesStream, contactsStream, dealsStream)
		testkit.RunSourceSuite(t, src, testkit.SourceSuite{Config: json.RawMessage(testConfig), BrokenConfig: broken, Catalog: catalog})
	})

	t.Run("incremental", func(t *testing.T) {
		catalog := catalogOf(t, src, synclet.SyncModeIncremental, tasksStream, allTypesStream)
		testkit.RunSourceSuite(t, src, testkit.SourceSuite{Config: json.RawMessage(testConfig), BrokenConfig: broken, Catalog: catalog})
	})
}
