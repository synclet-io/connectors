package main

import (
	"encoding/json"
	"testing"

	synclet "github.com/synclet-io/synclet-go-sdk"
	"github.com/synclet-io/synclet-go-sdk/testkit"
)

func TestConformance(t *testing.T) {
	srv, _ := fixtureServer(t, nil)
	src := testSource(srv, newClock())
	broken := json.RawMessage(`{"api_key":"wrong-key","project_id":"` + testProj + `"}`)
	testkit.RunSourceSuite(t, src, testkit.SourceSuite{
		Config: json.RawMessage(testConfig), BrokenConfig: broken,
		Catalog: catalogOf(t, src, synclet.SyncModeFullRefresh, allStreams()...),
	})
}
