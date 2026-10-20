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
	broken := json.RawMessage(`{"api_key":"wrong-api-key","secret_key":"wrong-secret-key","start_date":"2026-10-16T00:00:00Z"}`)

	for _, mode := range []synclet.SyncMode{synclet.SyncModeFullRefresh, synclet.SyncModeIncremental} {
		t.Run(string(mode), func(t *testing.T) {
			testkit.RunSourceSuite(t, src, testkit.SourceSuite{
				Config: json.RawMessage(testConfig), BrokenConfig: broken,
				Catalog: configured(t, src, mode, streamNames...),
			})
		})
	}
}
