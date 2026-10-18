package main

import (
	"testing"

	synclet "github.com/synclet-io/synclet-go-sdk"
	"github.com/synclet-io/synclet-go-sdk/testkit"
)

func TestConformance(t *testing.T) {
	o := fixtureServer(t, nil)
	src, _ := testSource(o)
	cfg := testCfg(o)
	broken := cfg
	broken.RefreshToken = "revoked-refresh-token"

	t.Run("full refresh", func(t *testing.T) {
		testkit.RunSourceSuite(t, src, testkit.SourceSuite{
			Config: rawConfig(t, cfg), BrokenConfig: rawConfig(t, broken),
			Catalog: catalogOf(t, src, cfg, synclet.SyncModeFullRefresh, "Account", "Contact", "Invoice__c", "Region__mdt"),
		})
	})

	t.Run("incremental", func(t *testing.T) {
		testkit.RunSourceSuite(t, src, testkit.SourceSuite{
			Config: rawConfig(t, cfg), BrokenConfig: rawConfig(t, broken),
			Catalog: catalogOf(t, src, cfg, synclet.SyncModeIncremental, "Account", "Contact", "Invoice__c"),
		})
	})
}
