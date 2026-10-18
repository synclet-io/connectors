package main

import (
	"testing"

	synclet "github.com/synclet-io/synclet-go-sdk"
	"github.com/synclet-io/synclet-go-sdk/testkit"
)

func TestConformance(t *testing.T) {
	srv, _ := fixtureServer(t, nil)
	src, _ := testSource(srv)
	broken := testConfig(t, func(c *Config) { c.Credentials.ServiceAccountInfo = serviceAccountKey(t, otherKey()) })

	t.Run("full refresh", func(t *testing.T) {
		cfg := testConfig(t, func(c *Config) { c.AdditionalSpreadsheetIDs = []string{testSheetB} })
		testkit.RunSourceSuite(t, src, testkit.SourceSuite{
			Config: cfg, BrokenConfig: broken, Catalog: configured(t, src, cfg, synclet.SyncModeFullRefresh),
		})
	})

	t.Run("full refresh unformatted", func(t *testing.T) {
		cfg := testConfig(t, func(c *Config) { c.ValueRender = valueRenderUnformatted; withStable(c) })
		cat := configured(t, src, cfg, synclet.SyncModeFullRefresh, "People", "Prices", "Gaps")
		testkit.RunSourceSuite(t, src, testkit.SourceSuite{Config: cfg, BrokenConfig: broken, Catalog: cat})
	})

	t.Run("incremental", func(t *testing.T) {
		cfg := testConfig(t, func(c *Config) { withIncremental(c); c.AdditionalSpreadsheetIDs = []string{testSheetB} })
		testkit.RunSourceSuite(t, src, testkit.SourceSuite{
			Config: cfg, BrokenConfig: broken, Catalog: configured(t, src, cfg, synclet.SyncModeIncremental),
		})
	})
}
