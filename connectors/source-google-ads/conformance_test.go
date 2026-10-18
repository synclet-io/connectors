package main

import (
	"testing"

	synclet "github.com/synclet-io/synclet-go-sdk"
	"github.com/synclet-io/synclet-go-sdk/testkit"
)

func TestConformance(t *testing.T) {
	srv, _ := fixtureServer(t, nil)
	src, _ := testSource(srv)
	cfg := testConfig(t, withKeywords)
	broken := testConfig(t, func(c *Config) { withKeywords(c); c.RefreshToken = "revoked-refresh-token" })
	all := []string{"my_keywords"}
	for _, d := range builtinDefs {
		all = append(all, d.name)
	}

	t.Run("full refresh", func(t *testing.T) {
		testkit.RunSourceSuite(t, src, testkit.SourceSuite{
			Config: cfg, BrokenConfig: broken, Catalog: configured(t, src, cfg, synclet.SyncModeFullRefresh, all...),
		})
	})

	t.Run("incremental", func(t *testing.T) {
		testkit.RunSourceSuite(t, src, testkit.SourceSuite{
			Config: cfg, BrokenConfig: broken,
			Catalog: configured(t, src, cfg, synclet.SyncModeIncremental,
				streamCampaignPerformance, streamAdGroupPerformance, streamAdPerformance, streamGeographicViewDaily, "my_keywords"),
			// A read starts lookback_days before the saved date, never before
			// start_date.
			Lookback: func(_ synclet.StreamDescriptor, cursor any) (any, error) {
				d, err := parseDate("cursor", cursor.(string))
				if err != nil {
					return nil, err
				}
				return max(d.AddDate(0, 0, -defaultLookbackDays).Format(dateLayout), "2026-10-01"), nil
			},
		})
	})
}
