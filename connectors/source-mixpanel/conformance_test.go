package main

import (
	"testing"
	"time"

	synclet "github.com/synclet-io/synclet-go-sdk"
	"github.com/synclet-io/synclet-go-sdk/testkit"
)

func TestConformance(t *testing.T) {
	srv, _ := fixtureServer(t, nil)
	src := testSource(srv, newClock())
	cfg := testConfig(t)
	broken := testConfig(t, func(c *Config) { c.Credentials.Secret = "wrong-secret" })

	t.Run("full refresh", func(t *testing.T) {
		testkit.RunSourceSuite(t, src, testkit.SourceSuite{
			Config: cfg, BrokenConfig: broken, Catalog: catalogOf(t, src, cfg, synclet.SyncModeFullRefresh, allStreams()...),
		})
	})

	t.Run("incremental", func(t *testing.T) {
		testkit.RunSourceSuite(t, src, testkit.SourceSuite{
			Config: cfg, BrokenConfig: broken,
			Catalog: catalogOf(t, src, cfg, synclet.SyncModeIncremental, incrementalStreams()...),
			// export, funnels and revenue start again attribution_window days
			// before the saved day, never before start_date.
			Lookback: func(desc synclet.StreamDescriptor, cursor any) (any, error) {
				switch desc.Name {
				case streamExport:
					ts, err := time.Parse(time.RFC3339Nano, cursor.(string))
					if err != nil {
						return nil, err
					}
					local := ts.In(projectZone)
					day := time.Date(local.Year(), local.Month(), local.Day()-defaultAttributionWindow, 0, 0, 0, 0, projectZone)
					start := time.Date(2026, 10, 10, 0, 0, 0, 0, projectZone)
					if day.Before(start) {
						day = start
					}
					return day.Format(time.RFC3339), nil
				case streamFunnels, streamRevenue:
					d, err := time.Parse(dateLayout, cursor.(string))
					if err != nil {
						return nil, err
					}
					return max(d.AddDate(0, 0, -defaultAttributionWindow).Format(dateLayout), "2026-10-10"), nil
				case streamCohortMembers:
					// Each cohort resumes at its own latest last_seen; cohort
					// 102's is the earliest of the fixtures.
					return "2026-10-12T08:15:00", nil
				}
				return cursor, nil
			},
		})
	})
}
