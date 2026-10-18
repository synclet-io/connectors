package main

import (
	"encoding/json"
	"testing"
	"time"

	synclet "github.com/synclet-io/synclet-go-sdk"
	"github.com/synclet-io/synclet-go-sdk/testkit"
)

func TestConformance(t *testing.T) {
	srv, _ := fixtureServer(t, nil)
	src := testSource(srv)
	broken := json.RawMessage(`{"api_key":"wrong-report-key","start_date":"2026-10-14"}`)

	for _, mode := range []synclet.SyncMode{synclet.SyncModeFullRefresh, synclet.SyncModeIncremental} {
		t.Run(string(mode), func(t *testing.T) {
			testkit.RunSourceSuite(t, src, testkit.SourceSuite{
				Config: json.RawMessage(testConfig), BrokenConfig: broken,
				Catalog: configured(t, src, mode, streamAdvertiserReport, streamMaxRevenueReport),
				// A read restarts lookback_days before the saved day, never
				// before the API's request window.
				Lookback: func(_ synclet.StreamDescriptor, cursor any) (any, error) {
					d, err := time.Parse(dateLayout, cursor.(string))
					if err != nil {
						return nil, err
					}
					from := d.AddDate(0, 0, -defaultLookbackDays)
					if earliest := day(testNow).AddDate(0, 0, -(requestWindowDays - 1)); from.Before(earliest) {
						from = earliest
					}
					return from.Format(dateLayout), nil
				},
			})
		})
	}
}
