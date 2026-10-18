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
	broken := json.RawMessage(`{"accounts":[{"name":"acme_test","public_key":"api_pk_test","secret_key":"api_sk_wrong"}],"start_date":"2026-10-16"}`)
	var all, incremental []string
	for _, d := range streamDefs {
		all = append(all, d.name)
		if d.cursor != "" {
			incremental = append(incremental, d.name)
		}
	}

	t.Run("full refresh", func(t *testing.T) {
		testkit.RunSourceSuite(t, src, testkit.SourceSuite{
			Config: json.RawMessage(testConfig), BrokenConfig: broken, Catalog: configured(t, src, synclet.SyncModeFullRefresh, all...),
		})
	})

	t.Run("incremental", func(t *testing.T) {
		testkit.RunSourceSuite(t, src, testkit.SourceSuite{
			Config: json.RawMessage(testConfig), BrokenConfig: broken,
			Catalog: configured(t, src, synclet.SyncModeIncremental, incremental...),
			// A read starts lookback_days before the day of the saved cursor,
			// never before start_date.
			Lookback: func(_ synclet.StreamDescriptor, cursor any) (any, error) {
				s := cursor.(string)
				c, err := parseCursor(s)
				if err != nil {
					return nil, err
				}
				layout := localTimeLayout
				if _, err := time.Parse(utcTimeLayout, s); err == nil {
					layout = utcTimeLayout
				}
				return day(c).AddDate(0, 0, -defaultLookbackDays).Format(layout), nil
			},
		})
	})
}
