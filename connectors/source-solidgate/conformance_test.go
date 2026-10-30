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
			// The first read saves today as the last day read, so the second
			// starts lookback_days before today, whatever cursor values the
			// first read had. subscription_invoices has no lower bound: the
			// subscriptions report is requested by the subscription's
			// updated_at and lists every invoice of each subscription, however
			// long ago the invoice changed.
			Lookback: func(desc synclet.StreamDescriptor, cursor any) (any, error) {
				if desc.Name == streamSubscriptionInvoices {
					return time.Time{}.Format(utcTimeLayout), nil
				}
				layout := localTimeLayout
				if _, err := time.Parse(utcTimeLayout, cursor.(string)); err == nil {
					layout = utcTimeLayout
				}
				return day(testNow).AddDate(0, 0, -defaultLookbackDays).Format(layout), nil
			},
		})
	})
}
