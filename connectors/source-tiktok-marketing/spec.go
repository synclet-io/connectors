package main

import (
	"context"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

func order(n int) *int { return &n }

func typed(t ...synclet.PropType) synclet.PropertyType {
	return synclet.PropertyType{Type: synclet.PropTypes(t)}
}

// Spec returns the connector's capabilities and configuration schema.
func (Source) Spec(_ context.Context) (*synclet.ConnectorSpecification, error) {
	return &synclet.ConnectorSpecification{
		DocumentationURL:    "https://github.com/synclet-io/connectors/tree/main/connectors/source-tiktok-marketing",
		SupportsIncremental: true,
		ProtocolVersion:     "0.5.2",
		SupportedDestinationSyncModes: []synclet.DestinationSyncMode{
			synclet.DestinationSyncModeOverwrite,
			synclet.DestinationSyncModeAppend,
			synclet.DestinationSyncModeAppendDedup,
		},
		ConnectionSpecification: synclet.ConnectionSpecification{
			Title:       "TikTok Marketing Source Spec",
			Description: "Reads TikTok advertisers, campaigns, ad groups, ads and daily BASIC reports from the TikTok Business API.",
			Type:        "object",
			Required:    []synclet.PropertyName{"access_token", "advertiser_ids", "start_date"},
			Properties: synclet.Properties{Properties: map[synclet.PropertyName]synclet.PropertySpec{
				"access_token": {
					Title:        "Access Token",
					Description:  "Long-lived access token of a TikTok Business API app, or a sandbox token. It is sent in the Access-Token header.",
					PropertyType: typed(synclet.String), IsSecret: true, Order: order(0),
				},
				"advertiser_ids": {
					Title:        "Advertiser IDs",
					Description:  "Advertisers to read; each must be authorised for the access token.",
					PropertyType: typed(synclet.Array), Items: map[string]any{"type": "string", "pattern": "^[0-9]+$"},
					Examples: []string{"1234567890123456789"}, Order: order(1),
				},
				"start_date": {
					Title:        "Start Date",
					Description:  "First day, YYYY-MM-DD, whose reports are read.",
					PropertyType: typed(synclet.String), Format: synclet.Date, Examples: []string{"2026-01-01"}, Order: order(2),
				},
				"end_date": {
					Title:        "End Date",
					Description:  "Last day, YYYY-MM-DD, whose reports are read; empty reads up to today (UTC).",
					PropertyType: typed(synclet.String), Format: synclet.Date, Order: order(3),
				},
				"lookback_days": {
					Title:        "Lookback Days",
					Description:  "Days, 0 to 30, before the saved stat_time_day an incremental report read starts again, since TikTok still updates recent days.",
					PropertyType: typed(synclet.Integer), Default: defaultLookbackDays, Order: order(4),
				},
				"environment": {
					Title:        "Environment",
					Description:  "production (business-api.tiktok.com) or sandbox (sandbox-ads.tiktok.com).",
					PropertyType: typed(synclet.String), Default: envProduction, Examples: []string{envProduction, envSandbox}, Order: order(5),
				},
			}},
		},
	}, nil
}
