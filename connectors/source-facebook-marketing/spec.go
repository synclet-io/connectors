package main

import (
	"context"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

func order(n int) *int { return &n }

func typed(t synclet.PropType) synclet.PropertyType {
	return synclet.PropertyType{Type: synclet.PropTypes{t}}
}

// Spec returns the connector's capabilities and configuration schema.
func (Source) Spec(_ context.Context) (*synclet.ConnectorSpecification, error) {
	return &synclet.ConnectorSpecification{
		DocumentationURL:    "https://github.com/synclet-io/connectors/tree/main/connectors/source-facebook-marketing",
		SupportsIncremental: true,
		ProtocolVersion:     "0.5.2",
		SupportedDestinationSyncModes: []synclet.DestinationSyncMode{
			synclet.DestinationSyncModeOverwrite,
			synclet.DestinationSyncModeAppend,
			synclet.DestinationSyncModeAppendDedup,
		},
		ConnectionSpecification: synclet.ConnectionSpecification{
			Title:       "Facebook Marketing Source Spec",
			Description: "Reads Meta ad accounts, campaigns, ad sets, ads, creatives, business extended credits and daily ad insights from the Facebook Marketing API.",
			Type:        "object",
			Required:    []synclet.PropertyName{"access_token", "account_ids", "start_date"},
			Properties: synclet.Properties{Properties: map[synclet.PropertyName]synclet.PropertySpec{
				"access_token": {
					Title:        "Access Token",
					Description:  "Long-lived user or system-user access token with the ads_read permission. It is sent in the Authorization header.",
					PropertyType: typed(synclet.String), IsSecret: true, Order: order(0),
				},
				"account_ids": {
					Title:        "Ad Account IDs",
					Description:  "Ad accounts to read, with or without the act_ prefix.",
					PropertyType: typed(synclet.Array), Items: map[string]any{"type": "string"}, Examples: []string{"act_1234567890"}, Order: order(1),
				},
				"start_date": {
					Title:        "Start Date",
					Description:  "First day, YYYY-MM-DD, whose insights are read.",
					PropertyType: typed(synclet.String), Format: synclet.Date, Examples: []string{"2026-01-01"}, Order: order(2),
				},
				"end_date": {
					Title:        "End Date",
					Description:  "Last day, YYYY-MM-DD, whose insights are read; empty reads up to today.",
					PropertyType: typed(synclet.String), Format: synclet.Date, Order: order(3),
				},
				"lookback_days": {
					Title:        "Lookback Days",
					Description:  "Days before the saved date_start an incremental insights read starts again, since Meta still changes insights for about 28 days.",
					PropertyType: typed(synclet.Integer), Default: defaultLookbackDays, Order: order(4),
				},
				"business_ids": {
					Title:        "Business IDs",
					Description:  "Businesses whose extended credits the business_extended_credits stream reads.",
					PropertyType: typed(synclet.Array), Items: map[string]any{"type": "string"}, Order: order(5),
				},
				"graph_api_base_url": {
					Title:        "Graph API Base URL",
					Description:  "Replaces https://graph.facebook.com, for example to send requests through a proxy; the API version path is appended.",
					PropertyType: typed(synclet.String), Order: order(6),
				},
				"graph_api_headers": {
					Title:        "Extra Request Headers",
					Description:  "Headers sent with every Graph API request, for example for a proxy.",
					PropertyType: typed(synclet.Array),
					Items: map[string]any{
						"type":     "object",
						"required": []string{"name", "value"},
						"properties": map[string]any{
							"name":  map[string]any{"type": "string"},
							"value": map[string]any{"type": "string", "airbyte_secret": true},
						},
					},
					Order: order(7),
				},
			}},
		},
	}, nil
}
