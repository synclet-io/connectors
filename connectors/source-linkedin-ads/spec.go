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
		DocumentationURL:    "https://github.com/synclet-io/connectors/tree/main/connectors/source-linkedin-ads",
		SupportsIncremental: true,
		ProtocolVersion:     "0.5.2",
		SupportedDestinationSyncModes: []synclet.DestinationSyncMode{
			synclet.DestinationSyncModeOverwrite,
			synclet.DestinationSyncModeAppend,
			synclet.DestinationSyncModeAppendDedup,
		},
		ConnectionSpecification: synclet.ConnectionSpecification{
			Title:       "LinkedIn Ads Source Spec",
			Description: "Reads LinkedIn ad accounts, campaign groups, campaigns, creatives and daily ad analytics from the LinkedIn Marketing API.",
			Type:        "object",
			Required:    []synclet.PropertyName{"client_id", "client_secret", "refresh_token", "start_date"},
			Properties: synclet.Properties{Properties: map[synclet.PropertyName]synclet.PropertySpec{
				"client_id": {
					Title:        "Client ID",
					Description:  "Client ID of a LinkedIn app with the Advertising API product.",
					PropertyType: typed(synclet.String), IsSecret: true, Order: order(0),
				},
				"client_secret": {
					Title:        "Client Secret",
					Description:  "Client secret of the LinkedIn app.",
					PropertyType: typed(synclet.String), IsSecret: true, Order: order(1),
				},
				"refresh_token": {
					Title:        "Refresh Token",
					Description:  "OAuth refresh token the app was issued with the r_ads and r_ads_reporting scopes; it is valid for a year.",
					PropertyType: typed(synclet.String), IsSecret: true, Order: order(2),
				},
				"account_ids": {
					Title:        "Ad Account IDs",
					Description:  "Ad accounts to read; empty reads every account the authorizing user has a role on.",
					PropertyType: typed(synclet.Array), Items: map[string]any{"type": "string", "pattern": "^[0-9]+$"},
					Examples: []string{"500000001"}, Order: order(3),
				},
				"start_date": {
					Title:        "Start Date",
					Description:  "First day, YYYY-MM-DD, whose analytics are read.",
					PropertyType: typed(synclet.String), Format: synclet.Date, Examples: []string{"2026-01-01"}, Order: order(4),
				},
				"end_date": {
					Title:        "End Date",
					Description:  "Last day, YYYY-MM-DD, whose analytics are read; empty reads up to today (UTC).",
					PropertyType: typed(synclet.String), Format: synclet.Date, Order: order(5),
				},
				"lookback_days": {
					Title:        "Lookback Days",
					Description:  "Days, 0 to 30, before the saved date an incremental analytics read starts again, since LinkedIn still updates recent days.",
					PropertyType: typed(synclet.Integer), Default: defaultLookbackDays, Order: order(6),
				},
			}},
		},
	}, nil
}
