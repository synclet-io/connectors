package main

import (
	"context"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

func order(n int) *int { return &n }

func typed(t synclet.PropType) synclet.PropertyType {
	return synclet.PropertyType{Type: synclet.PropTypes{t}}
}

var customerIDItems = map[string]any{"type": "string", "pattern": "^[0-9]{3}-?[0-9]{3}-?[0-9]{4}$"}

// Spec returns the connector's capabilities and configuration schema.
func (Source) Spec(_ context.Context) (*synclet.ConnectorSpecification, error) {
	return &synclet.ConnectorSpecification{
		DocumentationURL:    "https://github.com/synclet-io/connectors/tree/main/connectors/source-google-ads",
		SupportsIncremental: true,
		ProtocolVersion:     "0.5.2",
		SupportedDestinationSyncModes: []synclet.DestinationSyncMode{
			synclet.DestinationSyncModeOverwrite,
			synclet.DestinationSyncModeAppend,
			synclet.DestinationSyncModeAppendDedup,
		},
		ConnectionSpecification: synclet.ConnectionSpecification{
			Title:       "Google Ads Source Spec",
			Description: "Reads Google Ads accounts, campaigns, ad groups, ads, daily performance and custom GAQL queries from the Google Ads API.",
			Type:        "object",
			Required: []synclet.PropertyName{
				"developer_token", "client_id", "client_secret", "refresh_token", "customer_ids", "start_date",
			},
			Properties: synclet.Properties{Properties: map[synclet.PropertyName]synclet.PropertySpec{
				"developer_token": {
					Title:        "Developer Token",
					Description:  "Google Ads API developer token of your manager account, with Basic or Standard access to read production accounts. Sent in the developer-token header.",
					PropertyType: typed(synclet.String), IsSecret: true, Order: order(0),
				},
				"client_id": {
					Title:        "OAuth Client ID",
					Description:  "Client ID of the Google Cloud OAuth client the refresh token was issued to.",
					PropertyType: typed(synclet.String), Order: order(1),
				},
				"client_secret": {
					Title:        "OAuth Client Secret",
					Description:  "Client secret of the OAuth client.",
					PropertyType: typed(synclet.String), IsSecret: true, Order: order(2),
				},
				"refresh_token": {
					Title:        "Refresh Token",
					Description:  "OAuth refresh token with the https://www.googleapis.com/auth/adwords scope.",
					PropertyType: typed(synclet.String), IsSecret: true, Order: order(3),
				},
				"customer_ids": {
					Title:        "Customer IDs",
					Description:  "Accounts to read, with or without dashes. A manager account reads its enabled client accounts.",
					PropertyType: typed(synclet.Array), Items: customerIDItems, Examples: []string{"123-456-7890"}, Order: order(4),
				},
				"start_date": {
					Title:        "Start Date",
					Description:  "First day, YYYY-MM-DD, whose performance is read. Google Ads keeps 37 months; earlier days are not requested.",
					PropertyType: typed(synclet.String), Format: synclet.Date, Examples: []string{"2026-01-01"}, Order: order(5),
				},
				"end_date": {
					Title:        "End Date",
					Description:  "Last day, YYYY-MM-DD, whose performance is read; empty reads up to today (UTC).",
					PropertyType: typed(synclet.String), Format: synclet.Date, Order: order(6),
				},
				"lookback_days": {
					Title:        "Lookback Days",
					Description:  "Days before the saved date an incremental read starts again, since conversions are still attributed to past days.",
					PropertyType: typed(synclet.Integer), Default: defaultLookbackDays, Order: order(7),
				},
				"login_customer_id": {
					Title:        "Login Customer ID",
					Description:  "Manager account the credentials reach the customers through, sent as the login-customer-id header. Empty uses the manager in customer_ids for its clients.",
					PropertyType: typed(synclet.String), Examples: []string{"111-111-1111"}, Order: order(8),
				},
				"child_customer_ids": {
					Title:        "Child Customer IDs",
					Description:  "Client accounts of the managers in customer_ids to read; empty reads all enabled ones.",
					PropertyType: typed(synclet.Array), Items: customerIDItems, Order: order(9),
				},
				"custom_queries": {
					Title:        "Custom GAQL Queries",
					Description:  "Each query becomes a stream with its name. A query that selects segments.date is incremental by day. Select customer.id to tell accounts apart.",
					PropertyType: typed(synclet.Array),
					Items: map[string]any{
						"type":     "object",
						"required": []string{"name", "query"},
						"properties": map[string]any{
							"name":  map[string]any{"type": "string", "pattern": customQueryNamePatternLiteral},
							"query": map[string]any{"type": "string"},
						},
					},
					Order: order(10),
				},
			}},
		},
	}, nil
}
