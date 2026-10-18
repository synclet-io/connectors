package main

import (
	"context"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

func order(n int) *int { return &n }

func credentialsSpec() synclet.PropertySpec {
	return synclet.PropertySpec{
		Title:        "Authentication",
		Description:  "A service account key, or an OAuth client with the refresh token of a Google user. Either must be a user of every site in Search Console.",
		PropertyType: typed(synclet.Object),
		Order:        order(0),
		OneOf: []synclet.PropertySpec{
			{
				Title:        "Service Account Key",
				PropertyType: typed(synclet.Object),
				Required:     []synclet.PropertyName{"auth_type", "service_account_key"},
				Properties: map[synclet.PropertyName]synclet.PropertySpec{
					"auth_type": {PropertyType: typed(synclet.String), Const: authServiceAccount, Order: order(0)},
					"service_account_key": {
						Title:        "Service Account Key",
						Description:  "The JSON key of a service account of a Google Cloud project with the Google Search Console API enabled.",
						PropertyType: typed(synclet.String),
						IsSecret:     true,
						Order:        order(1),
					},
				},
			},
			{
				Title:        "Google OAuth",
				PropertyType: typed(synclet.Object),
				Required:     []synclet.PropertyName{"auth_type", "client_id", "client_secret", "refresh_token"},
				Properties: map[synclet.PropertyName]synclet.PropertySpec{
					"auth_type": {PropertyType: typed(synclet.String), Const: authOAuth, Order: order(0)},
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
						Description:  "OAuth refresh token with the https://www.googleapis.com/auth/webmasters.readonly scope.",
						PropertyType: typed(synclet.String), IsSecret: true, Order: order(3),
					},
				},
			},
		},
	}
}

// Spec returns the connector's capabilities and configuration schema.
func (Source) Spec(_ context.Context) (*synclet.ConnectorSpecification, error) {
	return &synclet.ConnectorSpecification{
		DocumentationURL:    "https://github.com/synclet-io/connectors/tree/main/connectors/source-google-search-console",
		SupportsIncremental: true,
		ProtocolVersion:     "0.5.2",
		SupportedDestinationSyncModes: []synclet.DestinationSyncMode{
			synclet.DestinationSyncModeOverwrite,
			synclet.DestinationSyncModeAppend,
			synclet.DestinationSyncModeAppendDedup,
		},
		ConnectionSpecification: synclet.ConnectionSpecification{
			Title:       "Google Search Console Source Spec",
			Description: "Reads the sites, sitemaps and daily search analytics, built-in and custom, of Google Search Console properties.",
			Type:        "object",
			Required:    []synclet.PropertyName{"credentials", "site_urls"},
			Properties: synclet.Properties{Properties: map[synclet.PropertyName]synclet.PropertySpec{
				"credentials": credentialsSpec(),
				"site_urls": {
					Title:        "Site URLs",
					Description:  "Search Console properties to read: URL-prefix properties such as https://www.example.com/ and domain properties such as sc-domain:example.com. Every stream reads each of them.",
					PropertyType: typed(synclet.Array),
					Items:        map[string]any{"type": "string", "pattern": `^(https://[^/\s]+/\S*|sc-domain:[^/:\s]+)$`},
					Examples:     []string{"https://www.example.com/", "sc-domain:example.com"},
					Order:        order(1),
				},
				"start_date": {
					Title:        "Start Date",
					Description:  "First day, YYYY-MM-DD, whose search analytics are read; empty reads the last 16 months, all Search Console keeps.",
					PropertyType: typed(synclet.String), Format: synclet.Date, Examples: []string{"2026-01-01"}, Order: order(2),
				},
				"end_date": {
					Title:        "End Date",
					Description:  "Last day, YYYY-MM-DD, whose search analytics are read; empty reads up to today (UTC).",
					PropertyType: typed(synclet.String), Format: synclet.Date, Order: order(3),
				},
				"lookback_days": {
					Title:        "Lookback Days",
					Description:  "Days before the saved date an incremental read starts again, since Search Console keeps updating recent days.",
					PropertyType: typed(synclet.Integer), Default: defaultLookbackDays, Order: order(4),
				},
				"data_state": {
					Title:        "Data State",
					Description:  "final reads only finalized data; all also reads fresh data that may still change.",
					PropertyType: typed(synclet.String), Default: dataStateFinal, Examples: []string{dataStateFinal, dataStateAll}, Order: order(5),
				},
				"custom_reports": {
					Title:        "Custom Reports",
					Description:  "Each report becomes a stream with its name, read by day for every site. date is added as the first dimension when missing. discover and googleNews do not support the query dimension.",
					PropertyType: typed(synclet.Array),
					Items: map[string]any{
						"type":     "object",
						"required": []string{"name"},
						"properties": map[string]any{
							"name": map[string]any{"type": "string", "pattern": reportNamePatternLiteral},
							"dimensions": map[string]any{
								"type": "array", "title": "Dimensions", "uniqueItems": true,
								"items": map[string]any{"type": "string", "enum": dimensions},
							},
							"search_type": map[string]any{"type": "string", "title": "Search Type", "enum": searchTypes, "default": "web"},
						},
					},
					Order: order(6),
				},
			}},
		},
	}, nil
}
