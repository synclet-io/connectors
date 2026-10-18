package main

import (
	"context"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

func order(n int) *int { return &n }

func credentialsSpec() synclet.PropertySpec {
	return synclet.PropertySpec{
		Title:        "Authentication",
		Description:  "A service account key, or an OAuth client with the refresh token of a Google user. Either needs the Viewer role on every property.",
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
						Description:  "The JSON key of a service account of a Google Cloud project with the Google Analytics Data API enabled.",
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
						Description:  "OAuth refresh token with the https://www.googleapis.com/auth/analytics.readonly scope.",
						PropertyType: typed(synclet.String), IsSecret: true, Order: order(3),
					},
				},
			},
		},
	}
}

func fieldList(title string, maxItems int) map[string]any {
	return map[string]any{"type": "array", "title": title, "items": map[string]any{"type": "string"}, "maxItems": maxItems}
}

// Spec returns the connector's capabilities and configuration schema.
func (Source) Spec(_ context.Context) (*synclet.ConnectorSpecification, error) {
	return &synclet.ConnectorSpecification{
		DocumentationURL:    "https://github.com/synclet-io/connectors/tree/main/connectors/source-google-analytics-data-api",
		SupportsIncremental: true,
		ProtocolVersion:     "0.5.2",
		SupportedDestinationSyncModes: []synclet.DestinationSyncMode{
			synclet.DestinationSyncModeOverwrite,
			synclet.DestinationSyncModeAppend,
			synclet.DestinationSyncModeAppendDedup,
		},
		ConnectionSpecification: synclet.ConnectionSpecification{
			Title:       "Google Analytics 4 Source Spec",
			Description: "Reads daily Google Analytics 4 reports, built-in and custom, through the Google Analytics Data API.",
			Type:        "object",
			Required:    []synclet.PropertyName{"credentials", "property_ids", "start_date"},
			Properties: synclet.Properties{Properties: map[synclet.PropertyName]synclet.PropertySpec{
				"credentials": credentialsSpec(),
				"property_ids": {
					Title:        "Property IDs",
					Description:  "Google Analytics 4 property ids to read, with or without the properties/ prefix. Every report reads each of them.",
					PropertyType: typed(synclet.Array),
					Items:        map[string]any{"type": "string", "pattern": "^(properties/)?[0-9]+$"},
					Examples:     []string{"123456789"},
					Order:        order(1),
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
					Description:  "Days before the saved date an incremental read starts again, since Google Analytics keeps processing recent days.",
					PropertyType: typed(synclet.Integer), Default: defaultLookbackDays, Order: order(4),
				},
				"custom_reports": {
					Title:        "Custom Reports",
					Description:  "Each report becomes a stream with its name, read by day. date is added as the first dimension when missing; at most 9 dimensions with date and 1 to 10 metrics.",
					PropertyType: typed(synclet.Array),
					Items: map[string]any{
						"type":     "object",
						"required": []string{"name", "metrics"},
						"properties": map[string]any{
							"name":       map[string]any{"type": "string", "pattern": reportNamePatternLiteral},
							"dimensions": fieldList("Dimensions", maxDimensions),
							"metrics":    fieldList("Metrics", maxMetrics),
						},
					},
					Order: order(5),
				},
				"data_quality_fields": {
					Title:        "Data Quality Fields",
					Description:  "Add sampled, sampling_ratio, subject_to_thresholding and data_loss_from_other_row to every record.",
					PropertyType: typed(propBoolean), Default: true, Order: order(6),
				},
			}},
		},
	}, nil
}
