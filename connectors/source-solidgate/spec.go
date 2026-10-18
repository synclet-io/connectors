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
		DocumentationURL:    "https://github.com/synclet-io/connectors/tree/main/connectors/source-solidgate",
		SupportsIncremental: true,
		ProtocolVersion:     "0.5.2",
		SupportedDestinationSyncModes: []synclet.DestinationSyncMode{
			synclet.DestinationSyncModeOverwrite,
			synclet.DestinationSyncModeAppend,
			synclet.DestinationSyncModeAppendDedup,
		},
		ConnectionSpecification: synclet.ConnectionSpecification{
			Title:       "Solidgate Source Spec",
			Description: "Reads orders, subscriptions, chargebacks, PayPal disputes, fraud alerts, products and financial entries from one or more Solidgate merchant accounts.",
			Type:        "object",
			Required:    []synclet.PropertyName{"accounts"},
			Properties: synclet.Properties{Properties: map[synclet.PropertyName]synclet.PropertySpec{
				"accounts": {
					Title:        "Merchant Accounts",
					Description:  "Solidgate merchant accounts to read. Each record carries the name of its account in solid_account.",
					PropertyType: typed(synclet.Array),
					Items: map[string]any{
						"type":     "object",
						"title":    "Merchant Account",
						"required": []string{"name", "public_key", "secret_key"},
						"properties": map[string]any{
							"name": map[string]any{
								"type": "string", "title": "Name", "order": 0,
								"description": "Name of the account, unique within this source; written to solid_account.",
							},
							"public_key": map[string]any{
								"type": "string", "title": "Public Key", "order": 1,
								"description": "API public key of the merchant account (api_pk_...). It is sent in the merchant header.",
							},
							"secret_key": map[string]any{
								"type": "string", "title": "Secret Key", "order": 2, "airbyte_secret": true,
								"description": "API secret key of the merchant account (api_sk_...). It signs each request and is never sent.",
							},
						},
					},
					Order: order(0),
				},
				"start_date": {
					Title: "Start Date", Description: "First UTC day, YYYY-MM-DD, to read. Empty reads the last two years.",
					PropertyType: typed(synclet.String), Format: synclet.Date, Examples: []string{"2026-01-01"}, Order: order(1),
				},
				"lookback_days": {
					Title: "Lookback Days", Description: "Days before the saved cursor an incremental read requests again, to pick up records that changed late. Default 2.",
					PropertyType: typed(synclet.Integer), Default: defaultLookbackDays, Order: order(2),
				},
			}},
		},
	}, nil
}
