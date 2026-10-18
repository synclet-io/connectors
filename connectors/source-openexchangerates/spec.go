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
		DocumentationURL:    "https://github.com/synclet-io/connectors/tree/main/connectors/source-openexchangerates",
		SupportsIncremental: true,
		ProtocolVersion:     "0.5.2",
		SupportedDestinationSyncModes: []synclet.DestinationSyncMode{
			synclet.DestinationSyncModeOverwrite,
			synclet.DestinationSyncModeAppend,
			synclet.DestinationSyncModeAppendDedup,
		},
		ConnectionSpecification: synclet.ConnectionSpecification{
			Title:       "Open Exchange Rates Source Spec",
			Description: "Reads currencies, the latest exchange rates and daily historical rates from Open Exchange Rates.",
			Type:        "object",
			Required:    []synclet.PropertyName{"app_id", "start_date"},
			Properties: synclet.Properties{Properties: map[synclet.PropertyName]synclet.PropertySpec{
				"app_id": {
					Title: "App ID", Description: "App ID of your Open Exchange Rates account. It is sent in the Authorization header.",
					PropertyType: typed(synclet.String), IsSecret: true, Order: order(0),
				},
				"start_date": {
					Title: "Start Date", Description: "First day, YYYY-MM-DD in UTC, whose historical rates are read.",
					PropertyType: typed(synclet.String), Format: synclet.Date, Examples: []string{"2026-01-01"}, Order: order(1),
				},
				"base": {
					Title: "Base Currency", Description: "Currency the rates are quoted against. Changing it needs a paid plan; empty uses USD.",
					PropertyType: typed(synclet.String), Examples: []string{"EUR"}, Order: order(2),
				},
				"symbols": {
					Title: "Symbols", Description: "Currencies to read rates for; empty reads every currency. Needs a paid plan.",
					PropertyType: typed(synclet.Array), Items: map[string]any{"type": "string"}, Order: order(3),
				},
			}},
		},
	}, nil
}
