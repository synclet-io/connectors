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
		DocumentationURL: "https://github.com/synclet-io/connectors/tree/main/connectors/source-revenuecat",
		ProtocolVersion:  "0.5.2",
		SupportedDestinationSyncModes: []synclet.DestinationSyncMode{
			synclet.DestinationSyncModeOverwrite,
			synclet.DestinationSyncModeAppend,
			synclet.DestinationSyncModeAppendDedup,
		},
		ConnectionSpecification: synclet.ConnectionSpecification{
			Title:       "RevenueCat Source Spec",
			Description: "Reads project configuration, customers with their subscriptions, purchases and active entitlements, and overview metrics from the RevenueCat REST API v2 with a v2 secret API key.",
			Type:        "object",
			Required:    []synclet.PropertyName{"api_key"},
			Properties: synclet.Properties{Properties: map[synclet.PropertyName]synclet.PropertySpec{
				"api_key": {
					Title:        "Secret API Key (v2)",
					Description:  "A RevenueCat secret API key of version V2 (Project settings > API keys). It is sent in the Authorization header. API v1 keys do not work with REST API v2. See the README for the read permissions each stream needs.",
					PropertyType: typed(synclet.String), IsSecret: true, Order: order(0),
				},
				"project_id": {
					Title:        "Project ID",
					Description:  "ID of the RevenueCat project to read. Leave empty when the key can see exactly one project.",
					PropertyType: typed(synclet.String), Examples: []string{"proj1ab2c3d4"}, Order: order(1),
				},
				"environment": {
					Title:        "Environment",
					Description:  "Store environment of the customer subscriptions and purchases to read: sandbox or production. Leave empty to read both.",
					PropertyType: typed(synclet.String), Examples: []string{"production", "sandbox"}, Order: order(2),
				},
			}},
		},
	}, nil
}
