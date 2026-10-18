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
		DocumentationURL:    "https://github.com/synclet-io/connectors/tree/main/connectors/source-shopify",
		SupportsIncremental: true,
		ProtocolVersion:     "0.5.2",
		SupportedDestinationSyncModes: []synclet.DestinationSyncMode{
			synclet.DestinationSyncModeOverwrite,
			synclet.DestinationSyncModeAppend,
			synclet.DestinationSyncModeAppendDedup,
		},
		ConnectionSpecification: synclet.ConnectionSpecification{
			Title:       "Shopify Source Spec",
			Description: "Reads a Shopify store through the Admin GraphQL API with the access token of a custom app, or the client credentials of a Dev Dashboard app installed on the store.",
			Type:        "object",
			Required:    []synclet.PropertyName{"shop"},
			Properties: synclet.Properties{Properties: map[synclet.PropertyName]synclet.PropertySpec{
				"shop": {
					Title: "Shop", Description: "The store name or its myshopify.com domain. Custom domains are not accepted.",
					PropertyType: typed(synclet.String), Examples: []string{"example", "example.myshopify.com"}, Order: order(0),
				},
				"access_token": {
					Title: "Admin API Access Token", Description: "Admin API access token of a custom app installed on the store, sent in the X-Shopify-Access-Token header. Leave empty when using client_id and client_secret.",
					PropertyType: typed(synclet.String), IsSecret: true, Order: order(1),
				},
				"client_id": {
					Title: "Client ID", Description: "Client ID of a Dev Dashboard app installed on the store. The connector exchanges it with client_secret for an access token (client credentials grant). Leave empty when using access_token.",
					PropertyType: typed(synclet.String), Order: order(2),
				},
				"client_secret": {
					Title: "Client Secret", Description: "Client secret of the Dev Dashboard app of client_id.",
					PropertyType: typed(synclet.String), IsSecret: true, Order: order(3),
				},
				"start_date": {
					Title: "Start Date", Description: "Earliest update time read: an RFC 3339 time or YYYY-MM-DD, in UTC. Empty reads from the shop's creation.",
					PropertyType: typed(synclet.String), Examples: []string{"2026-01-01", "2026-01-01T00:00:00Z"}, Order: order(4),
				},
				"window_days": {
					Title: "Window Days", Description: "Days of updates one query of an incremental sync reads; the sync saves its state after each window. A full refresh reads in one run. 1 to 365, default 30.",
					PropertyType: typed(synclet.Integer), Default: defaultWindowDays, Order: order(5),
				},
			}},
		},
	}, nil
}
