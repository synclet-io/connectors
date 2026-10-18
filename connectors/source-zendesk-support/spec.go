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
		DocumentationURL:    "https://github.com/synclet-io/connectors/tree/main/connectors/source-zendesk-support",
		SupportsIncremental: true,
		ProtocolVersion:     "0.5.2",
		SupportedDestinationSyncModes: []synclet.DestinationSyncMode{
			synclet.DestinationSyncModeOverwrite,
			synclet.DestinationSyncModeAppend,
			synclet.DestinationSyncModeAppendDedup,
		},
		ConnectionSpecification: synclet.ConnectionSpecification{
			Title:       "Zendesk Support Source Spec",
			Description: "Reads tickets, users, ticket events, organizations, groups, ticket fields and satisfaction ratings from Zendesk Support with an API token.",
			Type:        "object",
			Required:    []synclet.PropertyName{"subdomain", "email", "api_token", "start_date"},
			Properties: synclet.Properties{Properties: map[synclet.PropertyName]synclet.PropertySpec{
				"subdomain": {
					Title: "Subdomain", Description: "Account name of your Zendesk, such as acme for https://acme.zendesk.com.",
					PropertyType: typed(synclet.String), Examples: []string{"acme"}, Order: order(0),
				},
				"email": {
					Title: "Email", Description: "Email of the admin the API token belongs to.",
					PropertyType: typed(synclet.String), Examples: []string{"admin@example.com"}, Order: order(1),
				},
				"api_token": {
					Title: "API Token", Description: "Zendesk API token (Admin Center > Apps and integrations > Zendesk API). It is sent in the Authorization header.",
					PropertyType: typed(synclet.String), IsSecret: true, Order: order(2),
				},
				"start_date": {
					Title: "Start Date", Description: "Earliest update the first sync of tickets, users and ticket events reads: an RFC 3339 time or YYYY-MM-DD, in UTC.",
					PropertyType: typed(synclet.String), Examples: []string{"2026-01-01", "2026-01-01T00:00:00Z"}, Order: order(3),
				},
			}},
		},
	}, nil
}
