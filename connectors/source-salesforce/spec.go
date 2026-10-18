package main

import (
	"context"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

// boolean is the JSON schema type the SDK has no constant for.
const boolean synclet.PropType = "boolean"

func order(n int) *int { return &n }

func typed(t synclet.PropType) synclet.PropertyType {
	return synclet.PropertyType{Type: synclet.PropTypes{t}}
}

// Spec returns the connector's capabilities and configuration schema.
func (Source) Spec(_ context.Context) (*synclet.ConnectorSpecification, error) {
	patterns := map[string]any{"type": "string"}
	return &synclet.ConnectorSpecification{
		DocumentationURL:    "https://github.com/synclet-io/connectors/tree/main/connectors/source-salesforce",
		SupportsIncremental: true,
		ProtocolVersion:     "0.5.2",
		SupportedDestinationSyncModes: []synclet.DestinationSyncMode{
			synclet.DestinationSyncModeOverwrite,
			synclet.DestinationSyncModeAppend,
			synclet.DestinationSyncModeAppendDedup,
		},
		ConnectionSpecification: synclet.ConnectionSpecification{
			Title:       "Salesforce Source Spec",
			Description: "Reads queryable Salesforce objects through SOQL with a connected app's OAuth refresh token.",
			Type:        "object",
			Required:    []synclet.PropertyName{"client_id", "client_secret", "refresh_token", "instance_url"},
			Properties: synclet.Properties{Properties: map[synclet.PropertyName]synclet.PropertySpec{
				"client_id": {
					Title: "Client ID", Description: "Consumer key of the connected app.",
					PropertyType: typed(synclet.String), Order: order(0),
				},
				"client_secret": {
					Title: "Client Secret", Description: "Consumer secret of the connected app.",
					PropertyType: typed(synclet.String), IsSecret: true, Order: order(1),
				},
				"refresh_token": {
					Title: "Refresh Token", Description: "OAuth refresh token of the connected app for the integration user. Use a connected app whose refresh token policy is \"valid until revoked\", so the token does not rotate.",
					PropertyType: typed(synclet.String), IsSecret: true, Order: order(2),
				},
				"instance_url": {
					Title: "Instance URL", Description: "https URL of the org, as the token response's instance_url gives it.",
					PropertyType: typed(synclet.String), Examples: []string{"https://acme.my.salesforce.com"}, Order: order(3),
				},
				"is_sandbox": {
					Title: "Sandbox", Description: "The org is a sandbox: tokens come from test.salesforce.com instead of login.salesforce.com.",
					PropertyType: typed(boolean), Default: false, Order: order(4),
				},
				"start_date": {
					Title: "Start Date", Description: "Earliest SystemModstamp (or LastModifiedDate) read: an RFC 3339 time or YYYY-MM-DD, in UTC. Empty reads every record.",
					PropertyType: typed(synclet.String), Examples: []string{"2026-01-01", "2026-01-01T00:00:00Z"}, Order: order(5),
				},
				"streams_include": {
					Title: "Objects to include", Description: "Object name patterns (path.Match syntax, such as Account or *__c); empty includes every queryable object.",
					PropertyType: typed(synclet.Array), Items: patterns, Order: order(6),
				},
				"streams_exclude": {
					Title: "Objects to exclude", Description: "Object name patterns (path.Match syntax) left out after streams_include.",
					PropertyType: typed(synclet.Array), Items: patterns, Order: order(7),
				},
				"api_usage_threshold_percent": {
					Title: "API Usage Threshold", Description: "Share of the org's daily API request allocation, 1 to 100 percent, at which a read saves its state and stops.",
					PropertyType: typed(synclet.Integer), Default: defaultThreshold, Order: order(8),
				},
			}},
		},
	}, nil
}
