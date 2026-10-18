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
		DocumentationURL:    "https://github.com/synclet-io/connectors/tree/main/connectors/source-hubspot",
		SupportsIncremental: true,
		ProtocolVersion:     "0.5.2",
		SupportedDestinationSyncModes: []synclet.DestinationSyncMode{
			synclet.DestinationSyncModeOverwrite,
			synclet.DestinationSyncModeAppend,
			synclet.DestinationSyncModeAppendDedup,
		},
		ConnectionSpecification: synclet.ConnectionSpecification{
			Title:       "HubSpot Source Spec",
			Description: "Reads HubSpot CRM objects, custom objects, owners, pipelines and associations with a private app access token.",
			Type:        "object",
			Required:    []synclet.PropertyName{"access_token"},
			Properties: synclet.Properties{Properties: map[synclet.PropertyName]synclet.PropertySpec{
				"access_token": {
					Title: "Private App Access Token", Description: "Access token of a HubSpot private app (Settings > Integrations > Private Apps) with the read scopes of the streams to sync. It is sent in the Authorization header.",
					PropertyType: typed(synclet.String), IsSecret: true, Order: order(0),
				},
				"start_date": {
					Title: "Start Date", Description: "Earliest last modification the first incremental sync of CRM objects reads: an RFC 3339 time or YYYY-MM-DD, in UTC. Empty reads everything.",
					PropertyType: typed(synclet.String), Examples: []string{"2026-01-01", "2026-01-01T00:00:00Z"}, Order: order(1),
				},
			}},
		},
	}, nil
}
