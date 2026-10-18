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
		DocumentationURL:    "https://github.com/synclet-io/connectors/tree/main/connectors/source-airtable",
		SupportsIncremental: true,
		ProtocolVersion:     "0.5.2",
		SupportedDestinationSyncModes: []synclet.DestinationSyncMode{
			synclet.DestinationSyncModeOverwrite,
			synclet.DestinationSyncModeAppend,
			synclet.DestinationSyncModeAppendDedup,
		},
		ConnectionSpecification: synclet.ConnectionSpecification{
			Title:       "Airtable Source Spec",
			Description: "Reads every table of the Airtable bases a personal access token can see, one stream per table.",
			Type:        "object",
			Required:    []synclet.PropertyName{"personal_access_token"},
			Properties: synclet.Properties{Properties: map[synclet.PropertyName]synclet.PropertySpec{
				"personal_access_token": {
					Title:        "Personal Access Token",
					Description:  "Airtable personal access token (https://airtable.com/create/tokens) with the scopes data.records:read and schema.bases:read and access to the bases to sync. It is sent in the Authorization header.",
					PropertyType: typed(synclet.String), IsSecret: true, Order: order(0),
				},
				"base_ids": {
					Title:        "Base IDs",
					Description:  "Bases to sync, by the id in the base URL (appXXXXXXXXXXXXXX). Empty syncs every base the token can see.",
					PropertyType: typed(synclet.Array),
					Items:        map[string]any{"type": "string", "pattern": `^app[A-Za-z0-9]{14}$`},
					Examples:     []string{"appAbCdEfGhIjKlMn"}, Order: order(1),
				},
			}},
		},
	}, nil
}
