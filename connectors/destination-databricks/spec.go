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
func (Destination) Spec(context.Context) (*synclet.ConnectorSpecification, error) {
	return &synclet.ConnectorSpecification{
		DocumentationURL: "https://github.com/synclet-io/connectors/tree/main/connectors/destination-databricks",
		ProtocolVersion:  "0.5.2",
		SupportedDestinationSyncModes: []synclet.DestinationSyncMode{
			synclet.DestinationSyncModeOverwrite,
			synclet.DestinationSyncModeAppend,
			synclet.DestinationSyncModeAppendDedup,
		},
		ConnectionSpecification: synclet.ConnectionSpecification{
			Title:       "Databricks Destination Spec",
			Description: "Writes each stream into a Delta table of one Unity Catalog schema, staging batches in a Unity Catalog volume: overwrite, append or append_dedup.",
			Type:        "object",
			Required:    []synclet.PropertyName{"hostname", "http_path", "database", "schema", "volume", "authentication"},
			Properties: synclet.Properties{Properties: map[synclet.PropertyName]synclet.PropertySpec{
				"hostname": {
					Title: "Server Hostname", Description: "Workspace host name, from the SQL warehouse's connection details.",
					PropertyType: typed(synclet.String), Examples: []string{"dbc-a1b2c3d4-e5f6.cloud.databricks.com"}, Order: order(0),
				},
				"http_path": {
					Title: "HTTP Path", Description: "HTTP path of the SQL warehouse, from its connection details.",
					PropertyType: typed(synclet.String), Examples: []string{"/sql/1.0/warehouses/0123456789abcdef"}, Order: order(1),
				},
				"port": {
					Title: "Port", Description: "HTTPS port of the workspace.",
					PropertyType: typed(synclet.Integer), Default: 443, Examples: []string{"443"}, Order: order(2),
				},
				"database": {
					Title: "Catalog", Description: "Unity Catalog catalog the schema belongs to.",
					PropertyType: typed(synclet.String), Order: order(3),
				},
				"schema": {
					Title: "Schema", Description: "Schema every table is written to; it must exist.",
					PropertyType: typed(synclet.String), Default: "default", Examples: []string{"default"}, Order: order(4),
				},
				"volume": {
					Title: "Staging Volume", Description: "Unity Catalog volume in the schema where batches are staged before COPY INTO; it must exist.",
					PropertyType: typed(synclet.String), Examples: []string{"synclet_staging"}, Order: order(5),
				},
				"authentication": {
					Title: "Authentication", Description: "Personal access token or OAuth machine-to-machine service principal.",
					PropertyType: typed(synclet.Object), Order: order(6),
					OneOf: []synclet.PropertySpec{
						{
							Title: "Personal Access Token", PropertyType: typed(synclet.Object),
							Required: []synclet.PropertyName{"auth_type", "personal_access_token"},
							Properties: map[synclet.PropertyName]synclet.PropertySpec{
								"auth_type": {PropertyType: typed(synclet.String), Const: authPAT, Order: order(0)},
								"personal_access_token": {
									Title: "Access Token", Description: "Personal access token of a user or service principal.",
									PropertyType: typed(synclet.String), IsSecret: true, Order: order(1),
								},
							},
						},
						{
							Title: "OAuth (service principal)", PropertyType: typed(synclet.Object),
							Required: []synclet.PropertyName{"auth_type", "client_id", "secret"},
							Properties: map[synclet.PropertyName]synclet.PropertySpec{
								"auth_type": {PropertyType: typed(synclet.String), Const: authOAuth, Order: order(0)},
								"client_id": {
									Title: "Client ID", Description: "Application ID of the service principal.",
									PropertyType: typed(synclet.String), Order: order(1),
								},
								"secret": {
									Title: "OAuth Secret", Description: "OAuth secret of the service principal.",
									PropertyType: typed(synclet.String), IsSecret: true, Order: order(2),
								},
							},
						},
					},
				},
			}},
		},
	}, nil
}
