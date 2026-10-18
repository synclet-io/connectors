package main

import (
	"context"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

func order(n int) *int { return &n }

func typed(t synclet.PropType) synclet.PropertyType {
	return synclet.PropertyType{Type: synclet.PropTypes{t}}
}

func credentialsSpec() synclet.PropertySpec {
	return synclet.PropertySpec{
		Title:        "Authentication",
		Description:  "Credentials for the BigQuery API. When omitted, Application Default Credentials are used.",
		PropertyType: typed(synclet.Object),
		Order:        order(3),
		OneOf: []synclet.PropertySpec{
			{
				Title:        "Service Account Key Authentication",
				PropertyType: typed(synclet.Object),
				Required:     []synclet.PropertyName{"auth_type", "service_account_info"},
				Properties: map[synclet.PropertyName]synclet.PropertySpec{
					"auth_type": {PropertyType: typed(synclet.String), Const: authService, Order: order(0)},
					"service_account_info": {
						Title:        "Service Account Information",
						Description:  "The JSON key of the service account to use for authorization.",
						PropertyType: typed(synclet.String),
						IsSecret:     true,
						Order:        order(1),
					},
				},
			},
			{
				Title:        "Application Default Credentials",
				Description:  "Use Google Cloud Application Default Credentials (no extra fields needed).",
				PropertyType: typed(synclet.Object),
				Required:     []synclet.PropertyName{"auth_type"},
				Properties: map[synclet.PropertyName]synclet.PropertySpec{
					"auth_type": {PropertyType: typed(synclet.String), Const: authApplicationDefault, Order: order(0)},
				},
			},
		},
	}
}

// Spec returns the connector's capabilities and configuration schema.
func (Source) Spec(_ context.Context) (*synclet.ConnectorSpecification, error) {
	return &synclet.ConnectorSpecification{
		DocumentationURL:    "https://github.com/synclet-io/connectors/tree/main/connectors/source-bigquery",
		SupportsIncremental: true,
		ProtocolVersion:     "0.5.2",
		SupportedDestinationSyncModes: []synclet.DestinationSyncMode{
			synclet.DestinationSyncModeOverwrite,
			synclet.DestinationSyncModeAppend,
			synclet.DestinationSyncModeAppendDedup,
		},
		ConnectionSpecification: synclet.ConnectionSpecification{
			Title:       "BigQuery Source Spec",
			Description: "Reads tables and views of BigQuery datasets: full refresh, or incremental by a cursor column.",
			Type:        "object",
			Required:    []synclet.PropertyName{"project_id"},
			Properties: synclet.Properties{Properties: map[synclet.PropertyName]synclet.PropertySpec{
				"project_id": {
					Title: "Project ID", Description: "The Google Cloud project that runs the queries and holds the datasets.",
					PropertyType: typed(synclet.String), Order: order(0),
				},
				"datasets": {
					Title: "Datasets", Description: "Datasets to sync from. When empty, every dataset of the project.",
					PropertyType: typed(synclet.Array), Items: map[string]any{"type": "string"}, Order: order(1),
				},
				"location": {
					Title: "Location", Description: "Location the query jobs run in, such as US or europe-west1. When empty, BigQuery picks it from the tables read.",
					PropertyType: typed(synclet.String), Examples: []string{"US", "EU", "europe-west1"}, Order: order(2),
				},
				"credentials": credentialsSpec(),
			}},
		},
	}, nil
}
