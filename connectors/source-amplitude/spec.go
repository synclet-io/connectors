package main

import (
	"context"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

const propBoolean synclet.PropType = "boolean"

func order(n int) *int { return &n }

func typed(t ...synclet.PropType) synclet.PropertyType {
	return synclet.PropertyType{Type: synclet.PropTypes(t)}
}

// Spec returns the connector's capabilities and configuration schema.
func (Source) Spec(_ context.Context) (*synclet.ConnectorSpecification, error) {
	return &synclet.ConnectorSpecification{
		DocumentationURL:    "https://github.com/synclet-io/connectors/tree/main/connectors/source-amplitude",
		SupportsIncremental: true,
		ProtocolVersion:     "0.5.2",
		SupportedDestinationSyncModes: []synclet.DestinationSyncMode{
			synclet.DestinationSyncModeOverwrite,
			synclet.DestinationSyncModeAppend,
			synclet.DestinationSyncModeAppendDedup,
		},
		ConnectionSpecification: synclet.ConnectionSpecification{
			Title:       "Amplitude Source Spec",
			Description: "Reads raw events through the Amplitude Export API and active users, average session length, cohorts, annotations and the event list through the Dashboard REST API.",
			Type:        "object",
			Required:    []synclet.PropertyName{"api_key", "secret_key", "start_date"},
			Properties: synclet.Properties{Properties: map[synclet.PropertyName]synclet.PropertySpec{
				"data_region": {
					Title:        "Data Region",
					Description:  `Region the Amplitude project stores its data in: "Standard Server" (amplitude.com) or "EU Residency Server" (analytics.eu.amplitude.com).`,
					PropertyType: typed(synclet.String), Default: regionStandard,
					Examples: []string{regionStandard, regionEU}, Order: order(0),
				},
				"api_key": {
					Title:        "API Key",
					Description:  "API key of the Amplitude project, in Settings > Projects.",
					PropertyType: typed(synclet.String), IsSecret: true, Order: order(1),
				},
				"secret_key": {
					Title:        "Secret Key",
					Description:  "Secret key of the Amplitude project, in Settings > Projects.",
					PropertyType: typed(synclet.String), IsSecret: true, Order: order(2),
				},
				"start_date": {
					Title:        "Start Date",
					Description:  "UTC time, YYYY-MM-DDTHH:mm:ssZ, from which data is read. Events are read from the start of its hour, the daily metrics from its day.",
					PropertyType: typed(synclet.String), Format: synclet.DateTime,
					Examples: []string{"2026-01-01T00:00:00Z"}, Order: order(3),
				},
				"request_time_range": {
					Title:        "Request Time Range",
					Description:  "Hours of events one Export API request covers, 1 to 8760. A window the API rejects as too large is split in half until it is accepted. Default 24.",
					PropertyType: typed(synclet.Integer), Default: defaultRequestTimeRange, Order: order(4),
				},
				"active_users_group_by_country": {
					Title:        "Active Users Group by Country",
					Description:  "Break the active_users counts down by country. Default true.",
					PropertyType: typed(propBoolean), Default: true, Order: order(5),
				},
			}},
		},
	}, nil
}
