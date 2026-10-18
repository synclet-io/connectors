package main

import (
	"context"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

func order(n int) *int { return &n }

func typed(t ...synclet.PropType) synclet.PropertyType {
	return synclet.PropertyType{Type: synclet.PropTypes(t)}
}

// Spec returns the connector's capabilities and configuration schema.
func (Source) Spec(_ context.Context) (*synclet.ConnectorSpecification, error) {
	return &synclet.ConnectorSpecification{
		DocumentationURL:    "https://github.com/synclet-io/connectors/tree/main/connectors/source-applovin",
		SupportsIncremental: true,
		ProtocolVersion:     "0.5.2",
		SupportedDestinationSyncModes: []synclet.DestinationSyncMode{
			synclet.DestinationSyncModeOverwrite,
			synclet.DestinationSyncModeAppend,
			synclet.DestinationSyncModeAppendDedup,
		},
		ConnectionSpecification: synclet.ConnectionSpecification{
			Title:       "AppLovin Source Spec",
			Description: "Reads the AppLovin advertiser (Axon) report and the MAX revenue report day by day.",
			Type:        "object",
			Required:    []synclet.PropertyName{"api_key", "start_date"},
			Properties: synclet.Properties{Properties: map[synclet.PropertyName]synclet.PropertySpec{
				"api_key": {
					Title:        "Report Key",
					Description:  "Report Key of the AppLovin account, listed on the Keys page of the dashboard. AppLovin accepts it only as the api_key query parameter; the connector never logs request URLs and removes the key from error messages.",
					PropertyType: typed(synclet.String), IsSecret: true, Order: order(0),
				},
				"start_date": {
					Title:        "Start Date",
					Description:  "First UTC day, YYYY-MM-DD, to read. The API serves only the last 45 days; an earlier day starts at the oldest day it serves.",
					PropertyType: typed(synclet.String), Format: synclet.Date, Examples: []string{"2026-09-01"}, Order: order(1),
				},
				"lookback_days": {
					Title:        "Lookback Days",
					Description:  "Days before the saved day an incremental read requests again, so revenue and cohort metrics that keep changing after the day are picked up. Default 3.",
					PropertyType: typed(synclet.Integer), Default: defaultLookbackDays, Order: order(2),
				},
			}},
		},
	}, nil
}
