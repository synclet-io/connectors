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
		DocumentationURL:    "https://github.com/synclet-io/connectors/tree/main/connectors/source-notion",
		SupportsIncremental: true,
		ProtocolVersion:     "0.5.2",
		SupportedDestinationSyncModes: []synclet.DestinationSyncMode{
			synclet.DestinationSyncModeOverwrite,
			synclet.DestinationSyncModeAppend,
			synclet.DestinationSyncModeAppendDedup,
		},
		ConnectionSpecification: synclet.ConnectionSpecification{
			Title:       "Notion Source Spec",
			Description: "Reads the users, databases, pages and blocks shared with a Notion internal integration.",
			Type:        "object",
			Required:    []synclet.PropertyName{"access_token"},
			Properties: synclet.Properties{Properties: map[synclet.PropertyName]synclet.PropertySpec{
				"access_token": {
					Title:        "Internal Integration Token",
					Description:  "Secret of a Notion internal integration (https://www.notion.so/profile/integrations) with the capability \"Read content\", and \"Read user information including email addresses\" for the users stream. Pages and databases are visible to it once shared with it under ⋯ → Connections. It is sent in the Authorization header.",
					PropertyType: typed(synclet.String), IsSecret: true, Order: order(0),
				},
				"lookback_minutes": {
					Title:        "Lookback Window (minutes)",
					Description:  "An incremental read starts this many minutes before the saved last_edited_time, because Notion rounds last_edited_time to the minute and its search index lags behind edits. 0 or more.",
					PropertyType: typed(synclet.Integer), Default: defaultLookbackMinutes, Order: order(1),
				},
				"max_block_depth": {
					Title:        "Maximum Block Depth",
					Description:  "How deep blocks are read under a page. Between 1 and 100. A page with blocks nested deeper fails the read.",
					PropertyType: typed(synclet.Integer), Default: defaultMaxBlockDepth, Order: order(2),
				},
				"max_blocks_per_page": {
					Title:        "Maximum Blocks per Page",
					Description:  "How many blocks are read under one page. 1 or more. A page with more fails the read.",
					PropertyType: typed(synclet.Integer), Default: defaultMaxBlocksPerPage, Order: order(3),
				},
			}},
		},
	}, nil
}
