package main

import (
	"context"
	"fmt"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

const (
	streamName   = "numbers"
	defaultCount = 10
	maxCount     = 1_000_000
)

// Config is the connector configuration; the SDK decodes it from the
// configuration JSON before every command.
type Config struct {
	Count int `json:"count"`
}

func (c Config) rowCount() int {
	if c.Count == 0 {
		return defaultCount
	}
	return c.Count
}

func (c Config) validate() error {
	if c.Count < 0 || c.Count > maxCount {
		return fmt.Errorf("count must be between 1 and %d", maxCount)
	}
	return nil
}

func typed(t synclet.PropType) synclet.PropertyType {
	return synclet.PropertyType{Type: synclet.PropTypes{t}}
}

// Spec describes the configuration form Synclet renders for this connector.
func (Source) Spec(_ context.Context) (*synclet.ConnectorSpecification, error) {
	order := 0
	return &synclet.ConnectorSpecification{
		DocumentationURL: "https://github.com/synclet-io/connectors/tree/main/connectors/source-example",
		ProtocolVersion:  "0.5.2",
		SupportedDestinationSyncModes: []synclet.DestinationSyncMode{
			synclet.DestinationSyncModeOverwrite,
			synclet.DestinationSyncModeAppend,
		},
		ConnectionSpecification: synclet.ConnectionSpecification{
			Title:       "Example Source Spec",
			Description: "Generates a sequence of numbered rows",
			Type:        "object",
			Properties: synclet.Properties{
				Properties: map[synclet.PropertyName]synclet.PropertySpec{
					"count": {
						Title:        "Row count",
						Description:  fmt.Sprintf("Number of rows to emit (1-%d)", maxCount),
						PropertyType: typed(synclet.Integer),
						Default:      defaultCount,
						Order:        &order,
					},
				},
			},
		},
	}, nil
}

// Check validates the configuration. A real connector also reaches its
// upstream here; the returned error becomes the FAILED status message.
func (Source) Check(_ context.Context, cfg Config) error {
	return cfg.validate()
}

// Discover returns the single stream this source produces.
func (Source) Discover(_ context.Context, _ Config) (*synclet.Catalog, error) {
	return &synclet.Catalog{Streams: []synclet.Stream{{
		Name: streamName,
		JSONSchema: synclet.ObjectSchema(map[synclet.PropertyName]synclet.PropertySpec{
			"id":    {PropertyType: typed(synclet.Integer)},
			"label": {PropertyType: typed(synclet.String)},
		}),
		SupportedSyncModes:      []synclet.SyncMode{synclet.SyncModeFullRefresh},
		SourceDefinedPrimaryKey: [][]string{{"id"}},
	}}}, nil
}

// Read emits one record per generated row for every configured stream and
// stops when ctx is cancelled.
func (Source) Read(ctx context.Context, cfg Config, catalog *synclet.ConfiguredCatalog, _ *synclet.InputState, em *synclet.Emitter) error {
	if err := cfg.validate(); err != nil {
		return err
	}
	for _, cs := range catalog.Streams {
		if cs.Stream.Name != streamName {
			return fmt.Errorf("unknown stream %q", cs.Stream.Name)
		}
		desc := cs.Stream.Descriptor()
		for i := 1; i <= cfg.rowCount(); i++ {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := em.Record(desc, map[string]any{"id": i, "label": fmt.Sprintf("row %d", i)}); err != nil {
				return fmt.Errorf("emit record: %w", err)
			}
		}
	}
	return nil
}
