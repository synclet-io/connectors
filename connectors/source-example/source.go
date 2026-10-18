package main

import (
	"fmt"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

const (
	streamName   = "numbers"
	defaultCount = 10
	maxCount     = 1_000_000
)

// Config is the user-supplied connector configuration.
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

// ExampleSource emits a fixed number of generated rows into a single stream.
type ExampleSource struct{}

// NewExampleSource creates the example source.
func NewExampleSource() *ExampleSource {
	return &ExampleSource{}
}

// Spec describes the configuration form Synclet renders for this connector.
func (s *ExampleSource) Spec(_ synclet.LogTracker) (*synclet.ConnectorSpecification, error) {
	order := 0

	return &synclet.ConnectorSpecification{
		DocumentationURL: "https://github.com/synclet-io/connectors/tree/main/connectors/source-example",
		SupportedDestinationSyncModes: []synclet.DestinationSyncMode{
			synclet.DestinationSyncModeOverwrite,
			synclet.DestinationSyncModeAppend,
		},
		ConnectionSpecification: synclet.ConnectionSpecification{
			Title:       "Example Source Spec",
			Description: "Generates a sequence of numbered rows",
			Type:        "object",
			Required:    []synclet.PropertyName{},
			Properties: synclet.Properties{
				Properties: map[synclet.PropertyName]synclet.PropertySpec{
					"count": {
						Title:        "Row count",
						Description:  fmt.Sprintf("Number of rows to emit (1-%d)", maxCount),
						PropertyType: synclet.PropertyType{Type: []synclet.PropType{synclet.Integer}},
						Default:      defaultCount,
						Order:        &order,
					},
				},
			},
		},
	}, nil
}

// Check validates the configuration. A real connector would also try to reach its upstream here.
func (s *ExampleSource) Check(cfgPath string, logTracker synclet.LogTracker) error {
	cfg, err := loadConfig(cfgPath)
	if err != nil {
		return err
	}

	if err := cfg.validate(); err != nil {
		return err
	}

	return logTracker.Log(synclet.LogLevelInfo, "configuration is valid")
}

// Discover returns the single stream this source produces.
func (s *ExampleSource) Discover(_ string, _ synclet.LogTracker) (*synclet.Catalog, error) {
	return &synclet.Catalog{Streams: []synclet.Stream{{
		Name: streamName,
		JSONSchema: synclet.Properties{
			Properties: map[synclet.PropertyName]synclet.PropertySpec{
				"id":    {PropertyType: synclet.PropertyType{Type: []synclet.PropType{synclet.Integer}}},
				"label": {PropertyType: synclet.PropertyType{Type: []synclet.PropType{synclet.String}}},
			},
		},
		SupportedSyncModes:      []synclet.SyncMode{synclet.SyncModeFullRefresh},
		SourceDefinedPrimaryKey: [][]string{{"id"}},
	}}}, nil
}

// Read emits one RECORD message per generated row for every selected stream.
func (s *ExampleSource) Read(cfgPath, _ string, catalog *synclet.ConfiguredCatalog, tracker synclet.MessageTracker) error {
	cfg, err := loadConfig(cfgPath)
	if err != nil {
		return err
	}

	if err := cfg.validate(); err != nil {
		return err
	}

	for _, cs := range catalog.Streams {
		if cs.Stream.Name != streamName {
			return fmt.Errorf("unknown stream %q", cs.Stream.Name)
		}

		for i := 1; i <= cfg.rowCount(); i++ {
			record := map[string]any{"id": i, "label": fmt.Sprintf("row %d", i)}
			if err := tracker.Record(record, streamName, cs.Stream.Namespace); err != nil {
				return fmt.Errorf("emitting record: %w", err)
			}
		}
	}

	return nil
}

func loadConfig(path string) (Config, error) {
	var cfg Config
	if err := synclet.UnmarshalFromPath(path, &cfg); err != nil {
		return Config{}, fmt.Errorf("loading configuration: %w", err)
	}

	return cfg, nil
}
