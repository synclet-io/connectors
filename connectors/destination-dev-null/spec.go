package main

import (
	"context"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

func order(n int) *int { return &n }

func typed(t synclet.PropType) synclet.PropertyType {
	return synclet.PropertyType{Type: synclet.PropTypes{t}}
}

func constant(value string) synclet.PropertySpec {
	return synclet.PropertySpec{PropertyType: typed(synclet.String), Const: value, Default: value, Order: order(0)}
}

func maxEntryCount(title string) synclet.PropertySpec {
	return synclet.PropertySpec{
		Title:        title,
		Description:  "Number of entries to log per stream, from 1 to 1,000.",
		PropertyType: typed(synclet.Integer), Default: defaultMaxEntryCount, Examples: []string{"100"}, Order: order(9),
	}
}

func option(title, description string, props map[synclet.PropertyName]synclet.PropertySpec, required ...synclet.PropertyName) synclet.PropertySpec {
	return synclet.PropertySpec{
		Title: title, Description: description, PropertyType: typed(synclet.Object),
		Properties: props, Required: required,
	}
}

// Spec returns the connector's capabilities and configuration schema.
func (Destination) Spec(_ context.Context) (*synclet.ConnectorSpecification, error) {
	logging := synclet.PropertySpec{
		Title:        "Logging Configuration",
		Description:  "Which records of each stream are logged.",
		PropertyType: typed(synclet.Object),
		Order:        order(1),
		OneOf: []synclet.PropertySpec{
			option("First N Entries", "Log the first N records of each stream.", map[synclet.PropertyName]synclet.PropertySpec{
				"logging_type":    constant(loggingFirstN),
				"max_entry_count": maxEntryCount("N"),
			}, "logging_type", "max_entry_count"),
			option("Every N-th Entry", "Log every N-th record of each stream, up to a maximum.", map[synclet.PropertyName]synclet.PropertySpec{
				"logging_type": constant(loggingEveryNth),
				"nth_entry_to_log": {
					Title:        "N",
					Description:  "Log every N-th record of each stream, starting with the first one: 1 logs every record, 2 every other record. From 1 to 1,000.",
					PropertyType: typed(synclet.Integer), Examples: []string{"3"}, Order: order(1),
				},
				"max_entry_count": maxEntryCount("Max Log Entries"),
			}, "logging_type", "nth_entry_to_log", "max_entry_count"),
			option("Random Sampling", "Log a random share of the records of each stream, up to a maximum.", map[synclet.PropertyName]synclet.PropertySpec{
				"logging_type": constant(loggingRandomSampling),
				"sampling_ratio": {
					Title:        "Sampling Ratio",
					Description:  "Probability that a record is logged, from 0 to 1.",
					PropertyType: typed(synclet.Number), Default: defaultSamplingRatio, Examples: []string{"0.001"}, Order: order(1),
				},
				"seed": {
					Title:        "Random Number Generator Seed",
					Description:  "Seed of the sampling, which makes it repeatable; a random seed is used when it is unset.",
					PropertyType: typed(synclet.Integer), Examples: []string{"1900"}, Order: order(2),
				},
				"max_entry_count": maxEntryCount("Max Log Entries"),
			}, "logging_type", "sampling_ratio", "max_entry_count"),
		},
	}

	return &synclet.ConnectorSpecification{
		DocumentationURL:    "https://github.com/synclet-io/connectors/tree/main/connectors/destination-dev-null",
		ProtocolVersion:     "0.5.2",
		SupportsIncremental: true,
		SupportedDestinationSyncModes: []synclet.DestinationSyncMode{
			synclet.DestinationSyncModeOverwrite,
			synclet.DestinationSyncModeAppend,
			synclet.DestinationSyncModeAppendDedup,
		},
		ConnectionSpecification: synclet.ConnectionSpecification{
			Title:       "Dev Null Destination Spec",
			Description: "Accepts every record and stores none of them; for testing sources and pipelines.",
			Type:        "object",
			Required:    []synclet.PropertyName{"test_destination"},
			Properties: synclet.Properties{Properties: map[synclet.PropertyName]synclet.PropertySpec{
				"test_destination": {
					Title:        "Test Destination",
					Description:  "What the destination does with each record.",
					PropertyType: typed(synclet.Object),
					Default:      map[string]string{"test_destination_type": typeSilent},
					Order:        order(0),
					OneOf: []synclet.PropertySpec{
						option("Silent", "Drop every record.", map[synclet.PropertyName]synclet.PropertySpec{
							"test_destination_type": constant(typeSilent),
						}, "test_destination_type"),
						option("Logging", "Drop every record and log some of them.", map[synclet.PropertyName]synclet.PropertySpec{
							"test_destination_type": constant(typeLogging),
							"logging_config":        logging,
						}, "test_destination_type", "logging_config"),
						option("Throttled", "Drop every record after waiting a fixed time for each.", map[synclet.PropertyName]synclet.PropertySpec{
							"test_destination_type": constant(typeThrottled),
							"millis_per_record": {
								Title:        "Milliseconds per Record",
								Description:  "Milliseconds to wait for each record.",
								PropertyType: typed(synclet.Integer), Examples: []string{"10"}, Order: order(1),
							},
						}, "test_destination_type", "millis_per_record"),
						option("Failing", "Drop records and fail the sync after a number of records of a stream.", map[synclet.PropertyName]synclet.PropertySpec{
							"test_destination_type": constant(typeFailing),
							"num_messages": {
								Title:        "Number of Records",
								Description:  "Records of a stream accepted before the next one fails the sync.",
								PropertyType: typed(synclet.Integer), Examples: []string{"100"}, Order: order(1),
							},
						}, "test_destination_type", "num_messages"),
					},
				},
			}},
		},
	}, nil
}
