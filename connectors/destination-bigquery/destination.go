package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"
	airbyte "github.com/saturn4er/airbyte-go-sdk"
)

// BigQueryDestination implements airbyte.Destination for Google BigQuery.
type BigQueryDestination struct{}

// NewBigQueryDestination creates a new BigQuery destination connector.
func NewBigQueryDestination() *BigQueryDestination {
	return &BigQueryDestination{}
}

// Spec returns the connector specification defining config fields.
func (d *BigQueryDestination) Spec(_ airbyte.LogTracker) (*airbyte.ConnectorSpecification, error) {
	return &airbyte.ConnectorSpecification{
		DocumentationURL: "https://cloud.google.com/bigquery/docs",
		SupportedDestinationSyncModes: []airbyte.DestinationSyncMode{
			airbyte.DestinationSyncModeOverwrite,
			airbyte.DestinationSyncModeAppend,
			airbyte.DestinationSyncModeAppendDedup,
		},
		ConnectionSpecification: bigqueryConnectionSpec(),
	}, nil
}

// Check validates the configuration by verifying BigQuery dataset access
// and optionally GCS bucket access for staging mode.
func (d *BigQueryDestination) Check(dstCfgPath string, logTracker airbyte.LogTracker) error {
	var config Config
	if err := airbyte.UnmarshalFromPath(dstCfgPath, &config); err != nil {
		return fmt.Errorf("failed to load configuration: %w", err)
	}

	ctx := context.Background()

	// Determine auth scopes needed
	scopes := []string{bigqueryScope}
	lm, err := config.loadingMethod()
	if err != nil {
		return fmt.Errorf("failed to parse loading method: %w", err)
	}
	gcsMethod, isGCS := lm.(*LoadingMethodGCS)
	if isGCS {
		scopes = append(scopes, gcsReadWriteScope)
	}

	authOpt, err := createClientOption(config.Credentials, scopes...)
	if err != nil {
		return fmt.Errorf("failed to create auth: %w", err)
	}

	// Verify BigQuery dataset access
	bqClient, err := bigquery.NewClient(ctx, config.ProjectID, authOpt)
	if err != nil {
		return fmt.Errorf("failed to create BigQuery client: %w", err)
	}
	defer bqClient.Close()

	dataset := bqClient.Dataset(config.DatasetID)
	if _, err := dataset.Metadata(ctx); err != nil {
		// Dataset doesn't exist -- try to create it.
		logTracker.Log(airbyte.LogLevelInfo, fmt.Sprintf("Dataset %q not found, creating...", config.DatasetID))
		if err := dataset.Create(ctx, &bigquery.DatasetMetadata{
			Location: config.DatasetLocation,
		}); err != nil {
			return fmt.Errorf("failed to create dataset %q: %w", config.DatasetID, err)
		}
		logTracker.Log(airbyte.LogLevelInfo, fmt.Sprintf("Created dataset %q in %s", config.DatasetID, config.DatasetLocation))
	}

	// If GCS staging, verify bucket access.
	if isGCS {
		gcsClient, err := storage.NewClient(ctx, authOpt)
		if err != nil {
			return fmt.Errorf("failed to create GCS client: %w", err)
		}
		defer gcsClient.Close()

		if _, err := gcsClient.Bucket(gcsMethod.GCSBucketName).Attrs(ctx); err != nil {
			return fmt.Errorf("failed to access GCS bucket %q: %w", gcsMethod.GCSBucketName, err)
		}
		logTracker.Log(airbyte.LogLevelInfo, fmt.Sprintf("Verified GCS bucket %q access", gcsMethod.GCSBucketName))
	}

	logTracker.Log(airbyte.LogLevelInfo, "Successfully connected to BigQuery")
	return nil
}

// Write receives Airbyte messages from inputReader and writes records to BigQuery.
// Handles RECORD and STATE messages, routing records through the BigQueryWriter
// which manages sync modes (overwrite, append, append_dedup).
func (d *BigQueryDestination) Write(dstCfgPath string, catalogPath string, inputReader io.Reader, tracker airbyte.MessageTracker) error {
	// 1. Parse config.
	var config Config
	if err := airbyte.UnmarshalFromPath(dstCfgPath, &config); err != nil {
		return fmt.Errorf("failed to load configuration: %w", err)
	}

	// 2. Parse configured catalog using raw JSON to preserve format fields
	// in JSON schema (the SDK Properties type doesn't capture "format").
	var rawCatalog rawConfiguredCatalog
	if err := airbyte.UnmarshalFromPath(catalogPath, &rawCatalog); err != nil {
		return fmt.Errorf("failed to load catalog: %w", err)
	}

	// 3. Create auth client option.
	scopes := []string{bigqueryScope}
	lm, err := config.loadingMethod()
	if err != nil {
		return fmt.Errorf("failed to parse loading method: %w", err)
	}
	gcsMethod, isGCS := lm.(*LoadingMethodGCS)
	if isGCS {
		scopes = append(scopes, gcsReadWriteScope)
	}

	authOpt, err := createClientOption(config.Credentials, scopes...)
	if err != nil {
		return fmt.Errorf("failed to create auth: %w", err)
	}

	// 4. Create BigQuery client.
	ctx := context.Background()
	bqClient, err := bigquery.NewClient(ctx, config.ProjectID, authOpt)
	if err != nil {
		return fmt.Errorf("failed to create BigQuery client: %w", err)
	}
	defer bqClient.Close()

	// 5. Create loader based on loading method.
	var ldr loader
	if isGCS {
		gcsClient, err := storage.NewClient(ctx, authOpt)
		if err != nil {
			return fmt.Errorf("failed to create GCS client: %w", err)
		}
		defer gcsClient.Close()
		ldr = newGCSLoader(bqClient, gcsClient, gcsMethod.GCSBucketName, gcsMethod.GCSBucketPath,
			gcsMethod.KeepFiles == "Keep all tmp files in GCS")
	} else {
		ldr = newStandardLoader(bqClient)
	}
	defer ldr.close()

	// 6. Create ops and writer.
	ops := newBqOps(bqClient, config.ProjectID)
	writer := NewBigQueryWriter(ops, ldr, &config, rawCatalog.Streams)

	// 7. Scan input messages.
	scanner := bufio.NewScanner(inputReader)
	scanner.Buffer(make([]byte, 0, 4*1024*1024), 4*1024*1024) // 4MB buffer for large records.

	for scanner.Scan() {
		line := scanner.Bytes()

		var msg struct {
			Type   string          `json:"type"`
			Record json.RawMessage `json:"record,omitempty"`
			State  json.RawMessage `json:"state,omitempty"`
		}
		if err := json.Unmarshal(line, &msg); err != nil {
			continue
		}

		switch msg.Type {
		case "RECORD":
			var rec struct {
				Stream    string                 `json:"stream"`
				Namespace string                 `json:"namespace"`
				Data      map[string]interface{} `json:"data"`
			}
			if err := json.Unmarshal(msg.Record, &rec); err != nil {
				continue
			}
			sk := streamKeyFromRecord(rec.Namespace, rec.Stream)
			if err := writer.AddRecord(sk, rec.Data); err != nil {
				return fmt.Errorf("adding record for stream %q: %w", rec.Stream, err)
			}

		case "STATE":
			writer.QueueState(msg.State)
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("reading input: %w", err)
	}

	// 8. Final flush + finalize + emit states.
	if err := writer.FlushAll(tracker); err != nil {
		return fmt.Errorf("flushing remaining records: %w", err)
	}

	return nil
}

// rawConfiguredCatalog mirrors the Airbyte ConfiguredCatalog but preserves
// raw JSON schema (including "format" fields) that the SDK Properties type loses.
type rawConfiguredCatalog struct {
	Streams []catalogStream `json:"streams"`
}

// streamKeyFromRecord returns a stream key from record namespace and stream name.
func streamKeyFromRecord(namespace, stream string) string {
	if namespace != "" {
		return namespace + "." + stream
	}
	return stream
}
