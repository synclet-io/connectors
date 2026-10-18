package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"go.uber.org/multierr"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"
	synclet "github.com/synclet-io/synclet-go-sdk"
)

// BigQueryDestination implements synclet.Destination for Google BigQuery.
type BigQueryDestination struct{}

// NewBigQueryDestination creates a new BigQuery destination connector.
func NewBigQueryDestination() *BigQueryDestination {
	return &BigQueryDestination{}
}

// Spec returns the connector specification defining config fields.
func (d *BigQueryDestination) Spec(_ context.Context) (*synclet.ConnectorSpecification, error) {
	return &synclet.ConnectorSpecification{
		DocumentationURL: "https://cloud.google.com/bigquery/docs",
		SupportedDestinationSyncModes: []synclet.DestinationSyncMode{
			synclet.DestinationSyncModeOverwrite,
			synclet.DestinationSyncModeAppend,
			synclet.DestinationSyncModeAppendDedup,
		},
		ConnectionSpecification: bigqueryConnectionSpec(),
	}, nil
}

// Check validates the configuration by verifying BigQuery dataset access
// and optionally GCS bucket access for staging mode.
func (d *BigQueryDestination) Check(ctx context.Context, config Config) (rerr error) {
	log := synclet.Logger(ctx)

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

	authOpt, err := createClientOption(ctx, config.Credentials, scopes...)
	if err != nil {
		return fmt.Errorf("failed to create auth: %w", err)
	}

	// Verify BigQuery dataset access
	bqClient, err := bigquery.NewClient(ctx, config.ProjectID, authOpt)
	if err != nil {
		return fmt.Errorf("failed to create BigQuery client: %w", err)
	}
	defer multierr.AppendInvoke(&rerr, multierr.Close(bqClient))

	dataset := bqClient.Dataset(config.DatasetID)
	if _, err := dataset.Metadata(ctx); err != nil {
		// Dataset doesn't exist -- try to create it.
		log.Info(fmt.Sprintf("Dataset %q not found, creating...", config.DatasetID))

		if err := dataset.Create(ctx, &bigquery.DatasetMetadata{
			Location: config.DatasetLocation,
		}); err != nil {
			return fmt.Errorf("failed to create dataset %q: %w", config.DatasetID, err)
		}

		log.Info(fmt.Sprintf("Created dataset %q in %s", config.DatasetID, config.DatasetLocation))
	}

	// If GCS staging, verify bucket access.
	if isGCS {
		gcsClient, err := storage.NewClient(ctx, authOpt)
		if err != nil {
			return fmt.Errorf("failed to create GCS client: %w", err)
		}
		defer multierr.AppendInvoke(&rerr, multierr.Close(gcsClient))

		if _, err := gcsClient.Bucket(gcsMethod.GCSBucketName).Attrs(ctx); err != nil {
			return fmt.Errorf("failed to access GCS bucket %q: %w", gcsMethod.GCSBucketName, err)
		}
		log.Info(fmt.Sprintf("Verified GCS bucket %q access", gcsMethod.GCSBucketName))
	}

	log.Info("Successfully connected to BigQuery")
	return nil
}

// Write reads RECORD and STATE messages from in and writes records to BigQuery
// through the BigQueryWriter, which manages sync modes (overwrite, append,
// append_dedup).
func (d *BigQueryDestination) Write(
	ctx context.Context,
	config Config,
	catalog *synclet.ConfiguredCatalog,
	in *synclet.MessageReader,
	em *synclet.Emitter,
) (rerr error) {
	streams, err := catalogStreams(catalog)
	if err != nil {
		return err
	}

	scopes := []string{bigqueryScope}
	lm, err := config.loadingMethod()
	if err != nil {
		return fmt.Errorf("failed to parse loading method: %w", err)
	}
	gcsMethod, isGCS := lm.(*LoadingMethodGCS)
	if isGCS {
		scopes = append(scopes, gcsReadWriteScope)
	}

	authOpt, err := createClientOption(ctx, config.Credentials, scopes...)
	if err != nil {
		return fmt.Errorf("failed to create auth: %w", err)
	}

	bqClient, err := bigquery.NewClient(ctx, config.ProjectID, authOpt)
	if err != nil {
		return fmt.Errorf("failed to create BigQuery client: %w", err)
	}
	defer multierr.AppendInvoke(&rerr, multierr.Close(bqClient))

	var ldr loader
	if isGCS {
		gcsClient, err := storage.NewClient(ctx, authOpt)
		if err != nil {
			return fmt.Errorf("failed to create GCS client: %w", err)
		}
		defer multierr.AppendInvoke(&rerr, multierr.Close(gcsClient))
		ldr = newGCSLoader(bqClient, gcsClient, gcsMethod.GCSBucketName, gcsMethod.GCSBucketPath,
			gcsMethod.KeepFiles == "Keep all tmp files in GCS")
	} else {
		ldr = newStandardLoader(bqClient)
	}
	defer multierr.AppendInvoke(&rerr, multierr.Invoke(ldr.close))

	writer := NewBigQueryWriter(newBqOps(bqClient, config.ProjectID), ldr, &config, streams)
	return writeMessages(ctx, writer, in, em)
}

// writeMessages adds every record of in to writer and queues every state;
// at the end of the input it flushes, finalizes and echoes the queued states.
func writeMessages(ctx context.Context, writer *BigQueryWriter, in *synclet.MessageReader, em *synclet.Emitter) error {
	for {
		msg, err := in.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fmt.Errorf("reading input: %w", err)
		}

		switch msg.Type {
		case synclet.MessageTypeRecord:
			if msg.Record == nil {
				continue
			}
			var data map[string]interface{}
			if err := json.Unmarshal(msg.Record.Data, &data); err != nil {
				continue
			}
			sk := streamKeyFromRecord(msg.Record.Namespace, msg.Record.Stream)
			if err := writer.AddRecord(ctx, sk, data); err != nil {
				return fmt.Errorf("adding record for stream %q: %w", msg.Record.Stream, err)
			}

		case synclet.MessageTypeState:
			if msg.State != nil {
				writer.QueueState(*msg.State)
			}
		}
	}

	if err := writer.FlushAll(ctx, em); err != nil {
		return fmt.Errorf("flushing remaining records: %w", err)
	}

	return nil
}

// catalogStreams converts the configured catalog into the writer's stream
// definitions, decoding each stream's JSON schema with its "format" keywords.
func catalogStreams(catalog *synclet.ConfiguredCatalog) ([]catalogStream, error) {
	streams := make([]catalogStream, 0, len(catalog.Streams))
	for _, cs := range catalog.Streams {
		var schema map[string]interface{}
		if len(cs.Stream.JSONSchema) > 0 {
			if err := json.Unmarshal(cs.Stream.JSONSchema, &schema); err != nil {
				return nil, fmt.Errorf("failed to load catalog: json_schema of stream %q: %w", cs.Stream.Name, err)
			}
		}
		streams = append(streams, catalogStream{
			Stream: streamDef{
				Name:       cs.Stream.Name,
				Namespace:  cs.Stream.Namespace,
				JSONSchema: schema,
			},
			SyncMode:            string(cs.SyncMode),
			DestinationSyncMode: string(cs.DestinationSyncMode),
			PrimaryKey:          cs.PrimaryKey,
			CursorField:         cs.CursorField,
		})
	}
	return streams, nil
}

// streamKeyFromRecord returns a stream key from record namespace and stream name.
func streamKeyFromRecord(namespace, stream string) string {
	if namespace != "" {
		return namespace + "." + stream
	}
	return stream
}
