package main

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"
	"github.com/google/uuid"
)

// loader abstracts the data loading strategy for BigQuery.
type loader interface {
	load(ctx context.Context, datasetID, tableName string, schema bigquery.Schema, records []map[string]interface{}) error
	close() error
}

// standardLoader loads data to BigQuery via JSONL using NewReaderSource (D-02).
type standardLoader struct {
	client *bigquery.Client
}

func newStandardLoader(client *bigquery.Client) *standardLoader {
	return &standardLoader{client: client}
}

func (l *standardLoader) load(ctx context.Context, datasetID, tableName string, schema bigquery.Schema, records []map[string]interface{}) error {
	if len(records) == 0 {
		return nil
	}

	// Marshal records as JSONL into a buffer.
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	for _, rec := range records {
		if err := enc.Encode(rec); err != nil {
			return fmt.Errorf("encoding record to JSONL: %w", err)
		}
	}

	// Create a reader source from the JSONL buffer.
	src := bigquery.NewReaderSource(&buf)
	src.SourceFormat = bigquery.JSON
	src.Schema = schema

	// Load data into BigQuery.
	tbl := l.client.Dataset(datasetID).Table(tableName)
	ldr := tbl.LoaderFrom(src)
	ldr.WriteDisposition = bigquery.WriteAppend

	job, err := ldr.Run(ctx)
	if err != nil {
		return fmt.Errorf("starting load job for %q.%q: %w", datasetID, tableName, err)
	}

	status, err := job.Wait(ctx)
	if err != nil {
		return fmt.Errorf("waiting for load job: %w", err)
	}
	if status.Err() != nil {
		return fmt.Errorf("load job failed: %w", status.Err())
	}

	return nil
}

func (l *standardLoader) close() error {
	return nil // no-op for standard loader
}

// gcsLoader loads data to BigQuery via GCS staging (D-01).
// Records are written as CSV to GCS, then a BigQuery load job reads from the GCS URI.
type gcsLoader struct {
	bqClient  *bigquery.Client
	gcsClient *storage.Client
	bucket    string
	path      string
	keepFiles bool
}

func newGCSLoader(bqClient *bigquery.Client, gcsClient *storage.Client, bucket, path string, keepFiles bool) *gcsLoader {
	return &gcsLoader{
		bqClient:  bqClient,
		gcsClient: gcsClient,
		bucket:    bucket,
		path:      path,
		keepFiles: keepFiles,
	}
}

func (l *gcsLoader) load(ctx context.Context, datasetID, tableName string, schema bigquery.Schema, records []map[string]interface{}) error {
	if len(records) == 0 {
		return nil
	}

	// Build the GCS object path.
	objectName := fmt.Sprintf("%s/%s/%s_%d.csv", l.path, tableName, uuid.New().String(), time.Now().UnixMilli())
	gcsURI := fmt.Sprintf("gs://%s/%s", l.bucket, objectName)

	// Write CSV to GCS.
	obj := l.gcsClient.Bucket(l.bucket).Object(objectName)
	w := obj.NewWriter(ctx)
	w.ContentType = "text/csv"

	csvWriter := csv.NewWriter(w)

	// Write header row from schema field names.
	headers := make([]string, len(schema))
	for i, field := range schema {
		headers[i] = field.Name
	}
	if err := csvWriter.Write(headers); err != nil {
		w.Close()
		return fmt.Errorf("writing CSV header: %w", err)
	}

	// Write data rows.
	for _, rec := range records {
		row := make([]string, len(headers))
		for i, h := range headers {
			row[i] = fmt.Sprintf("%v", rec[h])
		}
		if err := csvWriter.Write(row); err != nil {
			w.Close()
			return fmt.Errorf("writing CSV row: %w", err)
		}
	}

	csvWriter.Flush()
	if err := csvWriter.Error(); err != nil {
		w.Close()
		return fmt.Errorf("flushing CSV writer: %w", err)
	}
	if err := w.Close(); err != nil {
		return fmt.Errorf("closing GCS writer: %w", err)
	}

	// Create BigQuery load job from GCS.
	gcsRef := bigquery.NewGCSReference(gcsURI)
	gcsRef.SourceFormat = bigquery.CSV
	gcsRef.SkipLeadingRows = 1
	gcsRef.AllowQuotedNewlines = true
	gcsRef.Schema = schema

	tbl := l.bqClient.Dataset(datasetID).Table(tableName)
	ldr := tbl.LoaderFrom(gcsRef)
	ldr.WriteDisposition = bigquery.WriteAppend

	job, err := ldr.Run(ctx)
	if err != nil {
		return fmt.Errorf("starting GCS load job for %q.%q: %w", datasetID, tableName, err)
	}

	status, err := job.Wait(ctx)
	if err != nil {
		return fmt.Errorf("waiting for GCS load job: %w", err)
	}
	if status.Err() != nil {
		return fmt.Errorf("GCS load job failed: %w", status.Err())
	}

	// Clean up GCS file if not keeping.
	if !l.keepFiles {
		if err := obj.Delete(ctx); err != nil {
			// Non-fatal: log but don't fail the load.
			_ = err
		}
	}

	return nil
}

func (l *gcsLoader) close() error {
	return l.gcsClient.Close()
}
