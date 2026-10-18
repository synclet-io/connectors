package main

import (
	"context"
	"errors"
	"fmt"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/googleapi"
)

// bigqueryOps abstracts all BigQuery API calls for testability via mocks.
type bigqueryOps interface {
	ensureDataset(ctx context.Context, datasetID, location string) error
	createTable(ctx context.Context, datasetID, tableName string, schema bigquery.Schema, partitioning *bigquery.TimePartitioning, clustering *bigquery.Clustering) error
	tableExists(ctx context.Context, datasetID, tableName string) (bool, error)
	getTableSchema(ctx context.Context, datasetID, tableName string) (bigquery.Schema, error)
	getTableMetadata(ctx context.Context, datasetID, tableName string) (*bigquery.TableMetadata, error)
	dropTable(ctx context.Context, datasetID, tableName string) error
	copyTable(ctx context.Context, srcDataset, srcTable, dstDataset, dstTable string) error
	executeQuery(ctx context.Context, sql string) error
}

// bqOps implements bigqueryOps using the real BigQuery client.
type bqOps struct {
	client    *bigquery.Client
	projectID string
}

func newBqOps(client *bigquery.Client, projectID string) *bqOps {
	return &bqOps{client: client, projectID: projectID}
}

func (o *bqOps) ensureDataset(ctx context.Context, datasetID, location string) error {
	ds := o.client.Dataset(datasetID)
	if _, err := ds.Metadata(ctx); err != nil {
		if isNotFoundError(err) {
			return ds.Create(ctx, &bigquery.DatasetMetadata{Location: location})
		}
		return fmt.Errorf("checking dataset %q: %w", datasetID, err)
	}
	return nil
}

func (o *bqOps) createTable(ctx context.Context, datasetID, tableName string, schema bigquery.Schema, partitioning *bigquery.TimePartitioning, clustering *bigquery.Clustering) error {
	md := &bigquery.TableMetadata{
		Schema:           schema,
		TimePartitioning: partitioning,
		Clustering:       clustering,
	}
	return o.client.Dataset(datasetID).Table(tableName).Create(ctx, md)
}

func (o *bqOps) tableExists(ctx context.Context, datasetID, tableName string) (bool, error) {
	_, err := o.client.Dataset(datasetID).Table(tableName).Metadata(ctx)
	if err != nil {
		if isNotFoundError(err) {
			return false, nil
		}
		return false, fmt.Errorf("checking table %q.%q: %w", datasetID, tableName, err)
	}
	return true, nil
}

func (o *bqOps) getTableSchema(ctx context.Context, datasetID, tableName string) (bigquery.Schema, error) {
	md, err := o.client.Dataset(datasetID).Table(tableName).Metadata(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting schema for %q.%q: %w", datasetID, tableName, err)
	}
	return md.Schema, nil
}

func (o *bqOps) getTableMetadata(ctx context.Context, datasetID, tableName string) (*bigquery.TableMetadata, error) {
	md, err := o.client.Dataset(datasetID).Table(tableName).Metadata(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting metadata for %q.%q: %w", datasetID, tableName, err)
	}
	return md, nil
}

func (o *bqOps) dropTable(ctx context.Context, datasetID, tableName string) error {
	err := o.client.Dataset(datasetID).Table(tableName).Delete(ctx)
	if err != nil && isNotFoundError(err) {
		return nil // ignore not-found on delete
	}
	return err
}

func (o *bqOps) copyTable(ctx context.Context, srcDataset, srcTable, dstDataset, dstTable string) error {
	src := o.client.Dataset(srcDataset).Table(srcTable)
	dst := o.client.Dataset(dstDataset).Table(dstTable)

	copier := dst.CopierFrom(src)
	copier.WriteDisposition = bigquery.WriteTruncate
	copier.CreateDisposition = bigquery.CreateIfNeeded

	job, err := copier.Run(ctx)
	if err != nil {
		return fmt.Errorf("starting copy %q.%q -> %q.%q: %w", srcDataset, srcTable, dstDataset, dstTable, err)
	}

	status, err := job.Wait(ctx)
	if err != nil {
		return fmt.Errorf("waiting for copy job: %w", err)
	}
	if status.Err() != nil {
		return fmt.Errorf("copy job failed: %w", status.Err())
	}
	return nil
}

func (o *bqOps) executeQuery(ctx context.Context, sql string) error {
	q := o.client.Query(sql)

	job, err := q.Run(ctx)
	if err != nil {
		return fmt.Errorf("running query: %w", err)
	}

	status, err := job.Wait(ctx)
	if err != nil {
		return fmt.Errorf("waiting for query: %w", err)
	}
	if status.Err() != nil {
		return fmt.Errorf("query failed: %w", status.Err())
	}
	return nil
}

// isNotFoundError checks if the error is a Google API 404 Not Found error.
func isNotFoundError(err error) bool {
	var apiErr *googleapi.Error
	if errors.As(err, &apiErr) {
		return apiErr.Code == 404
	}
	return false
}
