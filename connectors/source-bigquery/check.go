package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
)

// Check runs a test query in the project and, for every configured dataset,
// reads its metadata and a page of its tables. A failure names the missing
// project or dataset, or the role the credentials lack.
func (s Source) Check(ctx context.Context, cfg Config) error {
	c, err := s.client(ctx, cfg)
	if err != nil {
		return err
	}
	defer func() { _ = c.Close() }()
	if _, err := c.Query("SELECT 1").Read(ctx); err != nil {
		switch apiCode(err) {
		case http.StatusNotFound:
			return fmt.Errorf("project %q not found: %w", cfg.ProjectID, err)
		case http.StatusForbidden:
			return fmt.Errorf("the account needs the BigQuery Job User role (bigquery.jobs.create) on project %q: %w", cfg.ProjectID, err)
		}
		return fmt.Errorf("run a test query in project %q: %w", cfg.ProjectID, err)
	}
	for _, name := range cfg.Datasets {
		if err := checkDataset(ctx, c.Dataset(name)); err != nil {
			return err
		}
	}
	return nil
}

func checkDataset(ctx context.Context, ds *bigquery.Dataset) error {
	ref := ds.ProjectID + "." + ds.DatasetID
	wrap := func(err error) error {
		switch apiCode(err) {
		case http.StatusNotFound:
			return fmt.Errorf("dataset %q not found: %w", ref, err)
		case http.StatusForbidden:
			return fmt.Errorf("the account needs the BigQuery Data Viewer role on dataset %q: %w", ref, err)
		}
		return fmt.Errorf("read dataset %q: %w", ref, err)
	}
	if _, err := ds.Metadata(ctx); err != nil {
		return wrap(err)
	}
	it := ds.Tables(ctx)
	it.PageInfo().MaxSize = 1
	if _, err := it.Next(); err != nil && !errors.Is(err, iterator.Done) {
		return wrap(err)
	}
	return nil
}

// apiCode is the HTTP status of a BigQuery API error, or 0.
func apiCode(err error) int {
	var gerr *googleapi.Error
	if errors.As(err, &gerr) {
		return gerr.Code
	}
	return 0
}
