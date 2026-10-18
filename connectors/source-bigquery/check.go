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

// tableProbes is how many tables of a dataset check looks at to find one
// whose rows it can read.
const tableProbes = 10

// Check runs a test query in the project and, for every configured dataset
// (every visible one when none is configured), reads its metadata, a page of
// its tables and a row of its first table. A failure names the missing
// project or dataset, or the role the account lacks.
func (s Source) Check(ctx context.Context, cfg Config) error {
	c, err := s.client(ctx, cfg)
	if err != nil {
		return err
	}
	defer func() { _ = c.Close() }()
	if _, err := c.Query("SELECT 1").Read(ctx); err != nil {
		switch {
		case apiCode(err) == http.StatusNotFound:
			return fmt.Errorf("project %q not found: %w", cfg.ProjectID, err)
		case accessDenied(err):
			return fmt.Errorf("the account needs the BigQuery Job User role (bigquery.jobs.create) on project %q: %w", cfg.ProjectID, err)
		}
		return fmt.Errorf("run a test query in project %q: %w", cfg.ProjectID, err)
	}
	datasets := cfg.Datasets
	if len(datasets) == 0 {
		datasets, err = datasetNames(ctx, c, cfg)
		switch {
		case accessDenied(err):
			return fmt.Errorf("the account needs the BigQuery Data Viewer role (bigquery.datasets.list) on project %q: %w", cfg.ProjectID, err)
		case err != nil:
			return err
		case len(datasets) == 0:
			return fmt.Errorf("no dataset of project %q is visible: the account needs the BigQuery Data Viewer role on the project or on a dataset", cfg.ProjectID)
		}
	}
	for _, name := range datasets {
		if err := checkDataset(ctx, c.Dataset(name)); err != nil {
			return err
		}
	}
	return nil
}

func checkDataset(ctx context.Context, ds *bigquery.Dataset) error {
	ref := ds.ProjectID + "." + ds.DatasetID
	wrap := func(err error) error {
		switch {
		case apiCode(err) == http.StatusNotFound:
			return fmt.Errorf("dataset %q not found: %w", ref, err)
		case accessDenied(err):
			return fmt.Errorf("the account needs the BigQuery Data Viewer role on dataset %q: %w", ref, err)
		}
		return fmt.Errorf("read dataset %q: %w", ref, err)
	}
	if _, err := ds.Metadata(ctx); err != nil {
		return wrap(err)
	}
	it := ds.Tables(ctx)
	for range tableProbes {
		bt, err := it.Next()
		if errors.Is(err, iterator.Done) {
			return nil
		}
		if err != nil {
			return wrap(err)
		}
		md, err := bt.Metadata(ctx)
		if err != nil {
			return wrap(err)
		}
		if md.Type != bigquery.RegularTable {
			continue
		}
		rows := bt.Read(ctx)
		rows.PageInfo().MaxSize = 1
		var row []bigquery.Value
		if err := rows.Next(&row); err != nil && !errors.Is(err, iterator.Done) {
			return wrap(err)
		}
		return nil
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

// accessDenied reports whether err is a BigQuery API error for a missing
// permission, as opposed to quota, rate or billing errors that share its
// status.
func accessDenied(err error) bool {
	var gerr *googleapi.Error
	if !errors.As(err, &gerr) || gerr.Code != http.StatusForbidden {
		return false
	}
	for _, e := range gerr.Errors {
		if e.Reason == "accessDenied" {
			return true
		}
	}
	return false
}
