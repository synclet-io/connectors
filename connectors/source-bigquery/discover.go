package main

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"cloud.google.com/go/bigquery"
	synclet "github.com/synclet-io/synclet-go-sdk"
	"google.golang.org/api/iterator"
)

// streamTypes are the table types discover returns a stream for.
var streamTypes = []bigquery.TableType{bigquery.RegularTable, bigquery.ViewTable, bigquery.MaterializedView}

type table struct {
	project    string
	dataset    string
	name       string
	typ        bigquery.TableType
	schema     bigquery.Schema
	primaryKey []string
}

func (t *table) column(name string) (*bigquery.FieldSchema, bool) {
	for _, f := range t.schema {
		if f.Name == name {
			return f, true
		}
	}
	return nil, false
}

func loadTable(ctx context.Context, bt *bigquery.Table) (*table, error) {
	md, err := bt.Metadata(ctx)
	if err != nil {
		return nil, err
	}
	t := &table{project: bt.ProjectID, dataset: bt.DatasetID, name: bt.TableID, typ: md.Type, schema: md.Schema}
	if md.TableConstraints != nil && md.TableConstraints.PrimaryKey != nil {
		t.primaryKey = md.TableConstraints.PrimaryKey.Columns
		for _, c := range t.primaryKey {
			if _, ok := t.column(c); !ok {
				t.primaryKey = nil
				break
			}
		}
	}
	return t, nil
}

func (t *table) stream() synclet.Stream {
	props := make(map[synclet.PropertyName]synclet.PropertySpec, len(t.schema))
	for _, f := range t.schema {
		props[synclet.PropertyName(f.Name)] = fieldSchema(f)
	}
	s := synclet.Stream{
		Name:               t.name,
		Namespace:          t.dataset,
		JSONSchema:         synclet.ObjectSchema(props),
		SupportedSyncModes: []synclet.SyncMode{synclet.SyncModeFullRefresh, synclet.SyncModeIncremental},
	}
	for _, c := range t.primaryKey {
		s.SourceDefinedPrimaryKey = append(s.SourceDefinedPrimaryKey, []string{c})
	}
	return s
}

// datasetNames returns the configured datasets, or every dataset of the
// project when none is configured.
func datasetNames(ctx context.Context, c *bigquery.Client, cfg Config) ([]string, error) {
	if len(cfg.Datasets) > 0 {
		return cfg.Datasets, nil
	}
	var names []string
	it := c.Datasets(ctx)
	for {
		ds, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("list datasets of project %q: %w", cfg.ProjectID, err)
		}
		names = append(names, ds.DatasetID)
	}
	slices.Sort(names)
	return names, nil
}

// Discover returns one stream per table, view and materialized view of the
// configured datasets, ordered by dataset and name.
func (s Source) Discover(ctx context.Context, cfg Config) (*synclet.Catalog, error) {
	c, err := s.client(ctx, cfg)
	if err != nil {
		return nil, err
	}
	defer func() { _ = c.Close() }()
	datasets, err := datasetNames(ctx, c, cfg)
	if err != nil {
		return nil, err
	}
	cat := &synclet.Catalog{Streams: []synclet.Stream{}}
	for _, name := range datasets {
		tables, err := listTables(ctx, c.Dataset(name))
		if err != nil {
			return nil, err
		}
		for _, t := range tables {
			cat.Streams = append(cat.Streams, t.stream())
		}
	}
	return cat, nil
}

func listTables(ctx context.Context, ds *bigquery.Dataset) ([]*table, error) {
	var tables []*table
	it := ds.Tables(ctx)
	for {
		bt, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("list tables of dataset %q: %w", ds.ProjectID+"."+ds.DatasetID, err)
		}
		t, err := loadTable(ctx, bt)
		if err != nil {
			return nil, fmt.Errorf("read table %s: %w", bt.FullyQualifiedName(), err)
		}
		if slices.Contains(streamTypes, t.typ) {
			tables = append(tables, t)
		}
	}
	slices.SortFunc(tables, func(a, b *table) int { return strings.Compare(a.name, b.name) })
	return tables, nil
}
