package main

import (
	"context"
	"database/sql"
	"fmt"

	airbyte "github.com/saturn4er/airbyte-go-sdk"
	"golang.org/x/sync/errgroup"
)

// PartitionedReader reads a table using concurrent partition workers.
// Non-resumable: if interrupted, the entire table is re-read.
type PartitionedReader struct {
	db      *sql.DB
	tracker airbyte.MessageTracker
}

// NewPartitionedReader creates a new PartitionedReader.
func NewPartitionedReader(db *sql.DB, tracker airbyte.MessageTracker) *PartitionedReader {
	return &PartitionedReader{db: db, tracker: tracker}
}

// ReadTable reads a table using concurrent partitions.
func (r *PartitionedReader) ReadTable(ctx context.Context, table TableInfo, maxWorkers int) error {
	if len(table.PrimaryKey) != 1 {
		return fmt.Errorf("partitioned reads require a single-column primary key, got %d columns", len(table.PrimaryKey))
	}

	pkColumn := table.PrimaryKey[0]
	numPartitions := estimatePartitions(table.DataLength, maxWorkers)

	partitions, err := buildPartitions(ctx, r.db, table.Schema, table.Name, pkColumn, numPartitions)
	if err != nil {
		return fmt.Errorf("building partitions: %w", err)
	}

	streamName := table.Name
	columns := columnNames(table.Columns)

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(maxWorkers)

	for _, part := range partitions {
		g.Go(func() error {
			return r.readPartition(gctx, table.Schema, table.Name, pkColumn, columns, streamName, part)
		})
	}

	return g.Wait()
}

// readPartition reads a single partition of a table.
func (r *PartitionedReader) readPartition(
	ctx context.Context,
	schema, table, pkColumn string,
	columns []string,
	streamName string,
	part Partition,
) error {
	var query string
	var args []interface{}

	selectCols := quoteColumns(columns)

	switch {
	case part.LowerBound == nil && part.UpperBound == nil:
		query = fmt.Sprintf("SELECT %s FROM `%s`.`%s` ORDER BY `%s`", selectCols, schema, table, pkColumn)
	case part.LowerBound == nil:
		query = fmt.Sprintf("SELECT %s FROM `%s`.`%s` WHERE `%s` <= ? ORDER BY `%s`", selectCols, schema, table, pkColumn, pkColumn)
		args = append(args, part.UpperBound)
	case part.UpperBound == nil:
		query = fmt.Sprintf("SELECT %s FROM `%s`.`%s` WHERE `%s` > ? ORDER BY `%s`", selectCols, schema, table, pkColumn, pkColumn)
		args = append(args, part.LowerBound)
	default:
		query = fmt.Sprintf("SELECT %s FROM `%s`.`%s` WHERE `%s` > ? AND `%s` <= ? ORDER BY `%s`", selectCols, schema, table, pkColumn, pkColumn, pkColumn)
		args = append(args, part.LowerBound, part.UpperBound)
	}

	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("partition query: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		record, err := scanRow(rows, columns)
		if err != nil {
			return fmt.Errorf("scanning row: %w", err)
		}

		if err := r.tracker.Record(record, streamName, ""); err != nil {
			return fmt.Errorf("emitting record: %w", err)
		}
	}

	return rows.Err()
}

// columnNames extracts column names from ColumnInfo slice.
func columnNames(cols []ColumnInfo) []string {
	names := make([]string, len(cols))
	for i, c := range cols {
		names[i] = c.Name
	}
	return names
}

// quoteColumns returns a comma-separated list of backtick-quoted column names.
func quoteColumns(cols []string) string {
	quoted := ""
	for i, c := range cols {
		if i > 0 {
			quoted += ", "
		}
		quoted += "`" + c + "`"
	}
	return quoted
}
