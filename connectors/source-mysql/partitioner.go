package main

import (
	"context"
	"database/sql"
	"fmt"
)

// Partition defines a range of primary key values to read.
type Partition struct {
	LowerBound interface{} // exclusive (nil = start from beginning)
	UpperBound interface{} // inclusive (nil = read to end)
}

// estimatePartitions returns the number of partitions based on table size.
func estimatePartitions(dataLength int64, maxWorkers int) int {
	if maxWorkers <= 1 {
		return 1
	}

	const targetPartitionSize = 50 * 1024 * 1024 // 50 MB per partition
	n := int(dataLength / int64(targetPartitionSize))
	if n < 1 {
		n = 1
	}
	if n > maxWorkers {
		n = maxWorkers
	}
	return n
}

// buildPartitions splits a table's PK range into n partitions.
// Only supports single-column numeric primary keys.
func buildPartitions(ctx context.Context, db *sql.DB, schema, table, pkColumn string, n int) ([]Partition, error) {
	if n <= 1 {
		return []Partition{{LowerBound: nil, UpperBound: nil}}, nil
	}

	query := fmt.Sprintf("SELECT MIN(`%s`), MAX(`%s`) FROM `%s`.`%s`", pkColumn, pkColumn, schema, table)
	var minVal, maxVal sql.NullInt64
	if err := db.QueryRowContext(ctx, query).Scan(&minVal, &maxVal); err != nil {
		return nil, fmt.Errorf("querying PK range: %w", err)
	}

	if !minVal.Valid || !maxVal.Valid {
		return []Partition{{LowerBound: nil, UpperBound: nil}}, nil
	}

	rangeSize := maxVal.Int64 - minVal.Int64
	if rangeSize <= 0 {
		return []Partition{{LowerBound: nil, UpperBound: nil}}, nil
	}

	step := rangeSize / int64(n)
	if step < 1 {
		step = 1
	}

	var partitions []Partition
	lower := minVal.Int64 - 1 // exclusive lower bound, so start before min
	for i := 0; i < n; i++ {
		upper := lower + step
		if i == n-1 {
			// Last partition captures everything remaining
			partitions = append(partitions, Partition{
				LowerBound: lower,
				UpperBound: nil,
			})
		} else {
			partitions = append(partitions, Partition{
				LowerBound: lower,
				UpperBound: upper,
			})
		}
		lower = upper
	}

	// First partition has nil lower bound (read from start)
	partitions[0].LowerBound = nil

	return partitions, nil
}
