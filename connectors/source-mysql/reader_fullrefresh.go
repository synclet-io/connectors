package main

import (
	"context"
	"database/sql"
	"fmt"

	"go.uber.org/multierr"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

// FullRefreshReader reads a table using full refresh mode with PK-based chunking.
type FullRefreshReader struct {
	db *sql.DB
	em *synclet.Emitter
}

// NewFullRefreshReader creates a new FullRefreshReader.
func NewFullRefreshReader(db *sql.DB, em *synclet.Emitter) *FullRefreshReader {
	return &FullRefreshReader{db: db, em: em}
}

// ReadTable reads all rows from a table, optionally resuming from previous state.
func (r *FullRefreshReader) ReadTable(ctx context.Context, table TableInfo, prevState *FullRefreshState) error {
	streamName := table.Name
	columns := columnNames(table.Columns)

	if prevState != nil && prevState.Done {
		return nil // already completed
	}

	if len(table.PrimaryKey) == 0 {
		return r.readWithoutPK(ctx, table, columns, streamName)
	}

	return r.readWithPK(ctx, table, columns, streamName, prevState)
}

// readWithoutPK reads the entire table in a single query (non-resumable).
func (r *FullRefreshReader) readWithoutPK(ctx context.Context, table TableInfo, columns []string, streamName string) (rerr error) {
	query := fmt.Sprintf("SELECT %s FROM `%s`.`%s`", quoteColumns(columns), table.Schema, table.Name)

	rows, err := r.db.QueryContext(ctx, query)
	if err != nil {
		return fmt.Errorf("query: %w", err)
	}
	defer multierr.AppendInvoke(&rerr, multierr.Close(rows))

	for rows.Next() {
		record, err := scanRow(rows, columns)
		if err != nil {
			return fmt.Errorf("scanning row: %w", err)
		}

		if err := r.em.Record(streamDescriptor(table.Schema, streamName), record); err != nil {
			return fmt.Errorf("emitting record: %w", err)
		}
	}

	return rows.Err()
}

// readWithPK reads the table in chunks using PK-based pagination, resumable via state.
func (r *FullRefreshReader) readWithPK(ctx context.Context, table TableInfo, columns []string, streamName string, prevState *FullRefreshState) error {
	pkCols := table.PrimaryKey
	selectCols := quoteColumns(columns)
	orderBy := quoteColumns(pkCols)

	for {
		var query string
		var args []interface{}

		var lastPK map[string]interface{}
		if prevState != nil && prevState.LastPKVals != nil {
			lastPK = prevState.LastPKVals
		}

		if lastPK != nil {
			// Resume from last PK position
			whereParts := buildPKWhereClause(pkCols)
			query = fmt.Sprintf("SELECT %s FROM `%s`.`%s` WHERE %s ORDER BY %s LIMIT %d",
				selectCols, table.Schema, table.Name, whereParts, orderBy, DefaultChunkSize)
			for _, pk := range pkCols {
				args = append(args, lastPK[pk])
			}
		} else {
			query = fmt.Sprintf("SELECT %s FROM `%s`.`%s` ORDER BY %s LIMIT %d",
				selectCols, table.Schema, table.Name, orderBy, DefaultChunkSize)
		}

		rows, err := r.db.QueryContext(ctx, query, args...)
		if err != nil {
			return fmt.Errorf("chunk query: %w", err)
		}

		count := 0
		var lastRecord map[string]interface{}

		for rows.Next() {
			record, err := scanRow(rows, columns)
			if err != nil {
				_ = rows.Close()
				return fmt.Errorf("scanning row: %w", err)
			}

			if err := r.em.Record(streamDescriptor(table.Schema, streamName), record); err != nil {
				_ = rows.Close()
				return fmt.Errorf("emitting record: %w", err)
			}

			lastRecord = record
			count++
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return err
		}
		if err := rows.Close(); err != nil {
			return fmt.Errorf("closing rows: %w", err)
		}

		if count == 0 {
			// No more rows, emit final state
			if err := emitStreamState(r.em, streamDescriptor(table.Schema, streamName), &FullRefreshState{Done: true}); err != nil {
				return fmt.Errorf("emitting final state: %w", err)
			}
			break
		}

		// Update state with last PK values for resumability
		pkVals := make(map[string]interface{}, len(pkCols))
		for _, pk := range pkCols {
			pkVals[pk] = lastRecord[pk]
		}

		state := &FullRefreshState{LastPKVals: pkVals}
		if err := emitStreamState(r.em, streamDescriptor(table.Schema, streamName), state); err != nil {
			return fmt.Errorf("emitting state: %w", err)
		}

		// Update for next iteration
		if prevState == nil {
			prevState = &FullRefreshState{}
		}
		prevState.LastPKVals = pkVals

		if count < DefaultChunkSize {
			// Last chunk
			if err := emitStreamState(r.em, streamDescriptor(table.Schema, streamName), &FullRefreshState{Done: true}); err != nil {
				return fmt.Errorf("emitting final state: %w", err)
			}
			break
		}
	}

	return nil
}

// buildPKWhereClause builds a WHERE clause for composite PK pagination.
// For single PK: `pk > ?`
// For composite PK (a, b): `(a > ?) OR (a = ? AND b > ?)`
func buildPKWhereClause(pkCols []string) string {
	if len(pkCols) == 1 {
		return fmt.Sprintf("`%s` > ?", pkCols[0])
	}

	// Composite PK: build tuple comparison
	// (a, b, c) > (?, ?, ?) is equivalent to:
	// (a > ?) OR (a = ? AND b > ?) OR (a = ? AND b = ? AND c > ?)
	// But MySQL supports tuple comparison directly:
	return fmt.Sprintf("(%s) > (%s)", quoteColumns(pkCols), placeholders(len(pkCols)))
}

// placeholders returns n comma-separated "?" placeholders.
func placeholders(n int) string {
	s := ""
	for i := 0; i < n; i++ {
		if i > 0 {
			s += ", "
		}
		s += "?"
	}
	return s
}
