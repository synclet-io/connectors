package main

import (
	"context"
	"database/sql"
	"fmt"

	airbyte "github.com/saturn4er/airbyte-go-sdk"
)

// IncrementalReader reads a table using incremental mode with cursor-based tracking.
type IncrementalReader struct {
	db      *sql.DB
	tracker airbyte.MessageTracker
}

// NewIncrementalReader creates a new IncrementalReader.
func NewIncrementalReader(db *sql.DB, tracker airbyte.MessageTracker) *IncrementalReader {
	return &IncrementalReader{db: db, tracker: tracker}
}

// ReadTable reads a table incrementally, using snapshot-then-cursor strategy.
func (r *IncrementalReader) ReadTable(ctx context.Context, table TableInfo, cursorField string, prevState *IncrementalState) error {
	streamName := table.Name
	columns := columnNames(table.Columns)

	if prevState != nil && prevState.SnapshotDone {
		// Resume in cursor phase
		return r.readCursorPhase(ctx, table, columns, streamName, cursorField, prevState.CursorValue)
	}

	if prevState != nil && prevState.Phase == "snapshot" {
		// Resume snapshot phase
		return r.resumeSnapshot(ctx, table, columns, streamName, cursorField, prevState)
	}

	// Fresh start: capture max cursor, then snapshot
	return r.startFresh(ctx, table, columns, streamName, cursorField)
}

// startFresh begins a new incremental read by capturing max cursor value, then doing a full snapshot.
func (r *IncrementalReader) startFresh(ctx context.Context, table TableInfo, columns []string, streamName, cursorField string) error {
	// Capture the max cursor value at start
	maxCursor, err := r.getMaxCursorValue(ctx, table, cursorField)
	if err != nil {
		return fmt.Errorf("getting max cursor: %w", err)
	}

	state := &IncrementalState{
		Phase:          "snapshot",
		CursorField:    cursorField,
		MaxCursorValue: maxCursor,
	}

	return r.runSnapshot(ctx, table, columns, streamName, cursorField, state)
}

// resumeSnapshot continues a previously interrupted snapshot.
func (r *IncrementalReader) resumeSnapshot(ctx context.Context, table TableInfo, columns []string, streamName, cursorField string, state *IncrementalState) error {
	return r.runSnapshot(ctx, table, columns, streamName, cursorField, state)
}

// runSnapshot reads the entire table by PK chunks, then transitions to cursor phase.
func (r *IncrementalReader) runSnapshot(ctx context.Context, table TableInfo, columns []string, streamName, cursorField string, state *IncrementalState) error {
	pkCols := table.PrimaryKey
	selectCols := quoteColumns(columns)
	orderBy := quoteColumns(pkCols)

	for {
		var query string
		var args []interface{}

		if state.SnapshotLastPK != nil && len(pkCols) > 0 {
			whereParts := buildPKWhereClause(pkCols)
			query = fmt.Sprintf("SELECT %s FROM `%s`.`%s` WHERE %s ORDER BY %s LIMIT %d",
				selectCols, table.Schema, table.Name, whereParts, orderBy, DefaultChunkSize)
			for _, pk := range pkCols {
				args = append(args, state.SnapshotLastPK[pk])
			}
		} else if len(pkCols) > 0 {
			query = fmt.Sprintf("SELECT %s FROM `%s`.`%s` ORDER BY %s LIMIT %d",
				selectCols, table.Schema, table.Name, orderBy, DefaultChunkSize)
		} else {
			// No PK: single scan
			query = fmt.Sprintf("SELECT %s FROM `%s`.`%s`", selectCols, table.Schema, table.Name)
		}

		rows, err := r.db.QueryContext(ctx, query, args...)
		if err != nil {
			return fmt.Errorf("snapshot query: %w", err)
		}

		count := 0
		var lastRecord map[string]interface{}

		for rows.Next() {
			record, err := scanRow(rows, columns)
			if err != nil {
				rows.Close()
				return fmt.Errorf("scanning row: %w", err)
			}

			if err := r.tracker.Record(record, streamName, ""); err != nil {
				rows.Close()

				return fmt.Errorf("emitting record: %w", err)
			}

			lastRecord = record
			count++
		}

		if err := rows.Err(); err != nil {
			return err
		}
		rows.Close()

		if count == 0 || len(pkCols) == 0 {
			break
		}

		// Update snapshot state
		pkVals := make(map[string]interface{}, len(pkCols))
		for _, pk := range pkCols {
			pkVals[pk] = lastRecord[pk]
		}
		state.SnapshotLastPK = pkVals

		if err := emitStreamState(r.tracker, streamName, state); err != nil {
			return fmt.Errorf("emitting snapshot state: %w", err)
		}

		if count < DefaultChunkSize {
			break
		}
	}

	// Snapshot done, transition to cursor phase
	state.SnapshotDone = true
	state.Phase = "incremental"
	state.CursorValue = state.MaxCursorValue
	state.SnapshotLastPK = nil

	if err := emitStreamState(r.tracker, streamName, state); err != nil {
		return fmt.Errorf("emitting cursor state: %w", err)
	}

	return nil
}

// readCursorPhase reads new rows where cursor > lastCursorValue.
func (r *IncrementalReader) readCursorPhase(ctx context.Context, table TableInfo, columns []string, streamName, cursorField string, lastCursorValue interface{}) error {
	selectCols := quoteColumns(columns)
	query := fmt.Sprintf("SELECT %s FROM `%s`.`%s` WHERE `%s` > ? ORDER BY `%s` ASC LIMIT %d",
		selectCols, table.Schema, table.Name, cursorField, cursorField, DefaultChunkSize)

	cursorVal := lastCursorValue

	for {
		rows, err := r.db.QueryContext(ctx, query, cursorVal)
		if err != nil {
			return fmt.Errorf("cursor query: %w", err)
		}

		count := 0
		var lastRecord map[string]interface{}

		for rows.Next() {
			record, err := scanRow(rows, columns)
			if err != nil {
				rows.Close()
				return fmt.Errorf("scanning row: %w", err)
			}

			if err := r.tracker.Record(record, streamName, ""); err != nil {
				rows.Close()
				return fmt.Errorf("emitting record: %w", err)
			}

			lastRecord = record
			count++
		}
		if err := rows.Err(); err != nil {
			return err
		}
		rows.Close()

		if count == 0 {
			break
		}

		cursorVal = lastRecord[cursorField]

		state := &IncrementalState{
			Phase:        "incremental",
			CursorField:  cursorField,
			CursorValue:  cursorVal,
			SnapshotDone: true,
		}

		if err := emitStreamState(r.tracker, streamName, state); err != nil {
			return fmt.Errorf("emitting cursor state: %w", err)
		}

		if count < DefaultChunkSize {
			break
		}
	}

	return nil
}

// getMaxCursorValue returns the current maximum value of the cursor field.
func (r *IncrementalReader) getMaxCursorValue(ctx context.Context, table TableInfo, cursorField string) (interface{}, error) {
	query := fmt.Sprintf("SELECT MAX(`%s`) FROM `%s`.`%s`", cursorField, table.Schema, table.Name)

	var maxVal interface{}
	if err := r.db.QueryRowContext(ctx, query).Scan(&maxVal); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("querying max cursor: %w", err)
	}

	return convertSQLValue(maxVal), nil
}
